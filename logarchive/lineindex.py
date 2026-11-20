"""On-demand line indexes for extracting line ranges from a segment.

A segment blob is one raw-DEFLATE stream, so a naive line query would have to
decompress the whole segment.  Instead a sidecar index stores the *original*
bytes a second time, split into independently compressed blocks, plus enough
line bookkeeping to find the block where a requested line starts.

Key properties:

* The sidecar is content addressed by the segment's original SHA-256.  The
  original bytes never change during dictionary recompression, so an index
  built before a dictionary switch stays valid afterwards and across restarts.
* Blocks are compressed without a preset dictionary, so deleting an old
  dictionary cannot make an index unreadable.
* Every read validates the header against the segment metadata, the table
  checksum, and the SHA-256 of each block that is touched.  Original bytes are
  only returned once all of those checks have passed.
* A partially written index is never installed: the builder writes a temporary
  file which the caller atomically renames into place.

File layout (all integers big-endian)::

    MAGIC (8 bytes)
    block 0 compressed (raw DEFLATE)
    block 1 compressed
    ...
    block table, zlib-compressed (48-byte ENTRY records)
    u32 table length
    header JSON (UTF-8)
    u32 header length

The header is at the end so the file can be written in one streaming pass.
Each table entry is::

    u32 compressed_size
    u32 raw_size
    [32 bytes] sha256(raw block)
    u32 number of line starts inside this block
    u32 offset of the first line start inside this block

Lines follow the operator-facing definition: line 1 starts at byte 0, every
LF starts a new line, and a final line without a trailing LF still counts.
A line's terminating LF belongs to that line, so asking for lines 3-5 returns
exactly the bytes needed to reproduce those three lines.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import os
import struct
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

MAGIC = b"LGIDX01\n"
INDEX_VERSION = 1
HEADER_MAX_BYTES = 256 * 1024
ENTRY = struct.Struct(">II32sII")
NO_LINE_START = 0xFFFFFFFF
MIN_BLOCK_SIZE = 16
COMPRESSION_LEVEL = 6


class LineIndexError(RuntimeError):
    """Base class for line-index failures."""


class LineIndexCorrupt(LineIndexError):
    """The sidecar is missing, malformed, or does not match its segment."""


class LineRangeError(LineIndexError):
    """The requested line range is not valid for this segment."""


class LineRangeTooLarge(LineRangeError):
    """The requested range exceeds the configured response size limit."""


@dataclass(frozen=True)
class BlockEntry:
    compressed_size: int
    raw_size: int
    sha256: bytes
    line_starts: int
    first_line_offset: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def compress_block(block: bytes) -> bytes:
    compressor = zlib.compressobj(COMPRESSION_LEVEL, zlib.DEFLATED, -15)
    return compressor.compress(block) + compressor.flush()


def inflate_block(payload: bytes, expected_size: int) -> bytes:
    """Inflate one block, bounding the output to the recorded size."""
    if not 0 <= expected_size <= 0xFFFFFFFF:
        raise LineIndexCorrupt("invalid block size in index")
    decompressor = zlib.decompressobj(-15)
    try:
        out = decompressor.decompress(payload, expected_size + 1)
        out += decompressor.flush()
    except zlib.error as exc:
        raise LineIndexCorrupt(f"block is not a valid DEFLATE stream: {exc}") from exc
    if len(out) != expected_size:
        raise LineIndexCorrupt(
            f"block inflated to {len(out)} bytes, expected {expected_size}"
        )
    if not decompressor.eof:
        raise LineIndexCorrupt("block DEFLATE stream is truncated")
    if decompressor.unused_data:
        raise LineIndexCorrupt("block has trailing bytes after its DEFLATE stream")
    return out


def _line_starts_in_block(
    block: bytes, entry: BlockEntry
) -> list[int]:
    """Recompute the line starts of a block and verify them against its entry.

    This validates the stored per-block line bookkeeping on every block that
    is touched by a query, in addition to the block checksum.
    """
    starts: list[int] = []
    starts_at_line_beginning = (
        entry.line_starts > 0 and entry.first_line_offset == 0
    )
    if starts_at_line_beginning:
        starts.append(0)
    position = 0
    while True:
        position = block.find(b"\n", position)
        if position < 0:
            break
        position += 1
        # A newline in the final byte of a block starts a line in the *next*
        # block; at end of file it starts no line at all.
        if position < len(block):
            starts.append(position)
    expected_first = starts[0] if starts else NO_LINE_START
    if len(starts) != entry.line_starts or expected_first != entry.first_line_offset:
        raise LineIndexCorrupt("block line bookkeeping does not match its content")
    return starts


class LineIndexBuilder:
    """Stream original bytes into a temporary line-index file.

    ``feed`` may be called with arbitrarily large chunks; the builder splits
    them into fixed-size blocks.  ``finalize`` appends the block table and the
    header and returns the header dictionary.  The caller is responsible for
    atomically installing the finished file.
    """

    def __init__(self, path: os.PathLike[str] | str, block_size: int):
        if block_size < MIN_BLOCK_SIZE:
            raise ValueError(f"block size must be at least {MIN_BLOCK_SIZE}")
        self.block_size = block_size
        self.path = Path(path)
        self._handle = open(self.path, "wb")
        self._handle.write(MAGIC)
        self._entries: list[BlockEntry] = []
        self._pending = bytearray()
        self._at_line_start = True
        self._total_lines = 0
        self._size = 0
        self._finished = False

    @property
    def total_lines(self) -> int:
        return self._total_lines

    def feed(self, data: bytes) -> None:
        if self._finished:
            raise RuntimeError("line index builder is already finished")
        if not data:
            return
        self._pending.extend(data)
        offset = 0
        block_size = self.block_size
        while len(self._pending) - offset >= block_size:
            block = bytes(self._pending[offset : offset + block_size])
            offset += block_size
            self._write_block(block)
        if offset:
            del self._pending[:offset]

    def _write_block(self, block: bytes) -> None:
        starts: list[int] = []
        if self._at_line_start:
            starts.append(0)
        position = 0
        while True:
            position = block.find(b"\n", position)
            if position < 0:
                break
            position += 1
            if position < len(block):
                starts.append(position)
        compressed = compress_block(block)
        self._handle.write(compressed)
        self._entries.append(
            BlockEntry(
                compressed_size=len(compressed),
                raw_size=len(block),
                sha256=hashlib.sha256(block).digest(),
                line_starts=len(starts),
                first_line_offset=starts[0] if starts else NO_LINE_START,
            )
        )
        self._total_lines += len(starts)
        self._size += len(block)
        self._at_line_start = block[-1] == 0x0A

    def finalize(
        self,
        *,
        original_sha256: str,
        original_size: int,
        created_at: Optional[str] = None,
    ) -> dict:
        if self._finished:
            raise RuntimeError("line index builder is already finished")
        if self._pending:
            block = bytes(self._pending)
            self._pending.clear()
            self._write_block(block)
        if self._size != original_size:
            raise LineIndexError(
                f"streamed {self._size} bytes but expected {original_size}"
            )
        table = b"".join(
            ENTRY.pack(
                entry.compressed_size,
                entry.raw_size,
                entry.sha256,
                entry.line_starts,
                entry.first_line_offset,
            )
            for entry in self._entries
        )
        compressed_table = zlib.compress(table, 9)
        table_offset = self._handle.tell()
        if table_offset != len(MAGIC) + sum(
            entry.compressed_size for entry in self._entries
        ):
            raise LineIndexError("internal index layout error")
        self._handle.write(compressed_table)
        header = {
            "format": "logarchive-line-index",
            "version": INDEX_VERSION,
            "original_sha256": original_sha256,
            "original_size": original_size,
            "block_size": self.block_size,
            "block_count": len(self._entries),
            "total_lines": self._total_lines,
            "table_offset": table_offset,
            "table_length": len(compressed_table),
            "table_sha256": hashlib.sha256(compressed_table).hexdigest(),
            "created_at": created_at or _utc_now(),
        }
        header_bytes = json.dumps(
            header, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        self._handle.write(struct.pack(">I", len(compressed_table)))
        self._handle.write(header_bytes)
        self._handle.write(struct.pack(">I", len(header_bytes)))
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()
        self._finished = True
        return header

    def abort(self) -> None:
        if not self._finished:
            try:
                self._handle.close()
            finally:
                self._finished = True


class LineIndexReader:
    """Read and validate an existing index; also extracts line ranges."""

    def __init__(
        self,
        path: os.PathLike[str] | str,
        *,
        expected_sha256: Optional[str] = None,
        expected_size: Optional[int] = None,
    ):
        self.path = Path(path)
        self._handle = open(self.path, "rb")
        self._cache_index = -1
        self._cache_block = b""
        self.blocks_decompressed = 0
        try:
            self._load(expected_sha256, expected_size)
        except Exception:
            self._handle.close()
            raise

    # ------------------------------------------------------------- loading

    def _load(
        self,
        expected_sha256: Optional[str],
        expected_size: Optional[int],
    ) -> None:
        file_size = os.fstat(self._handle.fileno()).st_size
        if file_size < len(MAGIC) + 8:
            raise LineIndexCorrupt("line index file is too short")
        self._handle.seek(0)
        if self._handle.read(len(MAGIC)) != MAGIC:
            raise LineIndexCorrupt("line index has a bad magic value")

        self._handle.seek(file_size - 4)
        header_length = struct.unpack(">I", self._read_exact(4))[0]
        if not 2 <= header_length <= HEADER_MAX_BYTES:
            raise LineIndexCorrupt("line index header length is invalid")
        header_end = file_size - 4
        header_start = header_end - header_length
        if header_start < len(MAGIC) + 4:
            raise LineIndexCorrupt("line index header overlaps the block table")
        self._handle.seek(header_start)
        header_bytes = self._read_exact(header_length)
        try:
            header = json.loads(header_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LineIndexCorrupt("line index header is not valid JSON") from exc
        if not isinstance(header, dict):
            raise LineIndexCorrupt("line index header is not an object")
        if header.get("format") != "logarchive-line-index":
            raise LineIndexCorrupt("line index header has an unknown format")
        if header.get("version") != INDEX_VERSION:
            raise LineIndexCorrupt(
                f"unsupported line index version {header.get('version')!r}"
            )

        self._handle.seek(header_start - 4)
        table_length = struct.unpack(">I", self._read_exact(4))[0]
        if table_length != header.get("table_length"):
            raise LineIndexCorrupt("line index table length does not match the header")
        table_offset = header_start - 4 - table_length
        if table_offset < len(MAGIC) or table_offset != header.get("table_offset"):
            raise LineIndexCorrupt("line index table offset does not match the header")
        self._handle.seek(table_offset)
        compressed_table = self._read_exact(table_length)
        table_digest = hashlib.sha256(compressed_table).hexdigest()
        if table_digest != header.get("table_sha256"):
            raise LineIndexCorrupt("line index table checksum does not match the header")

        block_count = header.get("block_count")
        if not isinstance(block_count, int) or block_count < 0:
            raise LineIndexCorrupt("line index block count is invalid")
        expected_table = block_count * ENTRY.size
        try:
            table = zlib.decompress(compressed_table, 15, expected_table + 1)
        except zlib.error as exc:
            raise LineIndexCorrupt(f"line index table is not valid zlib data: {exc}") from exc
        if len(table) != expected_table:
            raise LineIndexCorrupt(
                f"line index table has {len(table)} bytes, expected {expected_table}"
            )

        original_size = header.get("original_size")
        block_size = header.get("block_size")
        total_lines = header.get("total_lines")
        original_sha256 = header.get("original_sha256")
        if (
            not isinstance(original_size, int)
            or original_size < 0
            or not isinstance(block_size, int)
            or block_size < MIN_BLOCK_SIZE
            or not isinstance(total_lines, int)
            or total_lines < 0
            or not isinstance(original_sha256, str)
            or len(original_sha256) != 64
        ):
            raise LineIndexCorrupt("line index header fields are invalid")
        if expected_sha256 is not None and original_sha256 != expected_sha256:
            raise LineIndexCorrupt(
                "line index belongs to different original bytes (SHA-256 mismatch)"
            )
        if expected_size is not None and original_size != expected_size:
            raise LineIndexCorrupt(
                "line index size does not match the segment metadata"
            )

        entries: list[BlockEntry] = []
        for index in range(block_count):
            chunk = table[index * ENTRY.size : (index + 1) * ENTRY.size]
            compressed_size, raw_size, digest, line_starts, first_offset = ENTRY.unpack(
                chunk
            )
            if compressed_size == 0 or raw_size == 0 or raw_size > block_size:
                raise LineIndexCorrupt(f"line index block {index} has an invalid size")
            if line_starts == 0:
                if first_offset != NO_LINE_START:
                    raise LineIndexCorrupt(
                        f"line index block {index} has inconsistent line offsets"
                    )
            elif first_offset >= raw_size:
                raise LineIndexCorrupt(
                    f"line index block {index} has an invalid first line offset"
                )
            entries.append(
                BlockEntry(
                    compressed_size=compressed_size,
                    raw_size=raw_size,
                    sha256=digest,
                    line_starts=line_starts,
                    first_line_offset=first_offset,
                )
            )

        expected_blocks = (
            (original_size + block_size - 1) // block_size if original_size else 0
        )
        if block_count != expected_blocks:
            raise LineIndexCorrupt(
                f"line index has {block_count} blocks, expected {expected_blocks}"
            )
        if sum(entry.raw_size for entry in entries) != original_size:
            raise LineIndexCorrupt("line index block sizes do not add up to the segment size")
        for index, entry in enumerate(entries):
            expected_raw = (
                block_size
                if index < block_count - 1
                else original_size - block_size * (block_count - 1)
            )
            if entry.raw_size != expected_raw:
                raise LineIndexCorrupt(
                    f"line index block {index} has an unexpected raw size"
                )
        if sum(entry.line_starts for entry in entries) != total_lines:
            raise LineIndexCorrupt("line index line counts do not add up")
        if (
            len(MAGIC) + sum(entry.compressed_size for entry in entries)
            != table_offset
        ):
            raise LineIndexCorrupt("line index block data does not fill the expected span")

        self.header = header
        self.entries = entries
        self._block_starts: list[int] = []
        self._file_offsets: list[int] = []
        self._cumulative_lines: list[int] = []
        raw_offset = 0
        file_offset = len(MAGIC)
        line_offset = 0
        for entry in entries:
            self._block_starts.append(raw_offset)
            self._file_offsets.append(file_offset)
            self._cumulative_lines.append(line_offset)
            raw_offset += entry.raw_size
            file_offset += entry.compressed_size
            line_offset += entry.line_starts
        self._file_offsets.append(file_offset)

    def _read_exact(self, count: int) -> bytes:
        data = self._handle.read(count)
        if len(data) != count:
            raise LineIndexCorrupt("line index file ended unexpectedly")
        return data

    # ------------------------------------------------------------- reading

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> "LineIndexReader":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    @property
    def total_lines(self) -> int:
        return int(self.header["total_lines"])

    def summary(self) -> dict:
        return {
            "original_sha256": self.header["original_sha256"],
            "original_size": self.header["original_size"],
            "block_size": self.header["block_size"],
            "block_count": self.header["block_count"],
            "total_lines": self.header["total_lines"],
            "index_size": os.path.getsize(self.path),
            "created_at": self.header["created_at"],
        }

    def read_block(self, index: int) -> bytes:
        if index == self._cache_index:
            return self._cache_block
        entry = self.entries[index]
        self._handle.seek(self._file_offsets[index])
        payload = self._read_exact(entry.compressed_size)
        block = inflate_block(payload, entry.raw_size)
        if hashlib.sha256(block).digest() != entry.sha256:
            raise LineIndexCorrupt(f"line index block {index} failed its checksum")
        self.blocks_decompressed += 1
        self._cache_index = index
        self._cache_block = block
        return block

    def locate_line(self, line: int) -> int:
        """Return the byte offset where 1-based *line* starts."""
        if line < 1 or line > self.total_lines:
            raise LineRangeError(
                f"line {line} is outside the segment (1-{self.total_lines})"
            )
        index = bisect.bisect_right(self._cumulative_lines, line - 1) - 1
        if index < 0 or index >= len(self.entries):
            raise LineIndexCorrupt("line index cannot locate the requested line")
        entry = self.entries[index]
        block = self.read_block(index)
        starts = _line_starts_in_block(block, entry)
        within = line - self._cumulative_lines[index]
        return self._block_starts[index] + starts[within - 1]

    def _resolve_bounds(
        self, start_line: int, end_line: int, max_bytes: Optional[int]
    ) -> tuple[int, int]:
        if start_line < 1:
            raise LineRangeError("start line must be at least 1")
        if end_line < start_line:
            raise LineRangeError("end line must not be before the start line")
        if end_line > self.total_lines:
            raise LineRangeError(
                f"end line {end_line} is outside the segment (1-{self.total_lines})"
            )
        start_offset = self.locate_line(start_line)
        if end_line < self.total_lines:
            end_offset = self.locate_line(end_line + 1)
        else:
            end_offset = int(self.header["original_size"])
        if end_offset <= start_offset:
            raise LineIndexCorrupt("line index produced an empty byte range")
        length = end_offset - start_offset
        if max_bytes is not None and length > max_bytes:
            raise LineRangeTooLarge(
                f"requested lines span {length} bytes, above the {max_bytes}-byte limit"
            )
        return start_offset, end_offset

    def range_length(
        self, start_line: int, end_line: int, *, max_bytes: Optional[int] = None
    ) -> int:
        """Byte length of a line range without decompressing the middle."""
        start_offset, end_offset = self._resolve_bounds(
            start_line, end_line, max_bytes
        )
        return end_offset - start_offset

    def read_range(
        self, start_line: int, end_line: int, *, max_bytes: Optional[int] = None
    ) -> bytes:
        """Return the original bytes of lines ``start_line``..``end_line``.

        Both ends are inclusive.  A line's terminating LF is part of its bytes;
        the final line of a segment may have no LF.  Only blocks that cover the
        requested span are decompressed, and the range is rejected up front if
        it would exceed ``max_bytes``.
        """
        start_offset, end_offset = self._resolve_bounds(
            start_line, end_line, max_bytes
        )

        first_block = bisect.bisect_right(self._block_starts, start_offset) - 1
        last_block = bisect.bisect_right(self._block_starts, end_offset - 1) - 1
        if first_block < 0 or last_block >= len(self.entries):
            raise LineIndexCorrupt("line index cannot map the requested byte range")

        parts: list[bytes] = []
        for index in range(first_block, last_block + 1):
            block = self.read_block(index)
            block_start = self._block_starts[index]
            block_end = block_start + len(block)
            slice_start = max(start_offset, block_start) - block_start
            slice_end = min(end_offset, block_end) - block_start
            if slice_end > slice_start:
                parts.append(block[slice_start:slice_end])
        data = b"".join(parts)
        if len(data) != end_offset - start_offset:
            raise LineIndexCorrupt("line index range did not reconstruct the expected bytes")
        return data
