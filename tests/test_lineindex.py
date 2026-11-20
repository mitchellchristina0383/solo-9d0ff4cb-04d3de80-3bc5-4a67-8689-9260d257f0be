"""Line-index tests: block-crossing ranges, dictionary switches and recovery."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from pathlib import Path

from logarchive import lineindex, storage
from logarchive.archive import (
    ArchiveService,
    ConflictError,
    RangeRequestError,
)
from logarchive.config import Config

DICT_V1 = (b"timestamp= severity=INFO service=api-gateway user_id= request_id= "
           b"endpoint= method=GET status= duration_ms= message=") * 2
DICT_V2 = (b"ts= level=INFO ERROR WARN svc=gateway uid= rid= path= verb= code= "
           b"latency= msg= traceparent=") * 2


def make_lines(count: int, *, trailing_newline: bool = True) -> bytes:
    data = b"".join(
        f"2026-11-20T00:00:{i % 60:02d}Z INFO request {i} path=/v1/items/{i % 17} "
        f"user=u{i % 31} message=request completed normally\n".encode()
        for i in range(count)
    )
    if not trailing_newline and data.endswith(b"\n"):
        data = data[:-1]
    return data


def split_lines(payload: bytes) -> list[bytes]:
    if not payload:
        return []
    lines = payload.split(b"\n")
    if payload.endswith(b"\n"):
        lines = lines[:-1]
    return lines


def expected_slice(payload: bytes, start_line: int, end_line: int) -> bytes:
    """Bytes from the start of ``start_line`` through the end of ``end_line``."""
    lines = split_lines(payload)
    start = sum(len(line) + 1 for line in lines[: start_line - 1])
    if end_line < len(lines):
        end = sum(len(line) + 1 for line in lines[: end_line])
    else:
        end = len(payload)
    return payload[start:end]


class LineIndexSemanticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="logarchive-lines-"))

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def roundtrip(self, payload: bytes, block_size: int, ranges) -> None:
        path = self.tmp / "x.idx"
        builder = lineindex.LineIndexBuilder(path, block_size)
        step = max(1, block_size // 3)
        for offset in range(0, len(payload), step):
            builder.feed(payload[offset : offset + step])
        builder.finalize(
            original_sha256="0" * 64, original_size=len(payload)
        )
        with lineindex.LineIndexReader(
            path, expected_sha256="0" * 64, expected_size=len(payload)
        ) as reader:
            self.assertEqual(reader.total_lines, len(split_lines(payload)))
            for start, end in ranges:
                self.assertEqual(
                    reader.read_range(start, end),
                    expected_slice(payload, start, end),
                    f"range {start}-{end} with block size {block_size}",
                )

    def test_line_numbering_edges(self) -> None:
        self.roundtrip(b"", 16, [])
        # A single line without a trailing newline is still line 1.
        self.roundtrip(b"only line", 16, [(1, 1)])
        self.roundtrip(b"a\n", 16, [(1, 1)])
        self.roundtrip(b"a\nb", 16, [(1, 1), (2, 2), (1, 2)])
        # Blank lines count.
        self.roundtrip(b"\n\n\n", 16, [(1, 1), (2, 3), (1, 3)])
        self.roundtrip(b"a\n\nb\n", 16, [(1, 1), (2, 2), (3, 3), (1, 3)])
        self.roundtrip(b"a\r\nb\r\n", 16, [(1, 2)])

    def test_ranges_match_reference_across_block_boundaries(self) -> None:
        payload = make_lines(50)
        ranges = [(1, 1), (1, 2), (2, 3), (7, 7), (1, 50), (49, 50), (13, 27)]
        for size in (16, 17, 31, 64, 1000):
            self.roundtrip(payload, size, ranges)

    def test_final_line_without_newline(self) -> None:
        payload = make_lines(20, trailing_newline=False)
        self.roundtrip(payload, 37, [(1, 20), (20, 20), (19, 20)])
        with self.assertRaises(lineindex.LineRangeError):
            # There is no line 21.
            path = self.tmp / "y.idx"
            builder = lineindex.LineIndexBuilder(path, 37)
            builder.feed(payload)
            builder.finalize(original_sha256="1" * 64, original_size=len(payload))
            with lineindex.LineIndexReader(path) as reader:
                reader.read_range(21, 21)

    def test_out_of_bounds_and_inverted_ranges_are_rejected(self) -> None:
        payload = make_lines(10)
        path = self.tmp / "z.idx"
        builder = lineindex.LineIndexBuilder(path, 32)
        builder.feed(payload)
        builder.finalize(original_sha256="2" * 64, original_size=len(payload))
        with lineindex.LineIndexReader(path) as reader:
            for start, end in [(0, 1), (3, 2), (1, 11), (11, 11)]:
                with self.assertRaises(lineindex.LineRangeError):
                    reader.read_range(start, end)

    def test_only_touched_blocks_are_decompressed(self) -> None:
        payload = make_lines(400)
        path = self.tmp / "b.idx"
        builder = lineindex.LineIndexBuilder(path, 256)
        builder.feed(payload)
        builder.finalize(original_sha256="3" * 64, original_size=len(payload))
        with lineindex.LineIndexReader(path) as reader:
            self.assertGreater(reader.header["block_count"], 20)
            reader.read_range(5, 6)
            # Locating two lines and reading their span touches a handful of
            # blocks, never the whole segment.
            self.assertLess(reader.blocks_decompressed, reader.header["block_count"] // 4)
            # Repeating the read stays within the same small working set
            # instead of touching every block.
            before = reader.blocks_decompressed
            reader.read_range(5, 6)
            self.assertLessEqual(reader.blocks_decompressed - before, 8)
            self.assertLess(reader.blocks_decompressed, reader.header["block_count"] // 4)

    def test_max_bytes_limit_is_reported_before_materialising(self) -> None:
        payload = make_lines(100)
        path = self.tmp / "m.idx"
        builder = lineindex.LineIndexBuilder(path, 128)
        builder.feed(payload)
        builder.finalize(original_sha256="4" * 64, original_size=len(payload))
        with lineindex.LineIndexReader(path) as reader:
            with self.assertRaises(lineindex.LineRangeTooLarge):
                reader.read_range(1, 100, max_bytes=100)

    def test_checksum_validation_rejects_tampered_blocks_and_tables(self) -> None:
        payload = make_lines(40)
        path = self.tmp / "c.idx"
        builder = lineindex.LineIndexBuilder(path, 64)
        builder.feed(payload)
        builder.finalize(original_sha256="5" * 64, original_size=len(payload))
        raw = bytearray(path.read_bytes())
        raw[len(lineindex.MAGIC) + 2] ^= 0xFF
        path.write_bytes(bytes(raw))
        with lineindex.LineIndexReader(path) as reader:
            with self.assertRaises(lineindex.LineIndexCorrupt):
                reader.read_range(1, 5)

    def test_header_size_mismatch_is_rejected(self) -> None:
        payload = make_lines(5)
        path = self.tmp / "h.idx"
        builder = lineindex.LineIndexBuilder(path, 64)
        builder.feed(payload)
        builder.finalize(original_sha256="6" * 64, original_size=len(payload))
        with self.assertRaises(lineindex.LineIndexCorrupt):
            lineindex.LineIndexReader(path, expected_size=len(payload) + 1)
        with self.assertRaises(lineindex.LineIndexCorrupt):
            lineindex.LineIndexReader(path, expected_sha256="7" * 64)


class ServiceLineIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="logarchive-svc-lines-"))
        self.service = self.make_service()

    def tearDown(self) -> None:
        self.service.stop_worker()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_service(self, **overrides) -> ArchiveService:
        options = dict(
            data_dir=self.tmp,
            worker_enabled=False,
            recompress_delay_ms=0,
            line_index_block_size=128,
        )
        options.update(overrides)
        return ArchiveService(Config(**options))

    def upload(self, name: str, payload: bytes) -> dict:
        path = self.tmp / name
        path.write_bytes(payload)
        return self.service.ingest_file(path, name)

    def test_fragment_download_requires_a_completed_index(self) -> None:
        payload = make_lines(20)
        segment = self.upload("s.log", payload)
        self.assertEqual(self.service.index_status(segment["number"])["state"], "absent")
        with self.assertRaises(ConflictError) as ctx:
            self.service.read_line_range(segment["number"], 1, 1)
        self.assertEqual(ctx.exception.code, "index_not_ready")

        summary = self.service.build_line_index(segment["number"])
        self.assertEqual(summary["state"], "ready")
        self.assertEqual(summary["total_lines"], 20)
        data, length = self.service.read_line_range(segment["number"], 4, 8)
        self.assertEqual(data, expected_slice(payload, 4, 8))
        self.assertEqual(length, len(data))

    def test_cross_block_range_is_exact(self) -> None:
        payload = make_lines(300)
        segment = self.upload("big.log", payload)
        self.service.build_line_index(segment["number"])
        summary = self.service.index_status(segment["number"])
        self.assertGreater(summary["block_count"], 10)
        data, _ = self.service.read_line_range(segment["number"], 150, 260)
        self.assertEqual(data, expected_slice(payload, 150, 260))

    def test_index_survives_dictionary_switch_and_old_dictionary_deletion(self) -> None:
        payload = make_lines(40)
        v1 = self.service.create_dictionary("v1", DICT_V1)
        v2 = self.service.create_dictionary("v2", DICT_V2)
        self.service.activate_dictionary(v1["id"])
        segment = self.upload("switch.log", payload)
        self.service.build_line_index(segment["number"])

        self.service.activate_dictionary(v2["id"])
        self.service.run_recompression_once(limit=10)
        row = self.service.get_segment(segment["number"])
        self.assertEqual(row["dictionary_id"], v2["id"])
        self.assertEqual(row["line_index_state"], "ready")

        self.service.delete_dictionary(v1["id"])
        data, _ = self.service.read_line_range(segment["number"], 1, 40)
        self.assertEqual(data, payload)
        self.assertEqual(self.service.index_status(segment["number"])["state"], "ready")

    def test_index_is_shared_by_identical_content_and_reused(self) -> None:
        payload = make_lines(30)
        first = self.upload("a.log", payload)
        second = self.upload("b.log", payload)
        summary = self.service.build_line_index(first["number"])
        self.assertEqual(
            self.service.index_status(second["number"])["state"], "ready"
        )
        again = self.service.build_line_index(second["number"])
        self.assertEqual(again["created_at"], summary["created_at"])

    def test_rebuild_after_dictionary_switch_before_index_existed(self) -> None:
        payload = make_lines(25)
        v1 = self.service.create_dictionary("v1", DICT_V1)
        v2 = self.service.create_dictionary("v2", DICT_V2)
        self.service.activate_dictionary(v1["id"])
        segment = self.upload("late.log", payload)
        self.service.activate_dictionary(v2["id"])
        self.service.run_recompression_once(limit=10)
        summary = self.service.build_line_index(segment["number"])
        self.assertEqual(summary["state"], "ready")
        self.assertEqual(
            self.service.read_line_range(segment["number"], 1, 25)[0], payload
        )

    def test_ready_index_survives_restart_and_remains_usable(self) -> None:
        payload = make_lines(100)
        segment = self.upload("restart.log", payload)
        self.service.build_line_index(segment["number"])

        restarted = self.make_service()
        try:
            self.assertEqual(
                restarted.index_status(segment["number"])["state"], "ready"
            )
            data, _ = restarted.read_line_range(segment["number"], 33, 77)
            self.assertEqual(data, expected_slice(payload, 33, 77))
            self.assertEqual(
                restarted.spool_segment(segment["number"])[1].read_bytes(), payload
            )
        finally:
            restarted.stop_worker()

    def test_interrupted_build_is_cleared_and_can_be_restarted(self) -> None:
        payload = make_lines(20)
        segment = self.upload("interrupted.log", payload)
        sha = segment["original_sha256"]
        path = self.service._index_path(sha)
        # A crash leaves a 'building' row and a partial temp file, but the
        # content-addressed destination was never installed.
        with self.service.db.write() as conn:
            conn.execute(
                """
                INSERT INTO line_indexes
                    (original_sha256, original_size, state, updated_at)
                VALUES (?, ?, 'building', ?)
                """,
                (sha, len(payload), "2026-11-20T00:00:00Z"),
            )
        path.write_bytes(lineindex.MAGIC + b"partial garbage")

        restarted = self.make_service()
        try:
            status = restarted.index_status(segment["number"])
            self.assertEqual(status["state"], "failed")
            self.assertFalse(path.exists())
            with self.assertRaises(ConflictError):
                restarted.read_line_range(segment["number"], 1, 1)

            summary = restarted.build_line_index(segment["number"])
            self.assertEqual(summary["state"], "ready")
            self.assertEqual(
                restarted.read_line_range(segment["number"], 1, 20)[0], payload
            )
        finally:
            restarted.stop_worker()

    def test_completed_but_unrecorded_index_is_adopted_after_restart(self) -> None:
        payload = make_lines(20)
        segment = self.upload("crash-after-install.log", payload)
        sha = segment["original_sha256"]
        path = self.service._index_path(sha)
        builder = lineindex.LineIndexBuilder(
            path, self.service.config.line_index_block_size
        )
        builder.feed(payload)
        builder.finalize(original_sha256=sha, original_size=len(payload))
        # Simulate a crash after the atomic install but before the DB update.
        with self.service.db.write() as conn:
            conn.execute(
                """
                INSERT INTO line_indexes
                    (original_sha256, original_size, state, updated_at)
                VALUES (?, ?, 'building', ?)
                """,
                (sha, len(payload), "2026-11-20T00:00:00Z"),
            )

        restarted = self.make_service()
        try:
            status = restarted.index_status(segment["number"])
            self.assertEqual(status["state"], "ready")
            self.assertEqual(status["total_lines"], 20)
        finally:
            restarted.stop_worker()

    def test_corrupt_index_is_reported_and_marked_failed(self) -> None:
        payload = make_lines(20)
        segment = self.upload("corrupt.log", payload)
        self.service.build_line_index(segment["number"])
        path = self.service._index_path(segment["original_sha256"])
        raw = bytearray(path.read_bytes())
        raw[len(lineindex.MAGIC) + 1] ^= 0xFF
        path.write_bytes(bytes(raw))

        with self.assertRaises(storage.StorageError):
            self.service.read_line_range(segment["number"], 1, 3)
        self.assertEqual(self.service.index_status(segment["number"])["state"], "failed")
        # The whole-segment download does not depend on the index.
        row, temp = self.service.spool_segment(segment["number"])
        try:
            self.assertEqual(temp.read_bytes(), payload)
        finally:
            os.unlink(temp)

    def test_range_size_limit_is_enforced(self) -> None:
        payload = make_lines(500)
        service = self.make_service(max_line_range_bytes=128)
        try:
            path = self.tmp / "limited.log"
            path.write_bytes(payload)
            segment = service.ingest_file(path, "limited.log")
            service.build_line_index(segment["number"])
            with self.assertRaises(RangeRequestError) as ctx:
                service.read_line_range(segment["number"], 1, 500)
            self.assertEqual(ctx.exception.status, 413)
            self.assertEqual(ctx.exception.code, "line_range_too_large")
        finally:
            service.stop_worker()

    def test_empty_segment_has_no_lines(self) -> None:
        segment = self.upload("empty.log", b"")
        summary = self.service.build_line_index(segment["number"])
        self.assertEqual(summary["state"], "ready")
        self.assertEqual(summary["total_lines"], 0)
        with self.assertRaises(RangeRequestError) as ctx:
            self.service.read_line_range(segment["number"], 1, 1)
        self.assertEqual(ctx.exception.status, 416)


if __name__ == "__main__":
    unittest.main()
