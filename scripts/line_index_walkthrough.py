"""Line-index acceptance walkthrough against a live deployment.

    python3 scripts/line_index_walkthrough.py --url http://127.0.0.1:8080

Exercises the operator workflow: build an index on demand, extract a line range
that crosses several index blocks, keep extracting while a dictionary switch
recompresses the segment, keep extracting after the old dictionary is deleted,
and survive a dictionary switch + service restart.

With ``--restart-cmd "<command>"`` the script also restarts the service in the
middle of the backlog and re-checks the indexed range afterwards.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from acceptance_walkthrough import (  # noqa: E402  (same directory)
    DICT_V1,
    DICT_V2,
    Checker,
    api,
    build_index,
    download,
    download_lines,
    expected_lines,
    index_status,
    lines_of,
    request,
    upload_dictionary,
    upload_segment,
    wait_drained,
)


def make_payload(seed: int, line_count: int = 9000) -> bytes:
    random.seed(seed)
    out = []
    for i in range(1, line_count + 1):
        out.append(
            f"2026-11-20T00:00:{i % 60:02d}Z INFO request_id=req-{seed}-{i} "
            f"endpoint=/v1/items/{i % 211} user=u{(i * 13) % 997} "
            f"token={random.getrandbits(64):016x} "
            f"message=request completed for user u{(i * 13) % 997}\n"
        )
    return "".join(out).encode()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url", default=os.environ.get("ARCHIVE_URL", "http://127.0.0.1:8080")
    )
    parser.add_argument("--restart-cmd", default="")
    parser.add_argument("--lines", type=int, default=9000)
    args = parser.parse_args()
    url = args.url
    checks = Checker()
    tmp = Path(os.environ.get("LINE_WALKTHROUGH_DIR", "/tmp/line-index-walkthrough"))
    tmp.mkdir(parents=True, exist_ok=True)

    print(f"workdir: {tmp}")
    v1 = upload_dictionary(url, f"idx-v1-{uuid.uuid4().hex[:6]}", DICT_V1, activate=True)
    payload = make_payload(7, args.lines)
    path = tmp / "line-walkthrough.log"
    path.write_bytes(payload)
    segment = upload_segment(url, path, payload)
    number = segment["number"]
    total = len(lines_of(payload))
    print(f"uploaded segment #{number} ({len(payload)} bytes, {total} lines)")

    checks.check(
        index_status(url, number)["state"] == "absent",
        "new segment has no line index",
    )
    status, _, raw = request(url, "GET", f"/api/segments/{number}/lines?start=1&end=2")
    checks.check(status == 409, "line download refused before indexing", f"got {status}")
    checks.check(
        json.loads(raw)["error"]["code"] == "index_not_ready",
        "refusal names the missing index",
    )

    summary = build_index(url, number)
    checks.check(summary["state"] == "ready", "index built on demand")
    checks.check(summary["total_lines"] == total, "line count matches the source")
    checks.check(summary["block_count"] > 1, "segment spans multiple index blocks")

    mid_start = total // 2 - 3
    mid_end = total // 2 + 9
    ranges = [
        (1, 1),
        (1, total),
        (total - 2, total),
        (mid_start, mid_end),
        (total // 3, total // 3 + 1),
    ]
    for start, end in ranges:
        checks.check(
            download_lines(url, number, start, end) == expected_lines(payload, start, end),
            f"lines {start}-{end} byte-exact",
        )

    status, headers, _ = request(
        url, "HEAD", f"/api/segments/{number}/lines?start={mid_start}&end={mid_end}"
    )
    checks.check(status == 200, "HEAD validates the range")
    checks.check(
        int(headers.get("Content-Length", -1))
        == len(expected_lines(payload, mid_start, mid_end)),
        "HEAD reports the exact fragment length",
    )

    print("\n-- dictionary switch while the index exists --")
    v2 = upload_dictionary(url, f"idx-v2-{uuid.uuid4().hex[:6]}", DICT_V2, activate=True)
    checks.check(
        download_lines(url, number, mid_start, mid_end)
        == expected_lines(payload, mid_start, mid_end),
        "lines exact immediately after activation",
    )
    status = wait_drained(url)
    checks.check(status["backlog"] == 0, "backlog drained")
    row = api(url, "GET", f"/api/segments/{number}")
    checks.check(row["dictionary_id"] == v2["id"], "segment recompressed onto v2")
    checks.check(row["line_index_state"] == "ready", "index still ready after recompression")
    checks.check(
        download_lines(url, number, mid_start, mid_end)
        == expected_lines(payload, mid_start, mid_end),
        "lines exact after recompression",
    )

    if args.restart_cmd:
        print("\n-- restarting the service --")
        subprocess.run(args.restart_cmd, shell=True, check=False)
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                api(url, "GET", "/healthz")
                break
            except Exception:
                time.sleep(0.5)
        else:
            checks.check(False, "service came back after restart")
        wait_drained(url)
        checks.check(
            index_status(url, number)["state"] == "ready",
            "index reused after restart (no rebuild)",
        )
        checks.check(
            download_lines(url, number, mid_start, mid_end)
            == expected_lines(payload, mid_start, mid_end),
            "lines exact after restart",
        )

    print("\n-- delete old dictionaries --")
    api(url, "POST", f"/api/dictionaries/{v2['id']}/activate")
    status, _, _ = request(url, "DELETE", f"/api/dictionaries/{v1['id']}")
    checks.check(status == 204, "old dictionary deleted", f"got {status}")
    checks.check(
        download_lines(url, number, mid_start, mid_end)
        == expected_lines(payload, mid_start, mid_end),
        "lines exact after the old dictionary is deleted",
    )
    checks.check(download(url, number) == payload, "whole segment still byte-identical")

    print("\n-- summary --")
    print(f"{len(checks.passed)} passed, {len(checks.failed)} failed")
    for failure in checks.failed:
        print(f"  - {failure}")
    return 0 if not checks.failed else 1


if __name__ == "__main__":
    sys.exit(main())
