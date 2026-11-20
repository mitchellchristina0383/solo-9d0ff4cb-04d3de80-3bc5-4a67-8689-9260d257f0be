"""Hard-interruption test for line-index builds.

Starts its own server process on a scratch data directory, uploads a segment,
kills the process with SIGKILL while the index is being written, restarts it,
and checks that:

* the partial index was never exposed (line downloads stay refused),
* startup recovery reports the interrupted build as failed and clears temp files,
* a fresh build succeeds and returns byte-exact line ranges,
* the whole-segment download was never affected.

    python3 scripts/line_index_interruption_test.py
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from acceptance_walkthrough import (  # noqa: E402
    DICT_V1,
    download,
    download_lines,
    expected_lines,
    index_status,
    upload_dictionary,
    upload_segment,
)

LINES = 300_000


def make_payload() -> bytes:
    lines = []
    for i in range(LINES):
        lines.append(
            f"2026-11-20T00:00:{i % 60:02d}Z INFO request={i} "
            f"payload={'x' * 48} endpoint=/v1/items/{i % 401}\n"
        )
    return "".join(lines).encode()


def main() -> int:
    port = int(os.environ.get("LINE_INDEX_INTERRUPT_PORT", "8098"))
    workdir = Path(
        os.environ.get("LINE_INDEX_INTERRUPT_DIR", "/tmp/logarchive-index-interrupt")
    )
    if workdir.exists():
        shutil.rmtree(workdir)
    data = workdir / "data"
    data.mkdir(parents=True)
    log_path = workdir / "server.log"
    url = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "DATA_DIR": str(data),
        "PORT": str(port),
        "HOST": "127.0.0.1",
        "LINE_INDEX_BLOCK_SIZE": "65536",
        "LOG_LEVEL": "INFO",
    }

    def start() -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "logarchive.app"],
            cwd=str(ROOT),
            env=env,
            stdout=open(log_path, "ab"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        for _ in range(200):
            try:
                urllib.request.urlopen(url + "/healthz", timeout=1).read()
                return proc
            except Exception:
                if proc.poll() is not None:
                    raise SystemExit(f"server exited early; see {log_path}")
                time.sleep(0.1)
        raise SystemExit("server did not become healthy")

    failures: list[str] = []

    def check(condition: bool, label: str, detail: str = "") -> None:
        print(("PASS  " if condition else "FAIL  ") + label + (f"  {detail}" if detail else ""))
        if not condition:
            failures.append(label)

    payload = make_payload()
    path = workdir / "segment.log"
    path.write_bytes(payload)

    proc = start()
    try:
        upload_dictionary(url, "v1", DICT_V1, activate=True)
        segment = upload_segment(url, path, payload)
        number = segment["number"]
        print(f"uploaded segment #{number} ({len(payload)} bytes)")

        # Kick off the build in a background thread; watch the server DB until
        # the row is 'building' and a temp file exists, then SIGKILL.
        import threading

        def build() -> None:
            try:
                urllib.request.urlopen(
                    urllib.request.Request(
                        f"{url}/api/segments/{number}/index", method="POST"
                    ),
                    timeout=120,
                ).read()
            except Exception:
                pass

        worker = threading.Thread(target=build, daemon=True)
        worker.start()
        deadline = time.time() + 30
        saw_building = False
        while time.time() < deadline:
            temp_files = list((data / "tmp").glob("line-index-*"))
            status = index_status(url, number)
            if status["state"] == "building" or temp_files:
                saw_building = bool(temp_files)
                break
            time.sleep(0.02)
        check(saw_building, "index build started before the kill")

        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
        print("server killed with SIGKILL")
        partials = list((data / "line-indexes").iterdir())
        check(not partials, "no index file was installed by the killed build", str(partials))

        proc = start()
        print("server restarted")
        time.sleep(0.2)
        status = index_status(url, number)
        check(status["state"] == "failed", "interrupted build marked failed", status["state"])
        check(
            "interrupt" in (status["error"] or "").lower(),
            "failure reason mentions the interruption",
            status["error"] or "",
        )
        check(not list((data / "tmp").iterdir()), "temporary files cleaned on startup")
        check(
            download(url, number) == payload,
            "whole-segment download unaffected by the interrupted build",
        )
        code = 0
        try:
            download_lines(url, number, 1, 2)
        except RuntimeError as exc:
            code = int(str(exc).split("failed ")[1].split(":")[0]) if "failed " in str(exc) else 0
        check(code == 409, "line range refused until a rebuild completes", f"got {code}")

        summary = json.loads(
            urllib.request.urlopen(
                urllib.request.Request(
                    f"{url}/api/segments/{number}/index", method="POST"
                ),
                timeout=120,
            ).read()
        )
        check(summary["state"] == "ready", "rebuild after restart succeeds")
        check(summary["total_lines"] == LINES, "rebuilt line count matches", str(summary["total_lines"]))
        start_line = LINES // 2
        check(
            download_lines(url, number, start_line, start_line + 5)
            == expected_lines(payload, start_line, start_line + 5),
            "lines byte-exact after the rebuild",
        )
        check(
            download_lines(url, number, 1, LINES) == payload,
            "whole-segment line range equals the original after rebuild",
        )
    finally:
        if proc.poll() is None:
            os.kill(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.kill(proc.pid, signal.SIGKILL)

    print("\n" + ("ALL CHECKS PASSED" if not failures else f"FAILURES: {failures}"))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
