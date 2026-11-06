#!/usr/bin/env python3
"""macOS-friendly cluster daemon (double-fork)."""
from __future__ import annotations

import os
import pathlib
import socket
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUN = ROOT / ".run"


def stop_old() -> None:
    names = [
        "gateway",
        "storage-0", "storage-1", "storage-2", "storage-3", "storage-4", "storage-5",
        "meta-0", "meta-1", "meta-2", "meta", "supervisor",
    ]
    for name in names:
        pf = RUN / f"{name}.pid"
        if not pf.exists():
            continue
        try:
            pid = int(pf.read_text().strip())
            os.kill(pid, 15)
        except Exception:
            pass
        pf.unlink(missing_ok=True)
    time.sleep(0.5)


def wait_port(port: int, timeout: float = 20.0, hint_log=None) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), 0.2)
            s.close()
            return
        except OSError:
            time.sleep(0.2)
    msg = f"port {port} not ready"
    if hint_log:
        path = RUN / hint_log
        if path.exists():
            try:
                tail = path.read_text(errors="replace").splitlines()[-40:]
                msg += f"\n--- tail {hint_log} ---\n" + "\n".join(tail)
            except OSError:
                pass
    raise RuntimeError(msg)


def start(name: str, cmd: list[str], logname: str) -> subprocess.Popen:
    # 截断日志，避免旧崩溃堆叠难排查
    log = open(RUN / logname, "wb", buffering=0)
    p = subprocess.Popen(
        cmd,
        stdout=log,
        stderr=subprocess.STDOUT,
        cwd=ROOT,
        start_new_session=True,
    )
    (RUN / f"{name}.pid").write_text(str(p.pid))
    return p


def main() -> int:
    RUN.mkdir(exist_ok=True)
    stop_old()

    # parent exits immediately; child becomes daemon
    if os.fork() > 0:
        return 0
    os.setsid()
    if os.fork() > 0:
        os._exit(0)

    os.chdir(ROOT)
    os.umask(0)
    # stdin/out/err 全部脱离父管道，否则 start-cluster | tail 会因 daemon 占着
    # 写端永远等不到 EOF。
    with open(os.devnull, "rb", 0) as devnull, open(os.devnull, "wb", 0) as devnull_w:
        os.dup2(devnull.fileno(), 0)
        os.dup2(devnull_w.fileno(), 1)
        os.dup2(devnull_w.fileno(), 2)

    procs = [
        start("meta-0", [str(ROOT / "build/meta/meta_server"), "conf/bootstrap.conf"], "meta-0.log"),
        start("meta-1", [str(ROOT / "build/meta/meta_server"), "conf/meta-1.conf"], "meta-1.log"),
        start("meta-2", [str(ROOT / "build/meta/meta_server"), "conf/meta-2.conf"], "meta-2.log"),
        start(
            "storage-0",
            [str(ROOT / "build/storage/storage_node"), "conf/storage.conf"],
            "storage-0.log",
        ),
        start(
            "storage-1",
            [str(ROOT / "build/storage/storage_node"), "conf/storage-1.conf"],
            "storage-1.log",
        ),
        start(
            "storage-2",
            [str(ROOT / "build/storage/storage_node"), "conf/storage-2.conf"],
            "storage-2.log",
        ),
        start(
            "storage-3",
            [str(ROOT / "build/storage/storage_node"), "conf/storage-3.conf"],
            "storage-3.log",
        ),
        start(
            "storage-4",
            [str(ROOT / "build/storage/storage_node"), "conf/storage-4.conf"],
            "storage-4.log",
        ),
        start(
            "storage-5",
            [str(ROOT / "build/storage/storage_node"), "conf/storage-5.conf"],
            "storage-5.log",
        ),
    ]
    port_logs = {
        8500: "meta-0.log",
        8501: "meta-1.log",
        8502: "meta-2.log",
        9100: "storage-0.log",
        9101: "storage-1.log",
        9102: "storage-2.log",
        9103: "storage-3.log",
        9104: "storage-4.log",
        9105: "storage-5.log",
    }
    for port, logname in port_logs.items():
        wait_port(port, hint_log=logname)

    gw = RUN / "compute-gateway"
    procs.append(
        start(
            "gateway",
            [
                str(gw),
                "-listen",
                "0.0.0.0:19000",
                "-storages",
                "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,"
                "127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105",
                "-metas",
                "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502",
            ],
            "gateway.log",
        )
    )
    wait_port(19000, hint_log="gateway.log")
    (RUN / "supervisor.pid").write_text(str(os.getpid()))

    while True:
        if not any(p.poll() is None for p in procs):
            break
        time.sleep(2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
