#!/usr/bin/env python3
import os, pathlib, sys
ROOT = pathlib.Path(__file__).resolve().parents[1]
RUN = ROOT / ".run"
bin_path = RUN / "skv-web"
if not bin_path.exists():
    sys.exit("missing .run/skv-web")
if os.fork() > 0:
    sys.exit(0)
os.setsid()
if os.fork() > 0:
    os._exit(0)
os.chdir(ROOT)
log = open(RUN / "web.log", "ab", buffering=0)
os.dup2(log.fileno(), 1)
os.dup2(log.fileno(), 2)
os.execv(str(bin_path), [
    str(bin_path),
    "-listen", "0.0.0.0:19100",
    "-metas", "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502",
    "-storages", "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105",
    "-gateway", "127.0.0.1:19000",
    "-run-dir", str(RUN),
    "-root", str(ROOT),
])
