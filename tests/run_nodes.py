"""Parallel VPNGate node tester: one sing-box per node, official CLI per node."""
from __future__ import annotations

import concurrent.futures as cf
import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

WORKERS = int(os.environ.get("WORKERS", "8"))
SHARD = int(os.environ.get("SHARD", "1"))
TOTAL_SHARDS = int(os.environ.get("TOTAL_SHARDS", "6"))
MODEL = os.environ.get("MODEL", "opencode/deepseek-v4.1-flash-free")
PROMPT = "请用一句话说明你能做什么。只回复这一句。"
ANSI = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

cfg = json.loads(Path("/tmp/sb.json").read_text(encoding="utf-8"))
endpoints = cfg["endpoints"]
mine = [e for i, e in enumerate(endpoints) if i % TOTAL_SHARDS == SHARD - 1]
print(f"SHARD {SHARD}: assigned {len(mine)} of {len(endpoints)} endpoints", flush=True)


def wait_port(port: int, proc: subprocess.Popen, deadline: float) -> bool:
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), 1):
                return True
        except OSError:
            if proc.poll() is not None:
                return False
            time.sleep(0.4)
    return False


def curl(port: int, url: str, extra: list[str], timeout: int = 15):
    return subprocess.run(
        ["curl", "-s", "-m", str(timeout), "-x", f"http://127.0.0.1:{port}", *extra, url],
        capture_output=True, text=True,
    )


def run_node(idx: int, ep: dict) -> dict:
    port = 19000 + idx
    cfg_path = f"/tmp/n{SHARD}_{idx}.json"
    Path(cfg_path).write_text(json.dumps({
        "log": {"level": "warn"},
        "endpoints": [ep],
        "outbounds": [{"type": "direct", "tag": "direct"}],
        "inbounds": [{"type": "mixed", "tag": "in", "listen": "127.0.0.1", "listen_port": port}],
        "route": {"final": ep["tag"]},
    }), encoding="utf-8")
    log = open(f"/tmp/n{SHARD}_{idx}.log", "wb")
    proc = subprocess.Popen(["sing-box", "run", "-c", cfg_path],
                            stdout=subprocess.DEVNULL, stderr=log)
    rec = {"shard": SHARD, "tag": ep["tag"], "server": ep.get("server"),
           "exit_ip": "", "ipify": "000", "verdict": "TUNNEL_DOWN", "text": ""}
    try:
        if not wait_port(port, proc, time.time() + 25):
            return rec
        code = "000"
        deadline = time.time() + 50
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            r = curl(port, "https://api.ipify.org", ["-o", "/dev/null", "-w", "%{http_code}"], timeout=8)
            code = (r.stdout or "").strip() or "000"
            if code == "200":
                break
            time.sleep(2)
        rec["ipify"] = code
        if code != "200":
            rec["verdict"] = "NO_EGRESS"
            return rec
        rec["exit_ip"] = curl(port, "https://api.ipify.org/?format=text", []).stdout.strip()[:40]
        env = dict(os.environ,
                   HTTPS_PROXY=f"http://127.0.0.1:{port}",
                   HTTP_PROXY=f"http://127.0.0.1:{port}",
                   ALL_PROXY=f"http://127.0.0.1:{port}",
                   NO_PROXY="127.0.0.1,localhost",
                   OPENCODE_DISABLE_AUTOUPDATE="1", NO_COLOR="1")
        try:
            cp = subprocess.run(["opencode", "run", "-m", MODEL, PROMPT],
                                capture_output=True, text=True, timeout=120, env=env)
            raw, rc = (cp.stdout or "") + (cp.stderr or ""), cp.returncode
        except subprocess.TimeoutExpired:
            raw, rc = "", 124
        clean = ANSI.sub("", raw).replace("\r", "")
        rec["text"] = " ".join(clean.split())[-220:]
        if rc == 0 and len(clean.strip()) > 25:
            rec["verdict"] = "OK200"
        elif "Free usage limit" in clean:
            rec["verdict"] = "429"
        elif "Model unavailable" in clean:
            rec["verdict"] = "MODEL_BAD"
        elif "ECONNREFUSED" in clean:
            rec["verdict"] = "ECONNREFUSED"
        elif "ECONNRESET" in clean:
            rec["verdict"] = "ECONNRESET"
        elif rc == 124:
            rec["verdict"] = "TIMEOUT"
        else:
            rec["verdict"] = f"FAIL rc={rc}"
        return rec
    finally:
        proc.terminate()
        try:
            proc.wait(5)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()


results: list[dict] = []
start = time.time()
with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = {pool.submit(run_node, i, ep): ep for i, ep in enumerate(mine)}
    for fut in cf.as_completed(futures):
        try:
            rec = fut.result()
        except Exception as exc:  # noqa: BLE001
            rec = {"verdict": f"EXC {exc!r}"}
        results.append(rec)
        print(json.dumps(rec, ensure_ascii=False), flush=True)

Path(f"/tmp/results_{SHARD}.json").write_text(
    json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
summary: dict[str, int] = {}
for rec in results:
    summary[rec["verdict"]] = summary.get(rec["verdict"], 0) + 1
print(f"SUMMARY shard={SHARD} nodes={len(results)} elapsed={int(time.time() - start)}s {summary}", flush=True)
sys.exit(0)
