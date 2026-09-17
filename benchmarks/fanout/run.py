"""Run isolated benchmark steps inside existing Rex-managed Slurm allocations."""

import concurrent.futures as cf
import json
import os
import urllib.error
from pathlib import Path
import subprocess
import time
import urllib.request

PLACEMENT = os.environ.get("BENCH_MIDDLE_PLACEMENT", "gpu")
AVOID_SAME_HOST = os.environ.get("BENCH_AVOID_SAME_HOST", "0") == "1"
ROOT = Path(
    "/gscratch/ark/graf/tmp/litecast-fanout-"
    + PLACEMENT
    + "-"
    + os.environ.get("BENCH_PAYLOAD_BYTES", "adapter")
    + "-"
    + time.strftime("%Y%m%d-%H%M%S")
)
ROOT.mkdir()
SCRIPT = "/gscratch/ark/graf/litecast/benchmarks/fanout/run-node.sh"
ADAPTER = "/gscratch/ark/graf/prime-rl-shardcast/outputs/toy-f185ab72e55c4e49946b3aca4d5e5304/training/run_default/broadcasts/step_40"
processes = []
handles = []
rows = []


def launch(job, role, index):
    log = open(ROOT / (role + "-" + str(index) + ".log"), "w")
    handles.append(log)
    processes.append(
        subprocess.Popen(
            ["srun", "--jobid=" + str(job), "--overlap", "-N1", "-n1", "bash", SCRIPT, role, str(ROOT), str(index), ADAPTER],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    )


def wait(name):
    deadline = time.monotonic() + 180
    while not (ROOT / (name + ".json")).exists():
        if (ROOT / "stop").exists():
            raise RuntimeError("Benchmark stopped")
        if time.monotonic() > deadline:
            raise TimeoutError(name)
        time.sleep(1)
    return json.loads((ROOT / (name + ".json")).read_text())


def post(url, body):
    req = urllib.request.Request(url + "/run", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    try:
        return json.load(urllib.request.urlopen(req, timeout=120))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(exc.read().decode()) from exc


print(str(ROOT), flush=True)
try:
    launch(40264348, "origin", 0)
    info = wait("origin")
    middle_jobs = [40264348, 40264354, 40264355, 40264356] if PLACEMENT == "gpu" else [40264330, 40264332, 40264333, 40264334]
    for i, job in enumerate(middle_jobs):
        launch(job, "middle", i)
    for i, job in enumerate([40264352, 40264354, 40264355, 40264356]):
        launch(job, "client", i)
    middles = [wait("middle-" + str(i)) for i in range(4)]
    clients = [wait("client-" + str(i)) for i in range(4)]
    (ROOT / "topology.json").write_text(
        json.dumps(
            {
                "origin": info,
                "middles": middles,
                "clients": clients,
                "middle_placement": PLACEMENT,
                "assignment": "rotated round-robin, avoid same host" if AVOID_SAME_HOST else "round-robin, same-host allowed",
            },
            indent=2,
        )
    )
    ucxx_failed = False
    for m in [1, 2, 4]:
        for c in [1, 2, 4, 8]:
            for repeat in range(4):
                for mode in ["http", "ucxx"] if repeat % 2 == 0 else ["ucxx", "http"]:
                    if mode == "ucxx" and ucxx_failed:
                        continue
                    assignments = {}
                    for i in range(c):
                        choice = (i + 1) % m if AVOID_SAME_HOST else i % m
                        if AVOID_SAME_HOST and middles[choice]["host"] == clients[i % 4]["host"]:
                            choice = next((j for j in range(m) if middles[j]["host"] != clients[i % 4]["host"]), choice)
                        assignments.setdefault(i % 4, []).append({"id": i, "url": middles[choice]["url"]})
                    start_at = time.time() + 0.5
                    with cf.ThreadPoolExecutor(max_workers=4) as pool:
                        futures = [
                            pool.submit(post, clients[host]["url"], {"mode": mode, "start_at": start_at, "assignments": items})
                            for host, items in assignments.items()
                        ]
                        try:
                            samples = [s for f in futures for s in f.result()["clients"]]
                        except Exception as exc:
                            row = {"middles": m, "clients": c, "transport": mode, "repeat": repeat, "error": repr(exc)}
                            rows.append(row)
                            (ROOT / "results.json").write_text(json.dumps(rows, indent=2))
                            print(json.dumps(row), flush=True)
                            if mode == "ucxx":
                                ucxx_failed = True
                                continue
                            raise
                    elapsed = max(s["end"] for s in samples) - min(s["start"] for s in samples)
                    row = {
                        "middles": m,
                        "clients": c,
                        "transport": mode,
                        "repeat": repeat,
                        "warmup": repeat == 0,
                        "seconds": elapsed,
                        "aggregate_GiB_s": c * info["bytes"] / 1024**3 / elapsed,
                        "start_skew_s": max(s["start"] for s in samples) - min(s["start"] for s in samples),
                        "samples": samples,
                    }
                    rows.append(row)
                    (ROOT / "results.json").write_text(json.dumps(rows, indent=2))
                    print(json.dumps({k: v for k, v in row.items() if k != "samples"}), flush=True)
finally:
    (ROOT / "stop").touch()
    for process in processes:
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.terminate()
    for handle in handles:
        handle.close()
