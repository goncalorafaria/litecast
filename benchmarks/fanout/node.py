"""Isolated cache fan-out benchmark; no inference or production services modified."""

import concurrent.futures as cf
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from litecast import ClientNode, OriginServer
from litecast.middle_node import MiddleNode
from litecast.registry.protocol import pack_adapter
from litecast.transport.ucxx import UCXXEventLoopExecutor

root = Path(sys.argv[2])
role = sys.argv[1]
ident = sys.argv[3]
address = os.environ["BENCH_IB_ADDRESS"]
payload_bytes = int(os.environ.get("BENCH_PAYLOAD_BYTES", "0"))
cache = max(512 * 1024**2, payload_bytes + 64 * 1024**2)


def port():
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def write(name, value):
    p = root / (name + ".json")
    temp = p.with_suffix(".tmp")
    temp.write_text(json.dumps(value))
    temp.replace(p)


stop = threading.Event()
service = None
executor = None
clients = {}
if role == "origin":
    adapter = Path(sys.argv[4])
    if payload_bytes:
        payload = os.urandom(payload_bytes)
        source = "synthetic random bytes (not a model checkpoint)"
    else:
        payload = pack_adapter(adapter, cache)
        source = str(adapter)
    service = OriginServer(
        tempfile.mkdtemp(), port=0, transport="ucxx", ucxx_port=port(), ucxx_interface="ib0", ram_cache_bytes=cache, disk_mirror=False
    )
    version = service.broadcast_buffer(payload, 16 * 1024**2, disk_mirror=False)
    info = {
        "url": f"http://{address}:{service.port}",
        "version": version,
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "source": source,
        "host": socket.gethostname(),
    }
    write("origin", info)
elif role == "middle":
    info = json.loads((root / "origin.json").read_text())
    service = MiddleNode(
        [info["url"]],
        tempfile.mkdtemp(),
        port=0,
        transport="ucxx",
        ucxx_port=port(),
        ucxx_interface="ib0",
        ram_cache_bytes=cache,
        disk_mirror=False,
        check_interval=1,
    )
    service.client.transport = "http"
    deadline = time.monotonic() + 300
    while info["version"] not in service.processed_versions:
        if time.monotonic() > deadline:
            raise TimeoutError("Middle failed to populate")
        time.sleep(0.1)
    write("middle-" + ident, {"url": f"http://{address}:{service.port}", "host": socket.gethostname()})
else:
    info = json.loads((root / "origin.json").read_text())
    executor = UCXXEventLoopExecutor()

    def transfer(index, url, mode, start_at):
        key = (index, url, mode)
        if key not in clients:
            clients[key] = ClientNode(
                [url], tempfile.mkdtemp(), transport=mode, max_version_bytes=cache, ucxx_executor=executor if mode == "ucxx" else None
            )
        time.sleep(max(0, start_at - time.time()))
        start = time.time()
        data = clients[key].download_version_buffer(info["version"])
        end = time.time()
        if data is None or hashlib.sha256(data).hexdigest() != info["sha256"]:
            raise ValueError("Payload checksum mismatch")
        return {
            "host": socket.gethostname(),
            "client": index,
            "source": url,
            "start": start,
            "end": end,
            "seconds": end - start,
            "verified": True,
        }

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/stop":
                    stop.set()
                    result = {"stopped": True}
                else:
                    with cf.ThreadPoolExecutor(max_workers=len(body["assignments"])) as pool:
                        fs = [pool.submit(transfer, x["id"], x["url"], body["mode"], body["start_at"]) for x in body["assignments"]]
                        result = {"clients": [f.result() for f in fs]}
                status = 200
            except Exception as exc:
                status = 500
                result = {"error": repr(exc)}
            data = json.dumps(result).encode()
            self.send_response(status)
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    write("client-" + ident, {"url": f"http://{address}:{server.server_port}", "host": socket.gethostname()})
try:
    deadline = time.monotonic() + 1800
    while not stop.is_set() and not (root / "stop").exists() and time.monotonic() < deadline:
        time.sleep(0.5)
finally:
    if service:
        service.shutdown()
    if role == "client":
        server.shutdown()
        for client in clients.values():
            client.close()
    if executor:
        executor.close()
