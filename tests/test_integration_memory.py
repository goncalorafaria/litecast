from types import SimpleNamespace
from urllib.request import Request, urlopen
import socket
import threading
import time

import blake3

from litecast.client_node import ClientNode
from litecast.middle_node import MiddleNode
from litecast.origin_server import OriginServer
from litecast.store import CheckpointStore
from litecast.server import run_server


def test_origin_buffer_publish_and_memory_http(tmp_path):
    origin = OriginServer(
        str(tmp_path), port=0, transport="http", ram_cache_bytes=1024
    )
    try:
        version = origin.broadcast_buffer(b"abcdefgh", 3, disk_mirror=False)
        assert version == "v1"
        assert sorted(path.name for path in (tmp_path / version).iterdir()) == [
            "manifest.json"
        ]
        base = "http://127.0.0.1:{}".format(origin.port)
        with urlopen(base + "/v1/shard_00002.bin") as response:
            assert response.read() == b"def"
            assert response.headers["Content-Length"] == "3"
        request = Request(base + "/v1/shard_00001.bin", method="HEAD")
        with urlopen(request) as response:
            assert response.headers["Content-Length"] == "3"
    finally:
        origin.shutdown()


def test_disk_shard_falls_through_when_version_is_not_in_store(tmp_path):
    version_dir = tmp_path / "v1"
    version_dir.mkdir()
    (version_dir / "shard_00001.bin").write_bytes(b"disk-backed")
    http_server, thread = run_server(
        str(tmp_path), 0, store=CheckpointStore(0)
    )
    try:
        with urlopen(
            "http://127.0.0.1:{}/v1/shard_00001.bin".format(
                http_server.server_port
            )
        ) as response:
            assert response.read() == b"disk-backed"
    finally:
        http_server.shutdown()
        thread.join(5)
        http_server.server_close()


def test_client_http_fallback_stays_in_memory(tmp_path):
    data = b"complete-version"
    origin = OriginServer(
        str(tmp_path / "origin"), port=0, transport="http", ram_cache_bytes=1024
    )
    try:
        version = origin.broadcast_buffer(data, 4, disk_mirror=False)
        output = tmp_path / "client"
        client = ClientNode(
            ["http://127.0.0.1:{}".format(origin.port)],
            str(output),
            transport="http",
            max_version_bytes=1024,
        )
        assert client.download_version_buffer(version) == data
        assert not list(output.glob("temp_*"))
    finally:
        origin.shutdown()


def test_rf2_streams_each_origin_shard_once_and_clients_stripe(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")

    def free_port():
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        return port

    data = b"replicated-shards-arrive-out-of-order"
    origin = OriginServer(
        str(tmp_path / "origin"), port=0, transport="http",
        ram_cache_bytes=4096, disk_mirror=False,
    )
    version = origin.broadcast_buffer(data, 4, disk_mirror=False)
    counts = {"origin_shards": 0}
    count_lock = threading.Lock()
    original_get_shard = origin.store.get_shard

    def counted_get_shard(name, index):
        with count_lock:
            counts["origin_shards"] += 1
        return original_get_shard(name, index)

    origin.store.get_shard = counted_get_shard
    ports = [free_port(), free_port()]
    members = [
        "http://127.0.0.1:{}".format(port) for port in ports
    ]
    middles = []
    try:
        for index, member in enumerate(members):
            middles.append(
                MiddleNode(
                    ["http://127.0.0.1:{}".format(origin.port)],
                    str(tmp_path / "middle-{}".format(index)),
                    port=ports[index],
                    check_interval=1,
                    transport="http",
                    ram_cache_bytes=4096,
                    disk_mirror=False,
                    middle_id=member,
                    middle_servers=members,
                    replication_factor=2,
                )
            )
        deadline = time.monotonic() + 15
        while (
            not all(version in middle.processed_versions for middle in middles)
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        assert all(version in middle.processed_versions for middle in middles)
        client = ClientNode(
            members,
            str(tmp_path / "client"),
            transport="http",
            max_version_bytes=4096,
            replication_factor=2,
        )
        assert client.download_version_buffer(version) == data
        assert counts["origin_shards"] == origin.store.get_metadata(
            version
        ).shard_count
    finally:
        for middle in middles:
            middle.shutdown()
        origin.shutdown()


def test_auto_ucxx_failure_falls_back_to_http(monkeypatch, tmp_path):
    data = b"fallback"
    checksum = blake3.blake3(data).hexdigest()
    client = ClientNode(["http://unused"], str(tmp_path), transport="auto")
    client.list_available_versions = lambda: {"v1": checksum + "|1"}
    client._discover_endpoint = lambda: SimpleNamespace(
        verified_rdma=True,
        ucxx=SimpleNamespace(host="rdma", port=9999),
    )
    client.downloader.get_size = lambda path: len(data)

    def download_into(path, target):
        target[:] = data
        return True

    client.downloader.download_into = download_into

    class FailingUCXX:
        def __init__(self, *args, **kwargs):
            pass

        def fetch_version(self, version):
            raise RuntimeError("unavailable")

    monkeypatch.setattr("litecast.client_node.UCXXTransport", FailingUCXX)
    assert client.download_version_buffer("v1") == data


def test_partial_middle_version_is_not_marked_complete():
    node = MiddleNode.__new__(MiddleNode)
    node.processed_versions = set()
    node.known_shards = {}
    import threading
    node.lock = threading.Lock()
    node.client = SimpleNamespace(download_version_buffer=lambda version: None)
    node.store = CheckpointStore(1024)
    node.disk_mirror = False
    node._process_version("v1", "{}|2".format("0" * 64))
    assert "v1" not in node.processed_versions


def test_legacy_file_broadcast_still_writes_shards(tmp_path):
    source = tmp_path / "source.bin"
    source.write_bytes(b"legacy")
    origin = OriginServer(
        str(tmp_path / "origin"), port=0, transport="http", ram_cache_bytes=1024
    )
    try:
        version = origin.broadcast(str(source), shard_size=3)
        assert (tmp_path / "origin" / version / "shard_00001.bin").exists()
        assert origin.store.get(version).tobytes() == b"legacy"
    finally:
        origin.shutdown()


def test_memory_capacity_eviction_removes_unservable_manifest_version(tmp_path):
    origin = OriginServer(
        str(tmp_path), port=0, transport="http", ram_cache_bytes=8,
        disk_mirror=False,
    )
    try:
        assert origin.broadcast_buffer(b"12345678", 4, disk_mirror=False) == "v1"
        assert origin.broadcast_buffer(b"abcdefgh", 4, disk_mirror=False) == "v2"
        manifest = (tmp_path / "distribution.txt").read_text()
        assert "v1:" not in manifest
        assert "v2:" in manifest
        assert origin.store.get("v1") is None
    finally:
        origin.shutdown()
