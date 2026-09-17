from array import array
from types import SimpleNamespace

import pytest

from litecast.client_node import ClientNode
from litecast.origin_server import OriginServer
from litecast.streaming import build_manifest


@pytest.fixture
def selective_client(tmp_path, monkeypatch):
    payload = b"abcdefghijklmn"
    manifest = build_manifest("v1", payload, 4)
    client = ClientNode(["http://source"], str(tmp_path), transport="http")
    monkeypatch.setattr(client, "_load_shard_manifest", lambda version: manifest)
    monkeypatch.setattr(client, "list_available_versions", lambda: {"v1": manifest.checksum + "|4"})
    requests = []

    def download(server, path, target):
        index = int(path.split("shard_")[1].split(".")[0]) - 1
        requests.append(index)
        target[:] = payload[index * 4:index * 4 + len(target)]
        return True

    monkeypatch.setattr(client.downloader, "download_into_from", download)
    return client, manifest, requests


def test_selective_reuses_buffers_without_fetching_other_shards(selective_client):
    client, manifest, requests = selective_client
    client.max_version_bytes = 6  # Full checkpoint is larger than the receiver budget.
    buffer = bytearray(b"?" * 8)
    targets = {1: memoryview(buffer)[1:5], 3: memoryview(buffer)[5:7]}
    for _ in range(2):
        assert client.download_shards_into("v1", targets) == manifest
        assert buffer == b"?efghmn?"
    assert sorted(requests) == [1, 1, 3, 3]


def test_typed_cpu_buffer_uses_byte_size(selective_client):
    client, _, _ = selective_client
    buffer = array("I", [0])
    client.download_shards_into("v1", {0: buffer})
    assert buffer.tobytes() == b"abcd"


@pytest.mark.parametrize("targets, error", [
    ({True: bytearray(4)}, TypeError),
    ({-1: bytearray(4)}, IndexError),
    ({4: bytearray(4)}, IndexError),
    ({0: b"????"}, ValueError),
    ({0: bytearray(3)}, ValueError),
    ({0: memoryview(bytearray(8))[::2]}, ValueError),
])
def test_invalid_targets_fail_before_any_payload_write(selective_client, targets, error):
    client, _, requests = selective_client
    with pytest.raises(error):
        client.download_shards_into("v1", targets)
    assert requests == []


def test_overlap_and_budget_rejected(selective_client):
    client, _, requests = selective_client
    buffer = bytearray(6)
    with pytest.raises(ValueError, match="overlap"):
        client.download_shards_into("v1", {0: memoryview(buffer)[:4], 1: memoryview(buffer)[2:]})
    client.max_version_bytes = 3
    with pytest.raises(ValueError, match="max_version_bytes"):
        client.download_shards_into("v1", {0: bytearray(4)})
    assert requests == []


def test_empty_selection_does_not_fetch_payload(selective_client):
    client, manifest, requests = selective_client
    assert client.download_shards_into("v1", {}) == manifest
    assert requests == []


def test_distribution_mismatch_rejected(selective_client, monkeypatch):
    client, _, requests = selective_client
    monkeypatch.setattr(client, "list_available_versions", lambda: {"v1": "0" * 64 + "|4"})
    with pytest.raises(ValueError, match="distribution"):
        client.download_shards_into("v1", {0: bytearray(4)})
    assert requests == []


def test_corruption_does_not_report_completion(selective_client, monkeypatch):
    client, _, _ = selective_client
    monkeypatch.setattr("litecast.client_node.LITECAST_STREAMING_RETRY_SECONDS", 0.01)
    def corrupt(server, path, target):
        target[:] = b"x" * len(target)
        return True
    monkeypatch.setattr(client.downloader, "download_into_from", corrupt)
    with pytest.raises(RuntimeError, match="transfer failed"):
        client.download_shards_into("v1", {0: bytearray(4)})


def test_ucxx_batches_only_selected_shards_and_retries_unavailable(selective_client, monkeypatch):
    client, manifest, requests = selective_client
    client.transport = "auto"
    client.ucxx_executor = object()
    endpoint = SimpleNamespace(ucxx=object(), verified_rdma=True, capabilities=("get_shards",))
    monkeypatch.setattr(client, "_discover_endpoint_for", lambda owner: endpoint)
    batches = []
    def fetch(version, targets, checksums):
        batches.append(tuple(targets))
        assert checksums == {i: manifest.shard_checksums[i] for i in targets}
        targets[0][:] = b"abcd"
        return SimpleNamespace(completed=(0,), unavailable=(3,))
    transport = SimpleNamespace(fetch_shards_into=fetch)
    monkeypatch.setattr(client, "_ucxx_transport_for", lambda *args: transport)
    targets = {0: bytearray(4), 3: bytearray(2)}
    client.download_shards_into("v1", targets)
    assert batches == [(0, 3)]
    assert requests == [3]
    assert targets == {0: b"abcd", 3: b"mn"}


def test_real_http_origin_selective_transfer(tmp_path, monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    origin = OriginServer(str(tmp_path / "origin"), port=0, transport="http", ram_cache_bytes=1024)
    try:
        version = origin.broadcast_buffer(b"abcdefghij", 4, disk_mirror=False)
        client = ClientNode(["http://127.0.0.1:{}".format(origin.port)], str(tmp_path / "client"), transport="http")
        target = bytearray(2)
        manifest = client.download_shards_into(version, {2: target})
        assert target == b"ij"
        assert manifest.name == version
    finally:
        origin.shutdown()


def test_failure_waits_for_other_writers(selective_client, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    client, _, _ = selective_client
    client.shard_batch_size = 1
    started = threading.Event()
    release = threading.Event()
    failed = threading.Event()
    def download(server, path, target):
        if "00001" in path:
            assert started.wait(5)
            failed.set()
            raise RuntimeError("peer disconnected")
        started.set()
        assert release.wait(5)
        target[:] = b"efgh"
        return True
    monkeypatch.setattr(client.downloader, "download_into_from", download)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.download_shards_into, "v1", {0: bytearray(4), 1: bytearray(4)})
        try:
            assert failed.wait(5)
            assert not future.done()
        finally:
            release.set()
        with pytest.raises(RuntimeError, match="transfer failed"):
            future.result(timeout=5)


def test_legacy_manifest_selected_shards(selective_client, monkeypatch):
    from dataclasses import replace

    client, manifest, _ = selective_client
    legacy = replace(manifest, schema=1, merkle_root=None)
    monkeypatch.setattr(client, "_load_shard_manifest", lambda version: legacy)
    target = bytearray(2)
    assert client.download_shards_into("v1", {3: target}) == legacy
    assert target == b"mn"


def test_selective_completion_event_is_distinct(selective_client, monkeypatch):
    client, _, _ = selective_client
    events = []
    monkeypatch.setattr("litecast.client_node.emit_event", lambda event, **fields: events.append((event, fields)))
    client.download_shards_into("v1", {3: bytearray(2)})
    assert not any(event == "download_completed" for event, _ in events)
    event, fields = events[-1]
    assert event == "shards_download_completed"
    assert fields["total_size"] == 2
    assert fields["shard_count"] == 1
