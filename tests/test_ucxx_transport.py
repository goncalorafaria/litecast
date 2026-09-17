import asyncio
import importlib
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import blake3
import pytest

from litecast.protocol import (
    MessageType,
    decode_frame,
    decode_get_version,
    decode_get_shard,
    decode_get_shards,
    decode_metadata,
    decode_shard_metadata,
    decode_shards_metadata,
    encode_get_version,
    encode_get_shard,
    encode_get_shards,
    encode_metadata,
    encode_shard_metadata,
    encode_shards_metadata,
    encode_version_available,
    encode_wait_version,
)
from litecast.store import CheckpointStore, VersionMetadata
from litecast.transport.base import (
    TransportIntegrityError,
    TransportProtocolError,
)
from litecast.transport.ucxx import (
    UCXXConfigurationError,
    UCXXEventLoopExecutor,
    UCXXServer,
    UCXXTransport,
    preflight,
)


class FakeEndpoint:
    def __init__(self, response, chunks=()):
        self.response = (
            encode_metadata(response)
            if isinstance(response, dict)
            else response
        )
        self.chunks = iter(chunks)
        self.requests = []
        self.closed = False

    async def send_obj(self, value):
        self.requests.append(value)

    async def recv_obj(self):
        return self.response

    async def recv(self, buffer):
        chunk = next(self.chunks)
        assert len(buffer) == len(chunk)
        buffer[:] = chunk

    def close(self):
        self.closed = True


class FakeUCXX:
    __version__ = "fake"

    def __init__(self, endpoint=None, config=None, address="10.0.0.1"):
        self.endpoint = endpoint
        self.config = {"TLS": "rc"} if config is None else config
        self.address = address
        self.init_options = None
        self.create_endpoint_calls = 0

    def init(self, options=None):
        self.init_options = options

    def get_config(self):
        return self.config

    def get_address(self, interface):
        assert interface == "ib0"
        return self.address

    async def create_endpoint(self, host, port, **kwargs):
        assert host == "server"
        assert port == 9000
        self.create_endpoint_calls += 1
        return self.endpoint


class FakeServerEndpoint:
    def __init__(self, request):
        self.requests = iter(
            request if isinstance(request, (list, tuple)) else [request]
        )
        self.control = []
        self.chunks = []
        self.closed = False

    async def recv_obj(self):
        try:
            return next(self.requests)
        except StopIteration:
            raise EOFError("peer closed")

    async def send_obj(self, value):
        self.control.append(value)

    async def send(self, value):
        self.chunks.append(bytes(value))

    def close(self):
        self.closed = True


def metadata(data, shard_size=4, checksum=None):
    return {
        "status": "ok",
        "name": "v1",
        "total_size": len(data),
        "shard_size": shard_size,
        "shard_count": (len(data) + shard_size - 1) // shard_size,
        "checksum": checksum or blake3.blake3(data).hexdigest(),
    }


def test_module_import_does_not_import_ucxx(monkeypatch):
    monkeypatch.delitem(sys.modules, "ucxx", raising=False)
    module = importlib.import_module("litecast.transport.ucxx")
    assert "ucxx" not in sys.modules
    assert module.UCXXTransport


def test_preflight_requires_explicit_rc_configuration():
    with pytest.raises(UCXXConfigurationError, match="rc"):
        preflight("ib0", FakeUCXX(config={"TLS": "tcp"}))
    result = preflight("ib0", FakeUCXX(config={"TLS": "rc,tcp"}))
    assert result.address == "10.0.0.1"
    assert result.verified_rdma


def test_preflight_address_failure_is_not_rdma_verified():
    with pytest.raises(UCXXConfigurationError, match="address"):
        preflight("ib0", FakeUCXX(address=""))


def test_successful_multichunk_transfer_uses_one_receive_buffer():
    data = b"abcdefghij"
    endpoint = FakeEndpoint(metadata(data), [b"abcd", b"efgh", b"ij"])
    result = UCXXTransport(
        "server", 9000, ucxx_module=FakeUCXX(endpoint)
    ).fetch_version("v1")
    assert isinstance(result.data, bytearray)
    assert result.data == data
    assert result.metadata.shard_count == 3
    assert [
        decode_get_version(decode_frame(request)) for request in endpoint.requests
    ] == ["v1"]
    assert endpoint.closed


def test_client_can_submit_transfer_to_an_owned_event_loop():
    data = b"abcdefgh"
    endpoint = FakeEndpoint(metadata(data), [b"abcd", b"efgh"])
    submitted = []

    def executor(awaitable):
        submitted.append(awaitable)
        return asyncio.run(awaitable)

    result = UCXXTransport(
        "server",
        9000,
        ucxx_module=FakeUCXX(endpoint),
        executor=executor,
    ).fetch_version("v1")

    assert len(submitted) == 1
    assert result.data == data
    assert endpoint.closed


def test_client_executor_serializes_ucxx_loop_ownership():
    executor = UCXXEventLoopExecutor()

    async def identity():
        return id(asyncio.get_running_loop()), threading.get_ident()

    try:
        with ThreadPoolExecutor(max_workers=8) as workers:
            identities = list(workers.map(lambda _: executor(identity()), range(32)))
        assert len(set(identities)) == 1
    finally:
        executor.close()


@pytest.mark.parametrize(
    "response",
    [
        {"status": "ok"},
        {
            "status": "ok",
            "name": "v1",
            "total_size": 10,
            "shard_size": 4,
            "shard_count": 2,
            "checksum": "0" * 64,
        },
        {
            "status": "ok",
            "name": "v1",
            "total_size": 10**12,
            "shard_size": 4,
            "shard_count": 250_000_000_000,
            "checksum": "0" * 64,
        },
    ],
)
def test_malformed_or_unbounded_metadata_closes_endpoint(response):
    endpoint = FakeEndpoint(response)
    client = UCXXTransport(
        "server", 9000, max_total_size=100, ucxx_module=FakeUCXX(endpoint)
    )
    with pytest.raises(TransportProtocolError):
        client.fetch_version("v1")
    assert endpoint.closed


def test_checksum_mismatch_closes_endpoint():
    data = b"abcdefgh"
    endpoint = FakeEndpoint(metadata(data, checksum="0" * 64), [b"abcd", b"efgh"])
    with pytest.raises(TransportIntegrityError, match="checksum"):
        UCXXTransport(
            "server", 9000, ucxx_module=FakeUCXX(endpoint)
        ).fetch_version("v1")
    assert endpoint.closed


def test_server_uses_framed_control_and_zero_copy_chunk_sequence():
    store = CheckpointStore(100)
    metadata_value = store.put("v1", b"abcdefghij", 4)
    endpoint = FakeServerEndpoint(encode_get_version("v1"))
    server = UCXXServer(store, "ib0", ucxx_module=FakeUCXX())

    asyncio.run(server._handle_endpoint(endpoint))

    assert len(endpoint.control) == 1
    response = decode_metadata(decode_frame(endpoint.control[0]))
    assert response["checksum"] == metadata_value.checksum
    assert endpoint.chunks == [b"abcd", b"efgh", b"ij"]
    assert endpoint.closed


def test_client_fetches_and_verifies_one_indexed_shard():
    data = b"efgh"
    response = encode_shard_metadata(
        {
            "status": "ok",
            "version": "v1",
            "index": 1,
            "size": len(data),
            "checksum": blake3.blake3(data).hexdigest(),
        }
    )
    endpoint = FakeEndpoint(response, [data])
    result = UCXXTransport(
        "server", 9000, ucxx_module=FakeUCXX(endpoint)
    ).fetch_shard("v1", 1, blake3.blake3(data).hexdigest())
    assert result.data == data
    assert decode_get_shard(decode_frame(endpoint.requests[0])) == ("v1", 1)


def test_client_reuses_one_endpoint_for_multiple_shards():
    data = b"efgh"
    response = encode_shard_metadata(
        {
            "status": "ok",
            "version": "v1",
            "index": 1,
            "size": len(data),
            "checksum": blake3.blake3(data).hexdigest(),
        }
    )
    endpoint = FakeEndpoint(response, [data, data])
    ucxx = FakeUCXX(endpoint)
    executor = UCXXEventLoopExecutor()
    client = UCXXTransport(
        "server", 9000, ucxx_module=ucxx, executor=executor
    )
    try:
        client.warm()
        assert client.fetch_shard("v1", 1).data == data
        assert client.fetch_shard("v1", 1).data == data
        assert ucxx.create_endpoint_calls == 1
        assert len(endpoint.requests) == 2
        assert not endpoint.closed
    finally:
        client.close()
        executor.close()
    assert endpoint.closed


def test_client_waits_for_ucxx_version_notification():
    endpoint = FakeEndpoint(encode_version_available("v1"))
    executor = UCXXEventLoopExecutor()
    client = UCXXTransport(
        "server", 9000, ucxx_module=FakeUCXX(endpoint), executor=executor
    )
    try:
        client.wait_for_version("v1")
        assert decode_frame(endpoint.requests[0]).message_type == MessageType.WAIT_VERSION
    finally:
        client.close()
        executor.close()


def test_server_wait_notification_uses_explicit_discoverability():
    store = CheckpointStore(100)
    endpoint = FakeServerEndpoint(encode_wait_version("v1"))
    server = UCXXServer(store, "ib0", ucxx_module=FakeUCXX())
    server.notify_version_available("v1")

    asyncio.run(server._handle_endpoint(endpoint))

    assert decode_frame(
        endpoint.control[0]
    ).message_type == MessageType.VERSION_AVAILABLE


def test_client_fetches_batched_shards_into_final_buffers():
    first = b"abcd"
    second = b"efgh"
    response = encode_shards_metadata(
        {
            "version": "v1",
            "shards": [
                {
                    "index": 0,
                    "size": 4,
                    "checksum": blake3.blake3(first).hexdigest(),
                },
                {
                    "index": 1,
                    "size": 4,
                    "checksum": blake3.blake3(second).hexdigest(),
                },
            ],
            "unavailable": [],
        }
    )
    endpoint = FakeEndpoint(response, [first, second])
    executor = UCXXEventLoopExecutor()
    client = UCXXTransport(
        "server", 9000, ucxx_module=FakeUCXX(endpoint), executor=executor
    )
    targets = {0: memoryview(bytearray(4)), 1: memoryview(bytearray(4))}
    checksums = {
        0: blake3.blake3(first).hexdigest(),
        1: blake3.blake3(second).hexdigest(),
    }
    try:
        result = client.fetch_shards_into("v1", targets, checksums)
        assert result.completed == (0, 1)
        assert targets[0].tobytes() == first
        assert targets[1].tobytes() == second
        assert decode_get_shards(
            decode_frame(endpoint.requests[0])
        ) == ("v1", (0, 1))
    finally:
        client.close()
        executor.close()


def test_server_exposes_partial_shard_before_complete_version():
    data = b"efgh"
    store = CheckpointStore(100)
    store.begin_version(
        VersionMetadata("v1", 8, 4, 2, "0" * 64),
        ("0" * 64, blake3.blake3(data).hexdigest()),
    )
    store.put_shard("v1", 1, data)
    endpoint = FakeServerEndpoint(encode_get_shard("v1", 1))
    server = UCXXServer(store, "ib0", ucxx_module=FakeUCXX())
    asyncio.run(server._handle_endpoint(endpoint))
    response = decode_shard_metadata(decode_frame(endpoint.control[0]))
    assert response["index"] == 1
    assert endpoint.chunks == [data]


def test_server_serves_multiple_shards_on_one_endpoint():
    data = b"abcdefgh"
    checksums = (
        blake3.blake3(data[:4]).hexdigest(),
        blake3.blake3(data[4:]).hexdigest(),
    )
    store = CheckpointStore(100)
    store.put_verified(
        "v1", data, 4, blake3.blake3(data).hexdigest(), checksums
    )
    endpoint = FakeServerEndpoint(
        [encode_get_shard("v1", 0), encode_get_shard("v1", 1)]
    )
    server = UCXXServer(store, "ib0", ucxx_module=FakeUCXX())

    asyncio.run(server._handle_endpoint(endpoint))

    assert len(endpoint.control) == 2
    assert endpoint.chunks == [b"abcd", b"efgh"]
    assert endpoint.closed


def test_server_serves_batched_shards_and_version_notification():
    data = b"abcdefgh"
    checksums = (
        blake3.blake3(data[:4]).hexdigest(),
        blake3.blake3(data[4:]).hexdigest(),
    )
    store = CheckpointStore(100)
    store.put_verified(
        "v1", data, 4, blake3.blake3(data).hexdigest(), checksums
    )
    endpoint = FakeServerEndpoint(
        [encode_wait_version("v1"), encode_get_shards("v1", [0, 1])]
    )
    server = UCXXServer(store, "ib0", ucxx_module=FakeUCXX())
    server.notify_version_available("v1")

    asyncio.run(server._handle_endpoint(endpoint))

    assert decode_frame(endpoint.control[0]).message_type == MessageType.VERSION_AVAILABLE
    batch = decode_shards_metadata(decode_frame(endpoint.control[1]))
    assert [item["index"] for item in batch["shards"]] == [0, 1]
    assert endpoint.chunks == [b"abcd", b"efgh"]
