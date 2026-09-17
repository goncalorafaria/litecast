import json

import pytest

from litecast.protocol import (
    HEADER,
    HEADER_SIZE,
    MAGIC,
    MAX_PAYLOAD_SIZE,
    PROTOCOL_VERSION,
    Frame,
    FrameDecoder,
    MessageType,
    ProtocolError,
    decode_chunk_data,
    decode_error,
    decode_frame,
    decode_frames,
    decode_get_version,
    decode_get_shard,
    decode_get_shards,
    decode_header,
    decode_metadata,
    decode_shard_metadata,
    decode_shards_metadata,
    decode_shutdown,
    decode_version_available,
    decode_wait_version,
    encode_chunk_data,
    encode_error,
    encode_frame,
    encode_get_version,
    encode_get_shard,
    encode_get_shards,
    encode_metadata,
    encode_shard_metadata,
    encode_shards_metadata,
    encode_shutdown,
    encode_version_available,
    encode_wait_version,
)


@pytest.mark.parametrize(
    "message_type,payload,flags",
    [
        (MessageType.GET_VERSION, b"v17", 0),
        (MessageType.METADATA, b'{"name":"v17"}', 3),
        (MessageType.CHUNK_DATA, b"\0\0\0\1data", 0),
        (MessageType.ERROR, b"missing", 0),
        (MessageType.SHUTDOWN, b"", 0),
    ],
)
def test_frame_round_trip(message_type, payload, flags):
    encoded = encode_frame(message_type, payload, flags)
    decoded = decode_frame(encoded)
    assert decoded == Frame(message_type, payload, flags)
    assert len(encoded) == HEADER_SIZE + len(payload)


def test_message_helpers_round_trip():
    request = decode_frame(encode_get_version("checkpoint-\N{SNOWMAN}"))
    assert decode_get_version(request) == "checkpoint-\N{SNOWMAN}"
    shard_request = decode_frame(encode_get_shard("v4", 17))
    assert decode_get_shard(shard_request) == ("v4", 17)
    batch_request = decode_frame(encode_get_shards("v4", [1, 3, 8]))
    assert decode_get_shards(batch_request) == ("v4", (1, 3, 8))
    assert (
        decode_wait_version(decode_frame(encode_wait_version("v4"))) == "v4"
    )
    assert (
        decode_version_available(
            decode_frame(encode_version_available("v4"))
        )
        == "v4"
    )

    metadata = {
        "name": "v4",
        "total_size": 11,
        "shard_size": 4,
        "shard_count": 3,
        "checksum": "abc",
    }
    assert decode_metadata(decode_frame(encode_metadata(metadata))) == metadata
    shard_metadata = {
        "status": "ok", "version": "v4", "index": 1,
        "size": 4, "checksum": "a" * 64,
    }
    assert (
        decode_shard_metadata(
            decode_frame(encode_shard_metadata(shard_metadata))
        )
        == shard_metadata
    )
    batch_metadata = {
        "version": "v4",
        "shards": [{"index": 1, "size": 4, "checksum": "a" * 64}],
        "unavailable": [3],
    }
    assert decode_shards_metadata(
        decode_frame(encode_shards_metadata(batch_metadata))
    ) == batch_metadata

    index, data = decode_chunk_data(decode_frame(encode_chunk_data(7, b"payload")))
    assert index == 7
    assert isinstance(data, memoryview)
    assert data.readonly
    assert data.tobytes() == b"payload"

    assert decode_error(decode_frame(encode_error("not found"))) == "not found"
    assert decode_shutdown(decode_frame(encode_shutdown())) is None


def test_metadata_encoding_is_deterministic():
    first = encode_metadata({"z": 1, "a": 2})
    second = encode_metadata({"a": 2, "z": 1})
    assert first == second


def test_stream_decoder_handles_fragmentation_and_multiple_frames():
    frames = [encode_get_version("v1"), encode_error("no"), encode_shutdown()]
    wire = b"".join(frames)
    decoder = FrameDecoder()
    decoded = []
    for byte in wire:
        decoded.extend(decoder.feed(bytes([byte])))
    assert [frame.message_type for frame in decoded] == [
        MessageType.GET_VERSION,
        MessageType.ERROR,
        MessageType.SHUTDOWN,
    ]
    assert decoder.buffered_bytes == 0
    assert list(decode_frames(wire)) == decoded


@pytest.mark.parametrize(
    "wire,error",
    [
        (b"", "truncated"),
        (b"x" * (HEADER_SIZE - 1), "truncated"),
        (
            HEADER.pack(b"NOPE", PROTOCOL_VERSION, MessageType.ERROR, 0, 0),
            "magic",
        ),
        (
            HEADER.pack(MAGIC, PROTOCOL_VERSION + 1, MessageType.ERROR, 0, 0),
            "version",
        ),
        (HEADER.pack(MAGIC, PROTOCOL_VERSION, 255, 0, 0), "message type"),
        (
            HEADER.pack(MAGIC, PROTOCOL_VERSION, MessageType.ERROR, 0, 4) + b"abc",
            "truncated",
        ),
        (encode_error("bad") + b"x", "trailing"),
    ],
)
def test_malformed_frames_are_rejected(wire, error):
    with pytest.raises(ProtocolError, match=error):
        decode_frame(wire)


def test_declared_length_is_bounded_before_payload_is_received():
    header = HEADER.pack(
        MAGIC, PROTOCOL_VERSION, MessageType.CHUNK_DATA, 0, MAX_PAYLOAD_SIZE + 1
    )
    with pytest.raises(ProtocolError, match="maximum"):
        decode_header(header)
    with pytest.raises(ProtocolError, match="maximum"):
        FrameDecoder().feed(header)


def test_configurable_small_payload_bound():
    wire = encode_frame(MessageType.ERROR, b"1234")
    with pytest.raises(ProtocolError, match="maximum"):
        decode_frame(wire, max_payload_size=3)
    with pytest.raises(ProtocolError, match="maximum"):
        encode_frame(MessageType.ERROR, b"1234", max_payload_size=3)


@pytest.mark.parametrize("maximum", [-1, 1.5, True])
def test_invalid_payload_bounds_are_rejected(maximum):
    with pytest.raises(ValueError):
        decode_header(
            HEADER.pack(MAGIC, PROTOCOL_VERSION, MessageType.ERROR, 0, 0),
            maximum,
        )


def test_semantically_invalid_payloads_are_rejected():
    with pytest.raises(ProtocolError, match="empty"):
        encode_get_version("")
    with pytest.raises(ProtocolError, match="UTF-8"):
        decode_frame(encode_frame(MessageType.GET_VERSION, b"\xff"))
    with pytest.raises(ProtocolError, match="index"):
        decode_frame(encode_frame(MessageType.CHUNK_DATA, b"abc"))
    with pytest.raises(ProtocolError, match="empty payload"):
        decode_frame(encode_frame(MessageType.SHUTDOWN, b"x"))
    with pytest.raises(ProtocolError, match="JSON"):
        decode_metadata(Frame(MessageType.METADATA, b"{"))
    with pytest.raises(ProtocolError, match="object"):
        decode_metadata(Frame(MessageType.METADATA, json.dumps([1]).encode()))


@pytest.mark.parametrize("index", [-1, 2**32, 1.5, True])
def test_invalid_chunk_index_is_rejected(index):
    with pytest.raises(ProtocolError):
        encode_chunk_data(index, b"data")


def test_batched_shard_request_rejects_duplicates_and_empty_batches():
    with pytest.raises(ProtocolError):
        encode_get_shards("v1", [])
    with pytest.raises(ProtocolError):
        encode_get_shards("v1", [1, 1])


def test_helper_rejects_wrong_frame_type():
    with pytest.raises(ProtocolError, match="GET_VERSION"):
        decode_get_version(Frame(MessageType.ERROR, b"no"))


def test_decode_frames_rejects_incomplete_final_frame():
    with pytest.raises(ProtocolError, match="final"):
        list(decode_frames(encode_shutdown() + b"SC"))
