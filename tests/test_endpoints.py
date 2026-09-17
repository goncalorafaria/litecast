import json

import pytest

from litecast.endpoints import (
    ENDPOINT_SCHEMA,
    STREAMING_ENDPOINT_SCHEMA,
    EndpointMetadata,
    EndpointMetadataError,
    UCXXEndpoint,
    discovery_url,
    parse_endpoint_metadata,
    serialize_endpoint_metadata,
    write_endpoint_metadata,
)


def test_schema_one_round_trip_is_deterministic():
    metadata = EndpointMetadata(
        "https://node.example:8000/root/",
        UCXXEndpoint("10.0.0.7", 9000, "ib0", True),
    )
    encoded = serialize_endpoint_metadata(metadata)
    assert parse_endpoint_metadata(encoded) == EndpointMetadata(
        "https://node.example:8000/root",
        UCXXEndpoint("10.0.0.7", 9000, "ib0", True),
    )
    assert json.loads(encoded) == {
        "schema": ENDPOINT_SCHEMA,
        "http": {"base_url": "https://node.example:8000/root"},
        "ucxx": {
            "host": "10.0.0.7",
            "port": 9000,
            "interface": "ib0",
            "verified_rdma": True,
        },
    }


def test_http_only_metadata_and_discovery_url():
    metadata = parse_endpoint_metadata(
        b'{"schema":1,"http":{"base_url":"http://host:8000/base/"}}'
    )
    assert metadata.ucxx is None
    assert discovery_url(metadata.http_base_url) == (
        "http://host:8000/base/endpoints.json"
    )


def test_schema_two_streaming_capabilities_round_trip():
    metadata = EndpointMetadata(
        "http://middle:8000",
        UCXXEndpoint("10.0.0.2", 9000, "ib0", True),
        STREAMING_ENDPOINT_SCHEMA,
        "http://middle:8000",
        ("get_shard", "partial_shards"),
    )
    assert parse_endpoint_metadata(serialize_endpoint_metadata(metadata)) == metadata


@pytest.mark.parametrize(
    "document",
    [
        {"schema": 3, "http": {"base_url": "http://host"}},
        {"schema": 1, "http": {"base_url": "ftp://host"}},
        {"schema": 1, "http": {"base_url": "http://host"}, "extra": 1},
        {
            "schema": 1,
            "http": {"base_url": "http://host"},
            "ucxx": {"host": "h", "port": True, "verified_rdma": False},
        },
        {
            "schema": 1,
            "http": {"base_url": "http://host"},
            "ucxx": {"host": "h", "port": 1, "verified_rdma": "yes"},
        },
    ],
)
def test_strict_schema_validation(document):
    with pytest.raises(EndpointMetadataError):
        parse_endpoint_metadata(json.dumps(document))


def test_parser_applies_bound_before_json_decode():
    with pytest.raises(EndpointMetadataError, match="maximum"):
        parse_endpoint_metadata(b"{}" * 20, max_size=8)


def test_atomic_writer_replaces_complete_document(tmp_path):
    path = tmp_path / "endpoints.json"
    path.write_text("old")
    metadata = EndpointMetadata("http://host:8000")
    write_endpoint_metadata(path, metadata)
    assert parse_endpoint_metadata(path.read_bytes()) == metadata
    assert not list(tmp_path.glob(".endpoints.json.*"))
