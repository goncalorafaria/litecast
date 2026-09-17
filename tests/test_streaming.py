import blake3
import pytest

from litecast.streaming import (
    ShardManifest,
    build_manifest,
    canonical_member_profiles,
    canonical_members,
    merkle_root,
    order_owners_for_client,
    shard_owners,
)


def test_manifest_round_trip_contains_per_shard_integrity():
    data = b"abcdefghij"
    manifest = build_manifest("v1", data, 4)
    decoded = ShardManifest.from_bytes(manifest.to_bytes())
    assert decoded == manifest
    assert decoded.shard_count == 3
    assert decoded.shard_length(2) == 2
    assert decoded.checksum == blake3.blake3(data).hexdigest()
    assert decoded.shard_checksums[1] == blake3.blake3(b"efgh").hexdigest()
    assert decoded.merkle_root == merkle_root(decoded.shard_checksums)


def test_rendezvous_placement_is_deterministic_and_rf2():
    members = (
        "http://middle-a:8000",
        "http://middle-b:8000",
        "http://middle-c:8000",
    )
    first = shard_owners("v7", 11, members, 2)
    assert first == shard_owners("v7", 11, list(members), 2)
    assert len(first) == 2
    assert len(set(first)) == 2
    assert all(owner in members for owner in first)


def test_replication_factor_is_capped_by_membership():
    assert shard_owners("v1", 0, ["http://only"], 2) == ("http://only",)
    with pytest.raises(ValueError):
        canonical_members(["http://same", "http://same"])


def test_weighted_placement_prefers_capacity_over_many_shards():
    members = ("http://small", "http://large")
    profiles = {
        "http://small": {"capacity": 1.0},
        "http://large": {"capacity": 8.0},
    }
    primaries = [
        shard_owners("v1", index, members, 1, profiles)[0]
        for index in range(1000)
    ]
    assert primaries.count("http://large") > primaries.count("http://small") * 4


def test_replica_placement_diversifies_failure_domains():
    members = ("http://a", "http://b", "http://c")
    profiles = {
        "http://a": {"failure_domain": "rack-1"},
        "http://b": {"failure_domain": "rack-1"},
        "http://c": {"failure_domain": "rack-2"},
    }
    owners = shard_owners("v1", 0, members, 2, profiles)
    normalized = canonical_member_profiles(members, profiles)
    domains = {item[0]: item[4] for item in normalized}
    assert domains[owners[0]] != domains[owners[1]]


def test_client_locality_reorders_replicas_without_changing_owner_set():
    owners = ("http://remote", "http://local")
    profiles = {
        "http://remote": {"locality": "rack-1"},
        "http://local": {"locality": "rack-2"},
    }
    ordered = order_owners_for_client(owners, profiles, "rack-2")
    assert ordered == ("http://local", "http://remote")
    assert set(ordered) == set(owners)
