from concurrent.futures import ThreadPoolExecutor

import blake3
import pytest

from litecast.store import (
    CheckpointStore,
    InvalidShardIndexError,
    VersionNotFoundError,
    VersionStore,
    VersionTooLargeError,
    VersionMetadata,
)


def test_put_tracks_metadata_and_complete_buffer():
    data = b"abcdefghij"
    store = CheckpointStore(capacity_bytes=100)
    metadata = store.put("v1", data, shard_size=4)

    assert metadata.name == "v1"
    assert metadata.total_size == 10
    assert metadata.shard_size == 4
    assert metadata.shard_count == 3
    assert metadata.checksum == blake3.blake3(data).hexdigest()
    assert store.used_bytes == 10
    assert len(store) == 1
    assert "v1" in store
    assert store.get("v1").tobytes() == data
    assert store.get_metadata("v1") == metadata


def test_bytes_input_and_shards_are_zero_copy_read_only_views():
    data = b"abcdefghij"
    store = VersionStore(100)
    store.put("v1", data, shard_size=4)

    whole = store.get("v1")
    shards = [store.get_shard("v1", i) for i in range(3)]
    assert whole.obj is data
    assert [shard.tobytes() for shard in shards] == [b"abcd", b"efgh", b"ij"]
    assert all(shard.obj is data for shard in shards)
    assert all(shard.readonly for shard in shards)
    with pytest.raises(TypeError):
        shards[0][0] = 0


@pytest.mark.parametrize(
    "source",
    [
        bytearray(b"mutable"),
        memoryview(bytearray(b"mutable")),
    ],
)
def test_writable_input_is_copied_for_mutation_safety(source):
    store = CheckpointStore(100)
    store.put("v1", source, shard_size=100)
    if isinstance(source, memoryview):
        source[0] = ord("X")
    else:
        source[0] = ord("X")
    assert store.get("v1").tobytes() == b"mutable"


def test_readonly_view_is_copied_to_avoid_mutable_backing_alias():
    backing = bytearray(b"hidden alias")
    readonly = memoryview(backing).toreadonly()
    store = CheckpointStore(100)
    store.put("v1", readonly, shard_size=20)
    backing[0] = ord("X")
    assert store.get("v1").tobytes() == b"hidden alias"


def test_oldest_version_is_evicted_deterministically_not_lru():
    store = CheckpointStore(6)
    store.put("old", b"aa", 2)
    store.put("middle", b"bb", 2)
    store.put("new", b"cc", 2)
    assert store.get("old").tobytes() == b"aa"  # access does not reorder

    store.put("newest", b"dd", 2)
    assert [item.name for item in store.list()] == ["middle", "new", "newest"]
    assert store.get("old") is None
    assert store.used_bytes == 6


def test_eviction_removes_as_many_versions_as_needed():
    store = CheckpointStore(7)
    store.put("one", b"11", 1)
    store.put("two", b"22", 1)
    store.put("three", b"33", 1)
    store.put("large", b"12345", 2)
    assert [item.name for item in store.list_versions()] == ["three", "large"]
    assert store.used_bytes == 7


def test_replacement_is_atomic_and_becomes_newest():
    store = CheckpointStore(6)
    store.put("one", b"11", 1)
    store.put("two", b"22", 1)
    store.put("one", b"111", 2)
    assert [item.name for item in store.list()] == ["two", "one"]
    assert store.used_bytes == 5
    assert store.get("one").tobytes() == b"111"


def test_single_oversize_version_is_rejected_without_eviction():
    store = CheckpointStore(3)
    store.put("existing", b"abc", 2)
    with pytest.raises(VersionTooLargeError):
        store.put("too-big", b"abcd", 2)
    assert [item.name for item in store.list()] == ["existing"]
    assert store.used_bytes == 3


def test_checksum_can_be_verified_on_insert():
    data = b"checkpoint"
    checksum = blake3.blake3(data).hexdigest()
    store = CheckpointStore(100)
    metadata = store.put("v1", data, 4, checksum=checksum.upper())
    assert metadata.checksum == checksum
    with pytest.raises(ValueError, match="checksum"):
        store.put("v2", data, 4, checksum="wrong")
    assert store.get("v2") is None


def test_remove_and_clear_update_capacity_accounting():
    store = CheckpointStore(100)
    store.put("one", b"123", 2)
    store.put("two", b"4567", 2)
    assert store.remove("one")
    assert not store.remove("missing")
    assert store.used_bytes == 4
    store.clear()
    assert store.used_bytes == 0
    assert store.list() == []


def test_missing_versions_and_invalid_shards():
    store = CheckpointStore(100)
    assert store.get("missing") is None
    assert store.get_metadata("missing") is None
    with pytest.raises(VersionNotFoundError):
        store.get_shard("missing", 0)

    store.put("v1", b"abc", 2)
    with pytest.raises(InvalidShardIndexError):
        store.get_shard("v1", -1)
    with pytest.raises(InvalidShardIndexError):
        store.get_shard("v1", 2)
    with pytest.raises(TypeError):
        store.get_shard("v1", 1.0)


def test_empty_version_is_supported_and_has_no_shards():
    store = CheckpointStore(0)
    metadata = store.put("empty", b"", 4)
    assert metadata.total_size == 0
    assert metadata.shard_count == 0
    assert store.get("empty").tobytes() == b""
    with pytest.raises(InvalidShardIndexError):
        store.get_shard("empty", 0)


@pytest.mark.parametrize("capacity", [-1, 1.0, True])
def test_invalid_capacity_is_rejected(capacity):
    with pytest.raises(ValueError):
        CheckpointStore(capacity)


@pytest.mark.parametrize("shard_size", [0, -1, 1.0, True])
def test_invalid_shard_size_is_rejected(shard_size):
    with pytest.raises(ValueError):
        CheckpointStore(10).put("v1", b"x", shard_size)


def test_thread_safe_concurrent_updates_preserve_invariants():
    store = CheckpointStore(64)

    def insert(index):
        payload = bytes([index]) * 8
        store.put("v{}".format(index), payload, 3)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(insert, range(40)))

    versions = store.list()
    assert store.used_bytes == sum(item.total_size for item in versions)
    assert store.used_bytes <= store.capacity_bytes
    assert len(versions) == 8
    for metadata in versions:
        assert store.get(metadata.name).readonly


def test_partial_shards_are_visible_before_version_completion():
    shards = (b"abcd", b"efgh", b"ij")
    checksums = tuple(blake3.blake3(item).hexdigest() for item in shards)
    metadata = VersionMetadata(
        "v1", 10, 4, 3, blake3.blake3(b"".join(shards)).hexdigest()
    )
    store = CheckpointStore(10)
    store.begin_version(metadata, checksums)

    store.put_shard("v1", 2, shards[2])
    assert store.available_shards("v1") == (2,)
    assert store.get_shard("v1", 2).tobytes() == b"ij"
    assert not store.is_complete("v1")
    assert store.get("v1") is None

    store.put_shard("v1", 0, shards[0])
    store.put_shard("v1", 1, shards[1])
    assert store.is_complete("v1")
    assert store.used_bytes == 10


def test_partial_store_rejects_corrupt_or_wrong_sized_shard():
    payload = b"abcd"
    metadata = VersionMetadata(
        "v1", 4, 4, 1, blake3.blake3(payload).hexdigest()
    )
    store = CheckpointStore(10)
    store.begin_version(metadata, (blake3.blake3(payload).hexdigest(),))
    with pytest.raises(ValueError, match="size"):
        store.put_shard("v1", 0, b"abc")
    with pytest.raises(ValueError, match="checksum"):
        store.put_shard("v1", 0, b"wxyz")


def test_verified_shard_transfers_bytearray_ownership_without_copy():
    payload = bytearray(b"abcd")
    checksum = blake3.blake3(payload).hexdigest()
    metadata = VersionMetadata("v1", 4, 4, 1, checksum)
    store = CheckpointStore(4)
    store.begin_version(metadata, (checksum,))

    returned = store.put_verified_shard("v1", 0, payload, checksum)
    retained = store.get_shard("v1", 0)

    assert returned.readonly
    assert retained.readonly
    assert retained.obj is payload
    assert retained.tobytes() == b"abcd"


def test_verified_complete_buffer_is_retained_without_copy():
    payload = bytearray(b"abcdefgh")
    checksum = blake3.blake3(payload).hexdigest()
    shard_checksums = (
        blake3.blake3(payload[:4]).hexdigest(),
        blake3.blake3(payload[4:]).hexdigest(),
    )
    store = CheckpointStore(8)

    metadata = store.put_verified(
        "v1", payload, 4, checksum, shard_checksums
    )

    retained = store.get("v1")
    assert metadata.checksum == checksum
    assert retained.readonly
    assert retained.obj is payload
    assert store.get_shard_checksum("v1", 1) == shard_checksums[1]
