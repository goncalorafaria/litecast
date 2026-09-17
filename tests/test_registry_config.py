import pytest

from litecast.registry.config import PublisherConfig


@pytest.mark.parametrize("changes", [
    {"run_id": "../other"}, {"origin_port": 65536}, {"registry": ""},
    {"transport": "unknown"}, {"max_adapter_bytes": 0}, {"min_replicas": 0},
    {"poll_seconds": 2, "lease_seconds": 4},
])
def test_invalid_publisher_config(changes):
    with pytest.raises(ValueError):
        PublisherConfig(**(dict(registry="redis://localhost", run_id="a", origin_host="localhost") | changes))


def test_ephemeral_origin_port():
    config = PublisherConfig(registry="redis://localhost", run_id="a", origin_host="localhost")
    assert config.origin_port == 0
    assert config.min_replicas == 1
