"""Configuration for a framework-independent adapter publisher."""

from dataclasses import dataclass

from .protocol import validate_run_id


@dataclass(frozen=True)
class PublisherConfig:
    registry: str
    run_id: str
    origin_host: str
    origin_port: int = 0
    transport: str = "http"
    max_adapter_bytes: int = 512 * 1024 * 1024
    retain_versions: int = 8
    poll_seconds: float = 2
    lease_seconds: float = 30
    shard_bytes: int = 8 * 1024 * 1024
    min_replicas: int = 1

    def __post_init__(self):
        validate_run_id(self.run_id)
        if not self.registry or not self.origin_host:
            raise ValueError("registry and origin_host are required")
        if not 0 <= self.origin_port <= 65535:
            raise ValueError("origin_port must be between 0 and 65535")
        if self.transport not in {"http", "auto", "ucxx"}:
            raise ValueError("unsupported transport")
        if min(self.max_adapter_bytes, self.retain_versions, self.poll_seconds, self.shard_bytes, self.min_replicas) <= 0:
            raise ValueError("sizes, counts and polling interval must be positive")
        if self.lease_seconds <= 2 * self.poll_seconds:
            raise ValueError("lease_seconds must exceed twice poll_seconds")
