# LiteCast

LiteCast distributes weights from publishers through CPU middle caches to clients.
It derives from ShardCast 0.3.2; see [provenance](PROVENANCE.md). The package and
commands use `litecast` / `LITECAST` names.

## Installation

Python 3.11 or newer:

```bash
pip install .
pip install '.[registry,test]'  # optional LiteRegistry integration and tests
```

HTTP works without CUDA, GPUs, or UCXX. UCXX is an optional data plane and must be
installed separately with a compatible UCX stack; HTTP still handles discovery
and manifests. `auto` falls back to HTTP when no compatible RDMA endpoint is
available. Hardware RDMA performance is not established by the CPU test suite.

## Transport

- HTTP origins, middle caches, and clients, including in-memory checkpoint buffers.
- Capacity-bounded checkpoint stores and configurable disk mirroring.
- Version manifests, size checks, and integrity verification.
- Optional UCXX transfer and selective transfers into caller-owned CPU buffers.
- Streaming support and explicit middle ownership/replication metadata.

```python
from litecast import OriginServer, ClientNode

origin = OriginServer("/tmp/origin", port=0, ram_cache_bytes=1024 * 1024)
client = ClientNode([f"http://127.0.0.1:{origin.port}"], "/tmp/client", transport="http")
try:
    version = origin.broadcast_buffer(b"example weights", 4, disk_mirror=False)
    assert bytes(client.download_version_buffer(version)) == b"example weights"
finally:
    client.close()
    origin.shutdown()
```

`litecast-origin`, `litecast-middle`, and `litecast-client` provide the underlying
transport CLIs. Use each command's `--help` for arguments.

## LiteRegistry integration

The optional `litecast.registry` modules add discovery and distribution for
adapter publishers without importing PrimeRL or vLLM. Registry records carry
metadata; payload bytes travel through LiteCast.

- Publisher leases fence conflicting/replaced owners.
- Run-scoped adapter bundles carry immutable digests and step identities.
- Clients prefer middle/peer sources, download shards in parallel, retry failed
  sources, and record transfer timing, byte, source, and verification metrics.
- Multiple publishers can share CPU middle deployments, with independent caches,
  ports, verification, logging, and retries.
- Middles can start empty and accept publishers later through a reloadable list.

```python
from pathlib import Path
from litecast.registry.config import PublisherConfig
from litecast.registry.distribution import Publisher

async def publish_weights():
    config = PublisherConfig(
        registry="head+sqlite:///shared/fleet/head.sqlite3",
        run_id="trainer-a",
        origin_host="trainer-a.internal",
    )
    publisher = Publisher(config, base_model="your-base-model")
    await publisher.start()
    try:
        publication = await publisher.publish(Path("./adapter"), step=1)
        # Keep the publisher alive during training and publish subsequent steps.
        # A compatible inference integration advertises installation readiness.
        await publisher.wait_ready(publication.model_name, timeout=300)
    finally:
        await publisher.close()
```

The adapter directory contains `adapter_config.json` and
`adapter_model.safetensors`. A base model identifier must match the client fleet.
SQLite is shared **head discovery**; the head's `redis` endpoint resolves the Redis
service registry. Redis is not replaced by SQLite. Provision Redis and publish
its endpoint before starting consumers. Synchronous endpoint helpers must run
outside an active asyncio loop, or via `asyncio.to_thread`.

For compatibility with the existing PrimeRL integration, discovery keys retain
`prime_rl:litecast:<run_id>:desired`, and adapter aliases retain the `sc-` prefix.
These are wire-format identifiers, not Python package dependencies.

## Publishers joining after startup

Start with `publishers.json` containing `[]`:

```bash
litecast-registry-middle \
  --registry head+sqlite:///shared/fleet/head.sqlite3 \
  --publishers /shared/fleet/publishers.json \
  --advertise-host middle.internal --port 0
```

Atomically replace that file with `["trainer-a"]`, then later
`["trainer-a", "trainer-b"]`. Existing caches remain serving while a new publisher
joins. A publisher may also be listed before it starts. Invalid reloads preserve
the previous subscriptions. Removing a run withdraws only its cache services.

See [multi-publisher operation and memory limits](docs/multiple-publishers.md).
Enrollment is explicit; this does not automatically trust every publisher in Redis.

## Scope and validation

The repository contains transport and registry integration. Training loops,
request gateways, vLLM adapter installation, live inference tenant subscriptions,
and retained-state LoRA updates remain in the PrimeRL fork. Adding a publisher
to a middle alone does not enroll an inference server.

```bash
python -m pytest
```

Tests exercise CPU memory/HTTP transfers, cache eviction, manifests, selective
transfer, simulated UCXX contracts, real Redis fencing (when `redis-server` is
available), and two middle processes accepting publishers late. The middle test
also replaces publisher B and verifies that A keeps its existing endpoints.
