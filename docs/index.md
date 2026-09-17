# LiteCast

LiteCast distributes model weights through origins, shared middle caches, and
clients. Its optional LiteRegistry integration discovers publishers and replicas
without coupling the transport to PrimeRL or vLLM.

## Architecture

1. A trainer publishes a versioned weight bundle at an origin.
2. CPU middles cache bundles and advertise available versions in LiteRegistry.
3. Inference workers discover sources, download the bundle, verify its digest,
   and install it through their training-framework integration.

SQLite stores head discovery information; the discovered Redis service holds
registry metadata. Weight payloads travel through LiteCast rather than Redis.
A request gateway routes model inference separately from weight distribution.

## Install

From the source checkout:

```bash
uv sync --extra registry
```

The transport requires Python 3.11 or newer. HTTP works without CUDA or UCXX.

## Publish and download a buffer

```python
from litecast import ClientNode, OriginServer

origin = OriginServer("/tmp/origin", port=0, ram_cache_bytes=1024 * 1024)
client = ClientNode(
    [f"http://127.0.0.1:{origin.port}"], "/tmp/client", transport="http"
)
try:
    version = origin.broadcast_buffer(b"example weights", 4, disk_mirror=False)
    assert bytes(client.download_version_buffer(version)) == b"example weights"
finally:
    client.close()
    origin.shutdown()
```

This example demonstrates local weight transfer. Deploy model inference through
LiteRegistry gateways and registered model services.

## Share middles between training runs

Middles can start with an empty subscription list and accept publishers later.
Each publisher gets separate caches, transfer endpoints, and recovery state.
See [shared CPU middles](multiple-publishers.md) for configuration, memory limits,
and safe subscription updates.

## HTTP and UCXX

HTTP provides the baseline transport. UCXX is optional and requires a compatible
runtime and network. Enabling UCXX does not itself prove RDMA use or a speedup.
Compare end-to-end weight-update time on the target hardware before changing the
transport. See the [measured adapter fan-out benchmark](benchmarks.md) for HTTP
and UCXX results, including startup failures and measurement limits.
The [validation record](validation.md) distinguishes tested behavior
from hardware performance that has not been established.

## Run this documentation server

```bash
uv run --extra docs mkdocs serve --dev-addr 127.0.0.1:25439
```

Open <http://localhost:25439>. Changes reload automatically. For a static build:

```bash
uv run --extra docs mkdocs build --strict
```

The built site is written to `site/`.
