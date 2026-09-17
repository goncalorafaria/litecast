# Validation

Validated on September 17, 2026 with Python 3.12.12 and LiteRegistry 1.0.54 from
the development environment. Core transport and registry integration tests ran
without importing PrimeRL or loading a GPU model.

- 119 existing/extracted tests passed in 74.39 seconds.
- 8 publisher configuration tests passed in 0.88 seconds.
- Ruff checks passed after import cleanup.
- Source distribution and Python wheel built successfully with `uv build`.

The test suite includes real HTTP origin/middle/client transfers, bounded stores,
manifest integrity, selective transfer, streaming, mocked UCXX protocol behavior,
real Redis publisher fencing (direct and SQLite head discovery), and independent
middle subprocesses accepting multiple publishers after startup. It exercises
malformed subscription reloads, publisher replacement, and detaching B without
rebinding A's middle endpoints.

These results do not establish GPU multi-trainer correctness, RDMA hardware
throughput, or zero-downtime availability. A separate two-A40 PrimeRL late-publisher test passed after correcting its harness's
discovery call and optional Slurm metadata handling. It used two independent GPU
allocations on one physical GPU host, a separate CPU coordinator, two CPU middle
processes, SQLite head discovery, Redis, and a local LiteRegistry gateway.

Publisher A was already serving when B was added to both running sidecars and
middles. B joined both replicas in 8.55 seconds; A completed five requests during
joining. Both replicas then served both adapters with distinct log probabilities,
without an engine or sidecar restart. The 87,319,247-byte adapters came from saved
training outputs; there were no optimizer steps or simultaneous retained-state
updates in this test. Initial missing-middle-source errors were retried until the
middle caches advertised readiness. See [result summary](live-late-publisher.json).

This is external PrimeRL integration evidence, not a test of installing this
standalone wheel into the running GPU environment. The 127 CPU tests above cover
the extracted standalone implementation.
