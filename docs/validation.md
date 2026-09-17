# Validation

Validated on September 17, 2026 with Python 3.12.11 and LiteRegistry 1.0.54 from
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
throughput, or zero-downtime availability. A separate two-A40 PrimeRL late-publisher
test was launched; it is not part of this standalone test suite. Its first attempt
was blocked by its harness invoking synchronous head publication inside an async
loop; that harness was corrected and retried. Do not treat launch as a passing
GPU result.
