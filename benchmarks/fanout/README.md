# Cache fan-out benchmark

These scripts launch temporary origin, middle, and client processes inside
existing Rex-managed Slurm allocations. They do not launch model inference.
Update the job IDs, adapter path, Python environment, and UCX library paths for
your cluster before running. Each process stops after 30 minutes at most.

Real adapter with cross-host assignments:

```bash
BENCH_AVOID_SAME_HOST=1 python3 benchmarks/fanout/run.py
```

4 GiB synthetic payload with the same assignments:

```bash
BENCH_PAYLOAD_BYTES=4294967296 BENCH_AVOID_SAME_HOST=1 \
  python3 benchmarks/fanout/run.py
```

GPU middle placement is the default. `BENCH_MIDDLE_PLACEMENT=cpu` selects the
recorded CPU allocations instead. Same-host client/middle assignments are
allowed by default; `BENCH_AVOID_SAME_HOST=1` rotates assignments to avoid them.

The runner prints its output directory. Generate figures and shareable JSON:

```bash
python benchmarks/fanout/plot.py /path/to/run /path/to/figure-directory
```

Payloads live in CPU RAM and are checksum verified. Timings cover downloads from
already populated middle caches. They exclude initial cache population, the
external SHA-256 verification, and model installation. Internal LiteCast checksum
validation is included. Cold UCXX initialization is included in warm-up rounds,
which are retained in raw data but excluded from the plotted medians.

The matrix has 1/2/4 middles and 1/2/4/8 clients, with one warm-up and three measured
rounds per transport/configuration. A failed UCXX round stops further UCXX trials;
HTTP continues. No automatic transport fallback is enabled.
