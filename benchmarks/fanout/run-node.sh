#!/usr/bin/env bash
set -euo pipefail
export RAPIDS_LIBUCX_PREFER_SYSTEM_LIBRARY=true
export UCX_MODULE_DIR=/usr/lib64/ucx
export LD_PRELOAD=/usr/lib64/libucs.so.0:/usr/lib64/libuct.so.0:/usr/lib64/libucp.so.0
export UCX_NET_DEVICES=mlx5_0:1
export UCX_SOCKADDR_TLS_PRIORITY=rdmacm
export BENCH_IB_ADDRESS=$(ip -4 -o addr show ib0 | awk '{split($4,a,"/"); print a[1]}')
export PYTHONPATH=/gscratch/ark/graf/litecast
exec /gscratch/ark/graf/.local/bin/uv run --no-project --python /gscratch/ark/graf/miniconda3/envs/multilora/bin/python python -u /gscratch/ark/graf/litecast/benchmarks/fanout/node.py "$@"
