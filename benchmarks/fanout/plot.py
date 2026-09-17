"""Render measured fan-out latency and throughput, with min/max repeat ranges."""

import json
from pathlib import Path
import statistics
import sys
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

root = Path(sys.argv[1])
out = Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
rows = json.loads((root / "results.json").read_text())
topology = json.loads((root / "topology.json").read_text())
placement = topology.get("middle_placement", "cpu").upper()
valid = [r for r in rows if "error" not in r and not r["warmup"]]
plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.titleweight": "bold",
        "svg.fonttype": "none",
    }
)
fig, axes = plt.subplots(1, 2, figsize=(12, 5.5))
fig.patch.set_facecolor("#f8fafc")
colors = {1: "#2563eb", 2: "#0d9488", 4: "#9333ea"}
summary = []
for mode in ["http", "ucxx"]:
    for m in [1, 2, 4]:
        series = []
        for c in [1, 2, 4, 8]:
            group = [r for r in valid if r["transport"] == mode and r["middles"] == m and r["clients"] == c]
            if not group:
                continue
            record = {"transport": mode, "middles": m, "clients": c, "n": len(group)}
            for key in ["seconds", "aggregate_GiB_s"]:
                values = [r[key] for r in group]
                record[key] = statistics.median(values)
                record[key + "_min"] = min(values)
                record[key + "_max"] = max(values)
            summary.append(record)
            series.append(record)
        if not series:
            continue
        for ax, key, scale in zip(axes, ["seconds", "aggregate_GiB_s"], [1000, 1]):
            x = [r["clients"] for r in series]
            y = [r[key] * scale for r in series]
            ax.plot(
                x,
                y,
                color=colors[m],
                marker="o" if mode == "http" else "s",
                linestyle="-" if mode == "http" else "--",
                linewidth=2.3,
                label=f"{m} middle" + ("s" if m > 1 else "") + f" · {mode.upper()}",
            )
            ax.fill_between(
                x, [r[key + "_min"] * scale for r in series], [r[key + "_max"] * scale for r in series], color=colors[m], alpha=0.10
            )
for ax in axes:
    ax.set_facecolor("white")
    ax.grid(axis="y", alpha=0.2)
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xlabel("Concurrent clients")
    ax.set_ylim(bottom=0)
axes[0].set_title("Time until all clients receive the payload", loc="left", fontsize=12, pad=14)
axes[0].set_ylabel("Transfer time (ms) · lower is better")
axes[1].set_title("Aggregate delivery throughput", loc="left", fontsize=12, pad=14)
axes[1].set_ylabel("GiB/s · higher is better")
axes[1].legend(frameon=False, fontsize=9)
synthetic = topology["origin"]["source"].startswith("synthetic")
size_label = f"{topology['origin']['bytes'] / 1024**3:g} GiB" if synthetic else "162 MiB"
fig.suptitle(
    "4 GiB payload fan-out" if synthetic else "Rank-32 adapter fan-out", x=0.065, ha="left", fontsize=23, fontweight="bold", y=0.98
)
fig.text(
    0.065,
    0.89,
    f"{'Synthetic weight-sized buffer' if synthetic else 'Qwen3.5-4B'}  •  {size_label} per client  •  {placement}-host middles",
    color="#475569",
    fontsize=12,
)
has_ucxx = any(r["transport"] == "ucxx" for r in valid)
footer = "Median of 3 measured rounds; shading shows min–max. One warm-up per configuration excluded.\nFour GPU client hosts (up to 2 clients/host); existing active allocations. CPU buffers; no model loading."
if any("error" in r for r in rows):
    footer += "\nUCXX connection failures stopped further UCXX trials; missing configurations are unmeasured."
if not has_ucxx:
    footer += "\nUCXX fan-out unavailable: connection setup failed. HTTP points contain no UCXX fallback."
fig.text(0.065, 0.025, footer, color="#475569", fontsize=9, linespacing=1.5)
fig.subplots_adjust(left=0.075, right=0.97, top=0.79, bottom=0.25, wspace=0.28)
for ext in ["svg", "png"]:
    fig.savefig(out / ("adapter-fanout." + ext), dpi=180, facecolor=fig.get_facecolor())
(out / "fanout-summary.json").write_text(json.dumps(summary, indent=2))
(out / "fanout-results.json").write_text(json.dumps({"topology": topology, "rounds": rows}, indent=2))
print(json.dumps(summary, indent=2))
