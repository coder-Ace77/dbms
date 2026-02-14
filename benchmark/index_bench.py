#!/usr/bin/env python3
"""
DocDB Index Benchmark — 90% Read / 10% Write
==============================================
Compares performance WITH and WITHOUT a B+ Tree index.

Phase 1: No Index  — 90% find(filter) + 10% insert
Phase 2: With Index — same workload after creating an index on the filter field

Generates a comparison plot.
"""

import time
import random
import string
import os
import sys
import json
import statistics

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client import DocDBClient

# Config
HOST = "127.0.0.1"
PORT = 6379
COLLECTION = "idx_bench"
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

SEED_SIZES = [200, 500, 1000, 2000]
OPS_PER_RUN = 500  # Total mixed operations per workload size
READ_RATIO = 0.9
CITIES = ["NYC", "LA", "Chicago", "Houston", "Phoenix", "Dallas", "Austin", "Denver"]


def random_name(n=6):
    return "".join(random.choices(string.ascii_lowercase, k=n))


def random_doc(i):
    return {
        "name": f"user_{i}_{random_name(4)}",
        "age": random.randint(18, 65),
        "city": random.choice(CITIES),
        "score": round(random.uniform(0, 100), 2),
    }


def run_mixed_workload(db, label, seed_size, ops):
    """Run a 90/10 read/write workload and measure latencies."""
    read_latencies = []
    write_latencies = []
    write_counter = seed_size

    for _ in range(ops):
        if random.random() < READ_RATIO:
            # READ: find by city filter
            city = random.choice(CITIES)
            t0 = time.perf_counter()
            db.find(COLLECTION, {"city": city})
            t1 = time.perf_counter()
            read_latencies.append((t1 - t0) * 1000)
        else:
            # WRITE: insert a new document
            doc = random_doc(write_counter)
            write_counter += 1
            t0 = time.perf_counter()
            db.insert(COLLECTION, doc)
            t1 = time.perf_counter()
            write_latencies.append((t1 - t0) * 1000)

    return read_latencies, write_latencies


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    random.seed(42)

    results = {
        "no_index": {
            "sizes": [], "read_avg_ms": [], "read_p99_ms": [],
            "write_avg_ms": [], "write_p99_ms": [],
            "total_throughput": [],
        },
        "with_index": {
            "sizes": [], "read_avg_ms": [], "read_p99_ms": [],
            "write_avg_ms": [], "write_p99_ms": [],
            "total_throughput": [],
        },
    }

    with DocDBClient(HOST, PORT) as db:
        assert db.ping() == "pong", "Server not reachable!"

        print("=" * 65)
        print("  DocDB Index Benchmark — 90% Read / 10% Write")
        print("=" * 65)
        print(f"  Server: {HOST}:{PORT}")
        print(f"  Seed sizes: {SEED_SIZES}")
        print(f"  Operations per run: {OPS_PER_RUN}")
        print(f"  Read/Write ratio: {int(READ_RATIO*100)}% / {int((1-READ_RATIO)*100)}%")
        print()

        for seed_size in SEED_SIZES:
            print(f"\n{'='*65}")
            print(f"  Seed size: {seed_size} documents")
            print(f"{'='*65}")

            # ============================================================
            # Phase 1: NO INDEX
            # ============================================================
            db.drop_collection(COLLECTION)
            db.create_collection(COLLECTION)

            # Seed data
            print(f"  Seeding {seed_size} documents...", end=" ", flush=True)
            for i in range(seed_size):
                db.insert(COLLECTION, random_doc(i))
            print("done")

            print(f"  [NO INDEX] Running {OPS_PER_RUN} ops (90R/10W)...", end=" ", flush=True)
            t_start = time.perf_counter()
            rl, wl = run_mixed_workload(db, "no_index", seed_size, OPS_PER_RUN)
            t_total = time.perf_counter() - t_start
            throughput = OPS_PER_RUN / t_total

            r_avg = statistics.mean(rl) if rl else 0
            r_p99 = sorted(rl)[int(len(rl) * 0.99)] if rl else 0
            w_avg = statistics.mean(wl) if wl else 0
            w_p99 = sorted(wl)[int(len(wl) * 0.99)] if wl else 0

            results["no_index"]["sizes"].append(seed_size)
            results["no_index"]["read_avg_ms"].append(round(r_avg, 3))
            results["no_index"]["read_p99_ms"].append(round(r_p99, 3))
            results["no_index"]["write_avg_ms"].append(round(w_avg, 3))
            results["no_index"]["write_p99_ms"].append(round(w_p99, 3))
            results["no_index"]["total_throughput"].append(round(throughput, 1))

            print(f"done")
            print(f"         Read:  avg={r_avg:.3f}ms  p99={r_p99:.3f}ms")
            print(f"         Write: avg={w_avg:.3f}ms  p99={w_p99:.3f}ms")
            print(f"         Throughput: {throughput:.0f} ops/s")

            # ============================================================
            # Phase 2: WITH INDEX on "city"
            # ============================================================
            # Drop and re-seed to have the same starting point
            db.drop_collection(COLLECTION)
            db.create_collection(COLLECTION)

            for i in range(seed_size):
                db.insert(COLLECTION, random_doc(i))

            # Create index
            print(f"  Creating index on 'city'...", end=" ", flush=True)
            db.create_index(COLLECTION, "city")
            print("done")

            print(f"  [WITH INDEX] Running {OPS_PER_RUN} ops (90R/10W)...", end=" ", flush=True)
            t_start = time.perf_counter()
            rl2, wl2 = run_mixed_workload(db, "with_index", seed_size, OPS_PER_RUN)
            t_total2 = time.perf_counter() - t_start
            throughput2 = OPS_PER_RUN / t_total2

            r_avg2 = statistics.mean(rl2) if rl2 else 0
            r_p992 = sorted(rl2)[int(len(rl2) * 0.99)] if rl2 else 0
            w_avg2 = statistics.mean(wl2) if wl2 else 0
            w_p992 = sorted(wl2)[int(len(wl2) * 0.99)] if wl2 else 0

            results["with_index"]["sizes"].append(seed_size)
            results["with_index"]["read_avg_ms"].append(round(r_avg2, 3))
            results["with_index"]["read_p99_ms"].append(round(r_p992, 3))
            results["with_index"]["write_avg_ms"].append(round(w_avg2, 3))
            results["with_index"]["write_p99_ms"].append(round(w_p992, 3))
            results["with_index"]["total_throughput"].append(round(throughput2, 1))

            print(f"done")
            print(f"         Read:  avg={r_avg2:.3f}ms  p99={r_p992:.3f}ms")
            print(f"         Write: avg={w_avg2:.3f}ms  p99={w_p992:.3f}ms")
            print(f"         Throughput: {throughput2:.0f} ops/s")

            # Speedup
            if r_avg > 0:
                speedup = r_avg / r_avg2
                print(f"         📊 Read speedup with index: {speedup:.1f}x")

        # Cleanup
        db.drop_collection(COLLECTION)

    # Save results
    with open(os.path.join(RESULTS_DIR, "index_benchmark.json"), "w") as f:
        json.dump(results, f, indent=2)

    # ============================================================
    # Plot
    # ============================================================
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("\nmatplotlib not installed, skipping plots.")
        return

    plt.style.use("dark_background")

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("DocDB: Index vs No-Index  —  90% Read / 10% Write",
                 fontsize=16, fontweight="bold", color="white", y=0.98)

    sizes = results["no_index"]["sizes"]
    c_no = "#ff5252"
    c_idx = "#00d4aa"

    # ---- Top Left: Read Avg Latency ----
    ax = axes[0][0]
    ax.plot(sizes, results["no_index"]["read_avg_ms"], "o-", color=c_no, lw=2.5, ms=8, label="No Index")
    ax.plot(sizes, results["with_index"]["read_avg_ms"], "s-", color=c_idx, lw=2.5, ms=8, label="With Index")
    ax.set_title("Read Latency (avg)", fontsize=13, fontweight="bold")
    ax.set_xlabel("Collection Size"); ax.set_ylabel("Latency (ms)")
    ax.legend(fontsize=10); ax.grid(alpha=0.3)

    # ---- Top Right: Read P99 Latency ----
    ax = axes[0][1]
    ax.plot(sizes, results["no_index"]["read_p99_ms"], "o-", color=c_no, lw=2.5, ms=8, label="No Index")
    ax.plot(sizes, results["with_index"]["read_p99_ms"], "s-", color=c_idx, lw=2.5, ms=8, label="With Index")
    ax.set_title("Read Latency (P99)", fontsize=13, fontweight="bold")
    ax.set_xlabel("Collection Size"); ax.set_ylabel("Latency (ms)")
    ax.legend(fontsize=10); ax.grid(alpha=0.3)

    # ---- Bottom Left: Write Avg Latency ----
    ax = axes[1][0]
    ax.plot(sizes, results["no_index"]["write_avg_ms"], "o-", color=c_no, lw=2.5, ms=8, label="No Index")
    ax.plot(sizes, results["with_index"]["write_avg_ms"], "s-", color=c_idx, lw=2.5, ms=8, label="With Index")
    ax.set_title("Write Latency (avg)", fontsize=13, fontweight="bold")
    ax.set_xlabel("Collection Size"); ax.set_ylabel("Latency (ms)")
    ax.legend(fontsize=10); ax.grid(alpha=0.3)

    # ---- Bottom Right: Overall Throughput ----
    ax = axes[1][1]
    x_pos = range(len(sizes))
    w = 0.35
    bars1 = ax.bar([p - w/2 for p in x_pos], results["no_index"]["total_throughput"],
                   w, color=c_no, label="No Index", edgecolor="white", linewidth=0.5)
    bars2 = ax.bar([p + w/2 for p in x_pos], results["with_index"]["total_throughput"],
                   w, color=c_idx, label="With Index", edgecolor="white", linewidth=0.5)
    for bar, val in zip(bars1, results["no_index"]["total_throughput"]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f"{val:.0f}", ha="center", fontsize=8, color="white", fontweight="bold")
    for bar, val in zip(bars2, results["with_index"]["total_throughput"]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f"{val:.0f}", ha="center", fontsize=8, color="white", fontweight="bold")
    ax.set_xticks(list(x_pos))
    ax.set_xticklabels([str(s) for s in sizes])
    ax.set_title("Overall Throughput (90R/10W)", fontsize=13, fontweight="bold")
    ax.set_xlabel("Collection Size"); ax.set_ylabel("ops/sec")
    ax.legend(fontsize=10); ax.grid(axis="y", alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    path = os.path.join(RESULTS_DIR, "index_comparison.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"\n  Plot saved: {path}")
    plt.close("all")
    print("Done!")


if __name__ == "__main__":
    main()
