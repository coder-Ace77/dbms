#!/usr/bin/env python3
"""
DocDB Performance Benchmark
============================
Measures latency (per-operation) and throughput (ops/sec) for:
  1. INSERT   — bulk document insertion
  2. FIND     — full-collection scan
  3. FIND+F   — filtered find (equality match)
  4. UPDATE   — filtered update
  5. DELETE   — filtered delete
  6. COUNT    — count documents

Results are plotted using matplotlib and saved to benchmark/results/.
"""

import time
import random
import string
import os
import sys
import json
import statistics

# Allow running from the benchmark directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client import DocDBClient

# ============================================================================
# Configuration
# ============================================================================

HOST = "127.0.0.1"
PORT = 6379
COLLECTION = "bench_test"
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# Workload sizes for scaling tests
WORKLOAD_SIZES = [100, 500, 1000, 2000, 5000]
FIND_ITERATIONS = 50  # Number of find operations per workload


def random_name(length=8):
    return "".join(random.choices(string.ascii_lowercase, k=length))


def random_doc(i):
    """Generate a random document."""
    return {
        "name": f"user_{i}_{random_name(4)}",
        "age": random.randint(18, 65),
        "city": random.choice(["NYC", "LA", "Chicago", "Houston", "Phoenix"]),
        "score": round(random.uniform(0, 100), 2),
        "active": random.choice([True, False]),
    }


# ============================================================================
# Benchmark Runner
# ============================================================================

def benchmark_operation(db, name, op_func, iterations=1):
    """Run an operation, measure latency for each call."""
    latencies = []
    for _ in range(iterations):
        start = time.perf_counter()
        op_func()
        end = time.perf_counter()
        latencies.append((end - start) * 1000)  # ms
    return latencies


def run_benchmarks():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    all_results = {
        "insert": {"sizes": [], "avg_latency_ms": [], "p99_latency_ms": [], "throughput_ops": []},
        "find_all": {"sizes": [], "avg_latency_ms": [], "p99_latency_ms": [], "throughput_ops": []},
        "find_filter": {"sizes": [], "avg_latency_ms": [], "p99_latency_ms": [], "throughput_ops": []},
        "count": {"sizes": [], "avg_latency_ms": [], "p99_latency_ms": [], "throughput_ops": []},
        "update": {"sizes": [], "avg_latency_ms": [], "p99_latency_ms": [], "throughput_ops": []},
        "delete": {"sizes": [], "avg_latency_ms": [], "p99_latency_ms": [], "throughput_ops": []},
    }

    with DocDBClient(HOST, PORT) as db:
        print("=" * 60)
        print("  DocDB Performance Benchmark")
        print("=" * 60)
        print(f"  Server: {HOST}:{PORT}")
        print(f"  Workload sizes: {WORKLOAD_SIZES}")
        print()

        # Verify connection
        assert db.ping() == "pong", "Server not reachable!"

        for size in WORKLOAD_SIZES:
            print(f"\n--- Workload: {size} documents ---")

            # Clean up
            db.drop_collection(COLLECTION)
            db.create_collection(COLLECTION)

            # ---- 1. INSERT benchmark ----
            print(f"  INSERT x{size}...", end=" ", flush=True)
            docs = [random_doc(i) for i in range(size)]
            insert_latencies = []
            start_total = time.perf_counter()
            for doc in docs:
                start = time.perf_counter()
                db.insert(COLLECTION, doc)
                end = time.perf_counter()
                insert_latencies.append((end - start) * 1000)
            total_insert = time.perf_counter() - start_total

            avg_ins = statistics.mean(insert_latencies)
            p99_ins = sorted(insert_latencies)[int(len(insert_latencies) * 0.99)]
            tp_ins = size / total_insert

            all_results["insert"]["sizes"].append(size)
            all_results["insert"]["avg_latency_ms"].append(round(avg_ins, 3))
            all_results["insert"]["p99_latency_ms"].append(round(p99_ins, 3))
            all_results["insert"]["throughput_ops"].append(round(tp_ins, 1))
            print(f"avg={avg_ins:.3f}ms  p99={p99_ins:.3f}ms  throughput={tp_ins:.0f} ops/s")

            # ---- 2. FIND ALL benchmark ----
            print(f"  FIND_ALL x{FIND_ITERATIONS}...", end=" ", flush=True)
            find_latencies = benchmark_operation(
                db, "find_all",
                lambda: db.find(COLLECTION),
                FIND_ITERATIONS,
            )
            avg_find = statistics.mean(find_latencies)
            p99_find = sorted(find_latencies)[int(len(find_latencies) * 0.99)]
            tp_find = FIND_ITERATIONS / (sum(find_latencies) / 1000)

            all_results["find_all"]["sizes"].append(size)
            all_results["find_all"]["avg_latency_ms"].append(round(avg_find, 3))
            all_results["find_all"]["p99_latency_ms"].append(round(p99_find, 3))
            all_results["find_all"]["throughput_ops"].append(round(tp_find, 1))
            print(f"avg={avg_find:.3f}ms  p99={p99_find:.3f}ms  throughput={tp_find:.0f} ops/s")

            # ---- 3. FIND FILTERED benchmark ----
            print(f"  FIND_FILTER x{FIND_ITERATIONS}...", end=" ", flush=True)
            filter_latencies = benchmark_operation(
                db, "find_filter",
                lambda: db.find(COLLECTION, {"city": "NYC"}),
                FIND_ITERATIONS,
            )
            avg_ff = statistics.mean(filter_latencies)
            p99_ff = sorted(filter_latencies)[int(len(filter_latencies) * 0.99)]
            tp_ff = FIND_ITERATIONS / (sum(filter_latencies) / 1000)

            all_results["find_filter"]["sizes"].append(size)
            all_results["find_filter"]["avg_latency_ms"].append(round(avg_ff, 3))
            all_results["find_filter"]["p99_latency_ms"].append(round(p99_ff, 3))
            all_results["find_filter"]["throughput_ops"].append(round(tp_ff, 1))
            print(f"avg={avg_ff:.3f}ms  p99={p99_ff:.3f}ms  throughput={tp_ff:.0f} ops/s")

            # ---- 4. COUNT benchmark ----
            print(f"  COUNT x{FIND_ITERATIONS}...", end=" ", flush=True)
            count_latencies = benchmark_operation(
                db, "count",
                lambda: db.count(COLLECTION),
                FIND_ITERATIONS,
            )
            avg_cnt = statistics.mean(count_latencies)
            p99_cnt = sorted(count_latencies)[int(len(count_latencies) * 0.99)]
            tp_cnt = FIND_ITERATIONS / (sum(count_latencies) / 1000)

            all_results["count"]["sizes"].append(size)
            all_results["count"]["avg_latency_ms"].append(round(avg_cnt, 3))
            all_results["count"]["p99_latency_ms"].append(round(p99_cnt, 3))
            all_results["count"]["throughput_ops"].append(round(tp_cnt, 1))
            print(f"avg={avg_cnt:.3f}ms  p99={p99_cnt:.3f}ms  throughput={tp_cnt:.0f} ops/s")

            # ---- 5. UPDATE benchmark ----
            num_updates = min(50, size)
            print(f"  UPDATE x{num_updates}...", end=" ", flush=True)
            update_latencies = []
            for i in range(num_updates):
                start = time.perf_counter()
                db.update(COLLECTION, {"city": "NYC"}, {"score": 99.9})
                end = time.perf_counter()
                update_latencies.append((end - start) * 1000)

            avg_upd = statistics.mean(update_latencies)
            p99_upd = sorted(update_latencies)[int(len(update_latencies) * 0.99)]
            tp_upd = num_updates / (sum(update_latencies) / 1000)

            all_results["update"]["sizes"].append(size)
            all_results["update"]["avg_latency_ms"].append(round(avg_upd, 3))
            all_results["update"]["p99_latency_ms"].append(round(p99_upd, 3))
            all_results["update"]["throughput_ops"].append(round(tp_upd, 1))
            print(f"avg={avg_upd:.3f}ms  p99={p99_upd:.3f}ms  throughput={tp_upd:.0f} ops/s")

            # ---- 6. DELETE benchmark (delete a subset by filter) ----
            num_deletes = min(20, size)
            print(f"  DELETE x{num_deletes}...", end=" ", flush=True)
            delete_latencies = []
            # Insert specific docs to delete
            for i in range(num_deletes):
                db.insert(COLLECTION, {"name": f"delete_me_{i}", "age": 99, "city": "DEL"})
            for i in range(num_deletes):
                start = time.perf_counter()
                db.delete(COLLECTION, {"name": f"delete_me_{i}"})
                end = time.perf_counter()
                delete_latencies.append((end - start) * 1000)

            avg_del = statistics.mean(delete_latencies)
            p99_del = sorted(delete_latencies)[int(len(delete_latencies) * 0.99)]
            tp_del = num_deletes / (sum(delete_latencies) / 1000)

            all_results["delete"]["sizes"].append(size)
            all_results["delete"]["avg_latency_ms"].append(round(avg_del, 3))
            all_results["delete"]["p99_latency_ms"].append(round(p99_del, 3))
            all_results["delete"]["throughput_ops"].append(round(tp_del, 1))
            print(f"avg={avg_del:.3f}ms  p99={p99_del:.3f}ms  throughput={tp_del:.0f} ops/s")

        # Cleanup
        db.drop_collection(COLLECTION)

    # Save raw results
    results_path = os.path.join(RESULTS_DIR, "benchmark_results.json")
    with open(results_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {results_path}")

    return all_results


# ============================================================================
# Plotting
# ============================================================================

def plot_results(results):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as ticker
    except ImportError:
        print("matplotlib not installed. Skipping plots.")
        print("Install with: pip install matplotlib")
        return

    # Style
    plt.style.use("dark_background")
    colors = {
        "insert": "#00d4aa",
        "find_all": "#4fc3f7",
        "find_filter": "#7c4dff",
        "count": "#ffab40",
        "update": "#ff5252",
        "delete": "#e040fb",
    }
    labels = {
        "insert": "INSERT",
        "find_all": "FIND (all)",
        "find_filter": "FIND (filtered)",
        "count": "COUNT",
        "update": "UPDATE",
        "delete": "DELETE",
    }

    # ---- Figure 1: Avg Latency vs Collection Size ----
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("DocDB Performance Benchmark", fontsize=16, fontweight="bold", color="white")

    for op, data in results.items():
        if data["sizes"]:
            ax1.plot(data["sizes"], data["avg_latency_ms"],
                     marker="o", linewidth=2, markersize=6,
                     color=colors.get(op, "white"),
                     label=labels.get(op, op))

    ax1.set_xlabel("Collection Size (documents)", fontsize=12)
    ax1.set_ylabel("Average Latency (ms)", fontsize=12)
    ax1.set_title("Latency vs Collection Size", fontsize=13, fontweight="bold")
    ax1.legend(fontsize=9, loc="upper left")
    ax1.grid(alpha=0.3)
    ax1.set_xscale("log")

    # ---- Figure 2: Throughput vs Collection Size ----
    for op, data in results.items():
        if data["sizes"]:
            ax2.plot(data["sizes"], data["throughput_ops"],
                     marker="s", linewidth=2, markersize=6,
                     color=colors.get(op, "white"),
                     label=labels.get(op, op))

    ax2.set_xlabel("Collection Size (documents)", fontsize=12)
    ax2.set_ylabel("Throughput (ops/sec)", fontsize=12)
    ax2.set_title("Throughput vs Collection Size", fontsize=13, fontweight="bold")
    ax2.legend(fontsize=9, loc="upper right")
    ax2.grid(alpha=0.3)
    ax2.set_xscale("log")

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    path1 = os.path.join(RESULTS_DIR, "latency_throughput.png")
    fig.savefig(path1, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"  Plot saved: {path1}")

    # ---- Figure 3: P99 Latency bar chart (at max workload size) ----
    fig2, ax3 = plt.subplots(figsize=(10, 5))
    fig2.set_facecolor("#1e1e2e")
    ax3.set_facecolor("#1e1e2e")

    ops = [op for op in results if results[op]["p99_latency_ms"]]
    p99_vals = [results[op]["p99_latency_ms"][-1] for op in ops]
    bar_colors = [colors.get(op, "white") for op in ops]
    bar_labels = [labels.get(op, op) for op in ops]

    bars = ax3.bar(bar_labels, p99_vals, color=bar_colors, width=0.6, edgecolor="white", linewidth=0.5)

    # Add value labels on bars
    for bar, val in zip(bars, p99_vals):
        ax3.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                 f"{val:.2f}ms", ha="center", va="bottom", fontsize=10,
                 fontweight="bold", color="white")

    max_size = max(results[ops[0]]["sizes"]) if ops else 0
    ax3.set_title(f"P99 Latency at {max_size} Documents", fontsize=14, fontweight="bold", color="white")
    ax3.set_ylabel("P99 Latency (ms)", fontsize=12, color="white")
    ax3.grid(axis="y", alpha=0.3)

    path2 = os.path.join(RESULTS_DIR, "p99_latency.png")
    fig2.savefig(path2, dpi=150, bbox_inches="tight", facecolor=fig2.get_facecolor())
    print(f"  Plot saved: {path2}")

    # ---- Figure 4: Insert throughput bar chart ----
    fig3, ax4 = plt.subplots(figsize=(10, 5))
    fig3.set_facecolor("#1e1e2e")
    ax4.set_facecolor("#1e1e2e")

    ins = results["insert"]
    bars = ax4.bar(
        [str(s) for s in ins["sizes"]], ins["throughput_ops"],
        color=colors["insert"], width=0.6, edgecolor="white", linewidth=0.5,
    )
    for bar, val in zip(bars, ins["throughput_ops"]):
        ax4.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                 f"{val:.0f}", ha="center", va="bottom", fontsize=10,
                 fontweight="bold", color="white")

    ax4.set_title("Insert Throughput vs Batch Size", fontsize=14, fontweight="bold", color="white")
    ax4.set_xlabel("Batch Size", fontsize=12, color="white")
    ax4.set_ylabel("Throughput (ops/sec)", fontsize=12, color="white")
    ax4.grid(axis="y", alpha=0.3)

    path3 = os.path.join(RESULTS_DIR, "insert_throughput.png")
    fig3.savefig(path3, dpi=150, bbox_inches="tight", facecolor=fig3.get_facecolor())
    print(f"  Plot saved: {path3}")

    plt.close("all")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    results = run_benchmarks()
    print("\n--- Generating Plots ---")
    plot_results(results)
    print("\nDone!")
