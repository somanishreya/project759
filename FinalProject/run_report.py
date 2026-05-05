import csv
import math
import os
import re
import subprocess

# ----------------------------------------------------------------------
# Path Setup
# ----------------------------------------------------------------------

# Project root = directory where this script lives
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# gem5 root directory
GEM5_ROOT = os.path.join(PROJECT_ROOT, "gem5")

# gem5 binary and config script (absolute paths)
GEM5_BINARY = os.path.join(GEM5_ROOT, "build/Garnet_standalone/gem5.opt")
CFG_SCRIPT = os.path.join(GEM5_ROOT, "configs/example/garnet_synth_traffic.py")

# Output directory for all results
RESULTS_ROOT = os.path.join(PROJECT_ROOT, "results")
os.makedirs(RESULTS_ROOT, exist_ok=True)

# ----------------------------------------------------------------------
# Experiment Configuration
# ----------------------------------------------------------------------

SIM_CYCLES = 1000000000
SYNTHETIC = "uniform_random"
TOPOLOGY = "Mesh_XY"
NETWORK = "garnet"

NUM_THREADS_LIST = [1, 4, 8, 16]
NUM_CPUS_LIST = [16, 64]  # 4x4 and 8x8
INJECTION_RATES = [0.01, 0.05, 0.1, 0.2, 0.5, 0.8, 1.0]

output_file = os.path.join(PROJECT_ROOT, "simulation_results.csv")


# ----------------------------------------------------------------------
# Run a Single Simulation
# ----------------------------------------------------------------------
def run_simulation(num_cpus, injection_rate, num_threads):
    rows = int(math.sqrt(num_cpus))

    outdir = os.path.join(
        RESULTS_ROOT,
        f"cpu{num_cpus}_threads{num_threads}_rate{injection_rate}"
    )
    os.makedirs(outdir, exist_ok=True)

    cmd = [
        GEM5_BINARY,
        f"--outdir={outdir}",
        CFG_SCRIPT,
        f"--num-cpus={num_cpus}",
        f"--num-dirs={num_cpus}",
        f"--network={NETWORK}",
        f"--topology={TOPOLOGY}",
        f"--mesh-rows={rows}",
        f"--sim-cycles={SIM_CYCLES}",
        f"--synthetic={SYNTHETIC}",
        f"--injectionrate={injection_rate}",
        f"--num-threads={num_threads}",
    ]

    print(f"Running: {' '.join(cmd)}")

    try:
        subprocess.run(
            cmd,
            cwd=GEM5_ROOT,  # <-- run gem5 from inside gem5/
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Extract hostSeconds
        stats_path = os.path.join(outdir, "stats.txt")
        host_seconds = None

        if os.path.exists(stats_path):
            with open(stats_path) as f:
                for line in f:
                    if "hostSeconds" in line:
                        match = re.search(r"hostSeconds\s+([\d\.]+)", line)
                        if match:
                            host_seconds = float(match.group(1))
                            break

        return host_seconds

    except subprocess.CalledProcessError as e:
        print(f"Error running simulation: {e}")
        return None


# ----------------------------------------------------------------------
# Main Sweep
# ----------------------------------------------------------------------
def main():
    headers = ["num_cpus", "mesh_dim", "injection_rate", "num_threads", "host_seconds"]
    file_exists = os.path.isfile(output_file)

    with open(output_file, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)

        for num_cpus in NUM_CPUS_LIST:
            mesh_dim = int(math.sqrt(num_cpus))

            for num_threads in NUM_THREADS_LIST:
                for rate in INJECTION_RATES:

                    print(
                        f"Experiment: CPUs={num_cpus} ({mesh_dim}x{mesh_dim}), "
                        f"threads={num_threads}, rate={rate}"
                    )

                    host_seconds = run_simulation(num_cpus, rate, num_threads)

                    if host_seconds is not None:
                        writer.writerow(
                            [num_cpus, mesh_dim, rate, num_threads, host_seconds]
                        )
                        print(f"Result: {host_seconds}s")
                    else:
                        print("Failed to get results.")

                    f.flush()


if __name__ == "__main__":
    main()

