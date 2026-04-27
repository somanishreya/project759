import subprocess
import os
import re
import csv
import math

# Configuration
GEM5_BINARY = "./build/Garnet_standalone/gem5.opt"
CFG_SCRIPT = "configs/example/garnet_synth_traffic.py"
SIM_CYCLES = 10000000 # Use a smaller value if testing, but the user requested 10^9
SYNTHETIC = "uniform_random"
TOPOLOGY = "Mesh_XY"
NETWORK = "garnet"

# Parameters to sweep
NUM_CPUS_LIST = [16, 64] # Example sizes: 4x4 and 8x8
INJECTION_RATES = [0.01, 0.05, 0.1, 0.15, 0.2]

output_file = "simulation_results.csv"

def run_simulation(num_cpus, injection_rate):
    rows = int(math.sqrt(num_cpus))
    outdir = f"results/cpu{num_cpus}_rate{injection_rate}"
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
        f"--injectionrate={injection_rate}"
    ]
    
    print(f"Running: {' '.join(cmd)}")
    try:
        # Run simulation
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Extract hostSeconds from the specific outdir
        stats_path = os.path.join(outdir, "stats.txt")
        host_seconds = None
        if os.path.exists(stats_path):
            with open(stats_path, 'r') as f:
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

def main():
    # Header for CSV
    headers = ["num_cpus", "injection_rate", "host_seconds"]
    file_exists = os.path.isfile(output_file)
    
    with open(output_file, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)
        
        for num_cpus in NUM_CPUS_LIST:
            for rate in INJECTION_RATES:
                print(f"Starting experiment: num_cpus={num_cpus}, rate={rate}")
                host_seconds = run_simulation(num_cpus, rate)
                if host_seconds is not None:
                    writer.writerow([num_cpus, rate, host_seconds])
                    print(f"Result: {host_seconds}s")
                else:
                    print("Failed to get results.")
                f.flush() # Ensure data is written after each run

if __name__ == "__main__":
    main()
