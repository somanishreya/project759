# **README.md**

# **Parallelizing Garnet in gem5**

This project evaluates a parallelized version of Garnet, the Network‑on‑Chip (NoC) simulator inside gem5. We introduce OpenMP‑based parallelism into the router pipeline and measure performance across multiple mesh sizes, thread counts, and injection rates. All experiments are fully automated and reproducible using the provided scripts.

---

## **Repository Structure**

```
FinalProject/
│
├── gem5/                     # Clean clone of official gem5 source tree
│   ├── src/
│   ├── configs/
│   ├── ext/
│   ├── SConstruct
│   └── ...                   # No modifications made to gem5 itself
│
├── run_parallel.py           # Automation script for full experimental sweep
├── slurm_run.sh              # Slurm batch script for Euler cluster
├── README.md                 # This file
└── results/                  # Auto‑generated during execution
```

### **gem5/**
A complete, unmodified clone of the official gem5 repository. All experiments use the standard `Garnet_standalone` build.

### **run_parallel.py**
Automates the entire experiment sweep:

- Builds gem5 command lines  
- Runs simulations for all CPU counts, thread counts, and injection rates  
- Extracts `hostSeconds` from each run  
- Writes results to `simulation_results.csv`  
- Creates per‑run output directories under `results/`

### **slurm_run.sh**
A fully automated Slurm script for the Euler cluster. It:

1. Clones the project into the user’s home directory  
2. Builds gem5 (`Garnet_standalone`)  
3. Runs `run_parallel.py`  
4. Produces all CSV files and result folders automatically  

No manual setup is required beyond submitting this script.

### **results/**
Created automatically during execution. Contains folders such as:

```
results/cpu64_threads4_rate0.1/
results/cpu16_threads8_rate0.2/
simulation_results.csv
```

Each folder contains the gem5 `stats.txt` file for that run.

---

## **Prerequisites**

### **1. SCons (required for gem5 build)**  
Euler does not provide SCons as a module. Install it once:

```
pip install --user scons
```

This places SCons in:

```
$HOME/.local/bin/scons
```

The Slurm script automatically adds this to your PATH.

### **2. Required Modules on Euler**
The Slurm script loads:

```
gcc/11.2.0
python/3.10.4
```

---

## **How to Build and Run on Euler**

To reproduce all results, simply submit the Slurm script:

```
sbatch slurm_run.sh
```

The script will:

1. Clone a fresh copy of the repository  
2. Build gem5 using:

   ```
   scons build/Garnet_standalone/gem5.opt -j16
   ```

3. Run the full experiment sweep via:

   ```
   python3 run_parallel.py
   ```

4. Store all results in `FinalProject/results/`

No additional steps are required.

---

## **Output Files**

### **simulation_results.csv**
A consolidated CSV containing:

- `num_cpus`  
- `mesh_dim`  
- `injection_rate`  
- `num_threads`  
- `host_seconds`  

This file is used for all plotting and analysis.

### **Per‑run directories**
Each run produces:

```
stats.txt
config.ini
system.pc.com_1.trace
```

Only `stats.txt` is used for extracting runtime.

---

## **Workloads**

All experiments use gem5’s built‑in synthetic traffic generator:

```
configs/example/garnet_synth_traffic.py
```

No external input files are required.

---

## **Reproducibility**

This project is fully reproducible:

- A clean gem5 clone is used for every run  
- All paths are absolute  
- All results are generated automatically  
- No manual editing or environment setup is needed  
