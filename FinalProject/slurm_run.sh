#!/bin/bash
#SBATCH --job-name=gem5run
#SBATCH --partition=research
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=16G
#SBATCH --output=slurm-%j.out

module purge
module load gcc/11.2.0
module load python/3.10.4

# Ensure pip-installed SCons is visible
export PATH=$HOME/.local/bin:$PATH

# Work in home directory
cd $HOME

# Clone fresh copy of project
rm -rf project759
git clone https://github.com/somanishreya/project759.git

cd project759/FinalProject

echo "=== Building gem5 (Garnet_standalone) ==="
cd gem5
scons build/Garnet_standalone/gem5.opt -j16
echo "Build exit code: $?"
cd ..

echo "=== Running parallel experiment ==="
python3 run_parallel.py

echo "=== Done ==="


