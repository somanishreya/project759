To run the simulation:

First compile the gem5 simulator for Garnet standalone mode:

```bash
scons build/Garnet_standalone/gem5.opt -j16
```

Then run the simulation:

```bash
./build/Garnet_standalone/gem5.opt configs/example/garnet_synth_traffic.py          --num-cpus=64        --num-dirs=64         --network=garnet         --topology=Mesh_XY         --mesh-rows=8          --sim-cycles=10000000         --synthetic=uniform_random         --injectionrate=0.1
```