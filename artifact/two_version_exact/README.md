# Exact Two-Version Line Instance

This directory contains a small exact model that explains why useful version selection can differ from both sending every version and sending only the final version.

The instance has three sites on an A-B-C line, two cumulative versions, actual source-object sizes from the included manifest, and link rates of 240 and 640 bytes per tick. It enumerates all eight subsets of destinations that may receive the intermediate closure. For each subset, it solves a time-expanded transfer model with releases, per-link capacity, store-and-forward order, complete same-version installation, and final-version completion.

The lexicographic objectives are:

1. maximize weighted query time that meets the freshness limit;
2. minimize occupied edge ticks among the first-stage optima.

Run from the repository root:

```bash
python artifact/two_version_exact/run_two_version_exact.py
python artifact/two_version_exact/verify_two_version_exact.py
```

The frozen selected-closure solution has utility 6 and work 22 edge ticks. The exact all-version control has utility 6 and work 31. The final-only control has utility 3 and work 16. These controls isolate the value of selecting one useful intermediate closure.

`solve_elapsed_ns` records machine-dependent solver time, so full JSON hashes can vary across executions even when decisions and scientific results match. The independent verifier compares schedules, objective values, constraints, and all eight subset solutions rather than treating elapsed time as a reproducibility target.
