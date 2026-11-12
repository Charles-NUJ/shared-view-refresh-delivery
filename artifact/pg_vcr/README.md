# Controlled PG-VCR Study

This directory contains the controlled study for pressure-gated version coalescing and refresh.

## Contract

`protocol.json` fixes four six-site overlays, three link capacities, four key-sharing layouts, three query-demand profiles, nine method variants, and one comparison order. Every matched method receives the same source rows, releases, encoded object sizes, topology, link rates, fixed shortest paths, installation rule, and evaluation window.

The main methods are:

- **PG-VCR (ours):** causal load gate, creation-site version coalescing, and query-impact density service.
- **FIFO-SP:** retains every version and serves released objects in FIFO order on the same fixed paths.
- **QoDA-Greedy:** retains every version and orders objects by query impact per positive remaining work.
- **VC, VD, and VC-VD:** one-control and joint-control variants used for mechanism attribution.

## Run and verify

From the repository root:

```bash
python artifact/pg_vcr/run_pressure_gate.py --output artifact/pg_vcr/output
python artifact/pg_vcr/verify_output.py
```

The runner writes copied inputs, per-case evidence for the main demand profile, `SUMMARY.csv`, `RESULTS.json`, `ANALYSIS.json`, an execution receipt, and a SHA-256 file manifest. `verify_output.py` reopens this output and checks the manifest, row count, case uniqueness, installation correctness, comparison order, factorial controls, and pressure-gate decisions.

The optional archive-replay field in `protocol.json` is `null` in the public package because the full archived per-case bundle is much larger than the compact artifact. The frozen private execution replayed 192 earlier cases with zero mismatches; its receipt is preserved in `expected/`.

## Figures

```bash
python artifact/pg_vcr/plot_figures.py
```

The plotting script reads the frozen `expected/SUMMARY.csv` and writes grayscale figures plus the plotted aggregate values to `submission_figures/`.

## Frozen outputs

`expected/` contains compact results from the archived study. These files are immutable reference evidence and are not changed by the runner.
