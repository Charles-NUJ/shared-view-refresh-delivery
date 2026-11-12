# Pressure-Gated Version Coalescing for Multi-Site Materialized Aggregates

This repository contains the reproducibility artifact for the paper *Pressure-Gated Version Coalescing for Multi-Site Materialized Aggregates*.

## Problem

A multi-site analysis service refreshes one shared materialized aggregate from source fragments stored at different sites. A site installs version `k` after all source fragments for that version arrive. When cumulative versions overlap on shared links, sending every intermediate object repeats transfer work. Removing every intermediate version can also lose a useful query-visible closure.

PG-VCR uses released information. It follows FIFO while the declared work of every observed version is below one refresh period of bottleneck-link capacity. Once an observed version reaches that load boundary, PG-VCR coalesces covered objects that have not started service and orders retained objects by query impact per positive remaining link work. Every installation still requires a complete same-version manifest.

## Repository layout

- `artifact/pg_vcr/`: controlled PG-VCR study over six-site line, ring, tree, and mesh overlays.
- `artifact/two_version_exact/`: exact heterogeneous two-version line instance used to study intermediate-version selection.
- `artifact/three_host/`: SQLite/TCP three-host line worker and compact result summaries.
- `legacy_joint_calendar/`: earlier fixed-calendar JCR/CRP/RCRP prototype, preserved for provenance. It is separate from the current PG-VCR evidence.

## Environment

The artifact uses Python 3.11 and the packages in `requirements.txt`.

```bash
python -m venv .venv
python -m pip install -r requirements.txt
```

## Controlled PG-VCR study

```bash
python artifact/pg_vcr/run_pressure_gate.py \
  --protocol artifact/pg_vcr/protocol.json \
  --output artifact/pg_vcr/output
python artifact/pg_vcr/verify_output.py
```

The full study contains 1,296 executions from three demand profiles, four key-sharing layouts, four six-site topologies, three link capacities, and nine fixed method variants. The frozen verification record contains 1,296 unique cases, zero incorrect installations, and zero missing final-version installations.

Across the 144 matched contexts, the frozen results record PG-VCR leading, tying, and trailing FIFO-SP in 102, 36, and 6 contexts. PG-VCR leads the QoDA-derived greedy comparator in 100 contexts. The comparison applies the same correctness, freshness, bottleneck-work, byte, and completion order to every method.

Stored reference outputs are under `artifact/pg_vcr/expected/`. A new execution writes a separate output directory.

## Exact two-version instance

```bash
python artifact/two_version_exact/run_two_version_exact.py
python artifact/two_version_exact/verify_two_version_exact.py
```

This finite offline model jointly selects complete destination closures and schedules transfers over heterogeneous fixed-path links. It first maximizes weighted fresh-query time and then minimizes occupied edge time among those optima. The model is an exact small-instance reference, while PG-VCR is the causal online method.

## Three-host line

`artifact/three_host/` contains the coordinator and SQLite/TCP site agent used for the three-site line study. Copy `config.example.json` to a private local file and replace its address and path placeholders. Machine addresses, credentials, and private paths are intentionally absent from this repository.

## Scope

The controlled study assumes positive known link rates, fixed paths, released upward-rounded object sizes, cumulative versions, and dense all-site demand. The exact reference has full knowledge of its finite input. Claims about PG-VCR apply to the causal method and the stated information structure.

## License

No software license has been selected. Public visibility alone does not grant permission to reuse or redistribute the source.
