# Shared View Refresh and Delivery

This repository contains code and experiment contracts for joint reduction of
refresh and delivery work for shared materialized views. It studies a feasible,
unissued calendar with fixed capture epochs and command starts. Decisions remove
recipient copies and the source work that becomes unnecessary while preserving
declared freshness budgets and reserved resource intervals.

* **JCR** formulates the reduction as a mixed-integer optimization problem.
* **CRP** performs sequential feasible deletions.
* **RCRP** performs a one-step rollout over CRP and retains the CRP solution as a
  candidate. Its cost comparison is with this base policy in the stated domain.

The guarantees depend on the assumptions in the model, including valid resource
bounds and the stated installation conditions. They do not establish global
optimality over new capture times or command starts, or a production latency SLA.

## Requirements

The included experiment workers were prepared for Python 3.11, NumPy 1.24.3,
and SciPy 1.11.1 with the HiGHS solver bundled in SciPy. Install dependencies in
a virtual environment:

```bash
python -m venv .venv
# Activate .venv using the command for your operating system.
python -m pip install -r requirements.txt
```

## Run experiments

```bash
python reproduce.py verify
python reproduce.py tiny
python reproduce.py rollout
python reproduce.py jcr
python reproduce.py heldout
python reproduce.py controlled
python reproduce.py feedback
python reproduce.py transport
python audit.py jcr
python audit.py transport
```

Every command creates a fresh directory under `outputs/`. Use `--output PATH`
to select a different, nonexistent output directory. For suites other than
`feedback`, use `--limit N` for a smoke check on the first N cases; this does
not reproduce the full contract. The feedback worker requires its full contract
for boundary checks.
The runner invokes a copied, frozen worker with paths set for the local output;
the original worker scripts contain their historical workspace paths and should
be run through `reproduce.py`.

Each output contains the worker's results and a `REPRODUCTION_RECEIPT.json`
comparing selected objective values, status, or payload counts with stored
summaries. Runtime, exact solver tie choices, and physical timing are not
compared. The separate audit scripts reconstruct JCR model rows and SQL/SQLite
results for the respective completed full runs. A mismatch or an interrupted
worker requires investigation; a passing comparison does not replace
mathematical proof or an independent-host study.

| Suite | Contents |
| --- | --- |
| `tiny` | Enumeration and JCR comparison on 18 small constructed contracts. |
| `jcr` | Fixed-calendar JCR and CRP cases, plus weak-scale contracts. |
| `rollout` | RCRP and CRP on 12 design cases and 18 previously generated small contracts. |
| `heldout` | Same-domain comparisons on those 18 small contracts. |
| `controlled` | One-factor-at-a-time comparisons on 192 contracts. |
| `feedback` | Constructed execution-feedback and bounded-source-cost cases. |
| `transport` | Local SQL, loopback TCP, and SQLite installation sessions using synthetic input. |

The same-domain comparators include adaptations made for this problem. They are
not presented as reproductions of iShare, BIGSHARED, QoDA, Tempura, or Agamotto.
The small held-out contracts were generated previously; they are not independent
production workloads. The transport suite uses two local processes with logical
time labels and does not measure an interhost or production freshness guarantee.

## Artifact scope and provenance

`experiments/*/prepared/` contains the frozen inputs and workers used for the
listed experiments. `checks/` contains separate audit code.
`inputs/cases/` contains the synthetic SQL input required
by the local transport experiment. `SOURCE_MANIFEST.json` records SHA-256 hashes
of these copied files, and `experiments/*/expected.json` contains selected
summaries from the original results for comparison.

This is an initial code release. It does not yet contain every experiment,
full raw output, third-party dataset, or manuscript artifact. Results should be
interpreted within the described experimental contracts. See the paper for the
precise model and assumptions.

## License

A license has not yet been selected by the authors. Public visibility does not
itself grant an open-source license.
