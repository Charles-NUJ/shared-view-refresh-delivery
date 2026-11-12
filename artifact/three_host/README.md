# Three-Host SQLite/TCP Line

This directory contains the coordinator and site agent for the three-host line study.

Each site stores source rows in SQLite, computes a versioned partial aggregate, and sends the encoded object over TCP. A consumer installs a version after receiving the complete same-version manifest. The coordinator records SQL time, payload bytes, link bytes, installation history, gate events, and waiting-object replacement.

## Deployment

1. Copy `config.example.json` to a private local file.
2. Fill its host addresses, ports, and output directories.
3. Start `r46_site_agent.py` at each endpoint.
4. Start `r46_coordinator.py` on the middle host.
5. Copy every remote output directory back to local storage.
6. Verify the local archive before removing any remote copy.

Example commands:

```bash
python r46_site_agent.py --site SITE_ID --root SITE_OUTPUT \
  --port PORT --expected-peer COORDINATOR_ADDRESS
python r46_coordinator.py --config config.local.json --output LOCAL_OUTPUT
```

`overlap_summary.csv` contains the earlier low-pressure and overlap-pressure comparison. `conservative_clear_summary.csv` records the three 1.50-second-period executions in which the gate stayed inactive and the adaptive service key therefore remained FIFO. Addresses, credentials, and private paths are excluded.
