#!/usr/bin/env python3
"""Coordinator for the three-host SQLite/TCP line experiment."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import sqlite3
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import median

from r46_common import dump_json, recv_exact, recv_json, send_blob, send_json, sha_bytes, sha_file
from r46_site_agent import SessionState


class RemoteEndpoint:
    def __init__(self, site: int, host: str, port: int, timeout: float = 180.0) -> None:
        self.site = site
        self.host = host
        self.port = port
        self.timeout = timeout

    def _connect(self) -> socket.socket:
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        sock.settimeout(self.timeout)
        return sock

    @staticmethod
    def _check(response: dict) -> dict:
        if response.get("state") == "ERROR":
            raise RuntimeError(f"remote {response.get('error_type')}: {response.get('error')}")
        return response

    def rpc(self, request: dict) -> tuple[dict, dict]:
        started = time.perf_counter()
        with self._connect() as sock:
            request_wire = send_json(sock, request)
            response, response_wire = recv_json(sock)
        self._check(response)
        return response, {
            "request_wire_bytes": request_wire,
            "response_wire_bytes": response_wire,
            "elapsed_ms": (time.perf_counter() - started) * 1000.0,
        }

    def fetch(
        self,
        session_id: str,
        version: int,
        chunk_bytes: int,
        rate_bytes_per_second: float,
    ) -> tuple[bytes, dict, dict]:
        request = {
            "op": "fetch",
            "session_id": session_id,
            "version": version,
            "chunk_bytes": chunk_bytes,
            "rate_bytes_per_second": rate_bytes_per_second,
        }
        started = time.perf_counter()
        with self._connect() as sock:
            request_wire = send_json(sock, request)
            response, response_wire = recv_json(sock)
            self._check(response)
            payload = recv_exact(sock, int(response["payload_bytes"]))
        if sha_bytes(payload) != response["payload_sha256"]:
            raise AssertionError("fetched payload digest mismatch")
        return payload, response, {
            "request_wire_bytes": request_wire,
            "response_wire_bytes": response_wire,
            "payload_wire_bytes": len(payload),
            "elapsed_ms": (time.perf_counter() - started) * 1000.0,
        }

    def accept(
        self,
        session_id: str,
        source: int,
        version: int,
        payload: bytes,
        chunk_bytes: int,
        rate_bytes_per_second: float,
    ) -> tuple[dict, dict]:
        request = {
            "op": "accept",
            "session_id": session_id,
            "source": source,
            "version": version,
            "payload_bytes": len(payload),
            "payload_sha256": sha_bytes(payload),
        }
        started = time.perf_counter()
        with self._connect() as sock:
            request_wire = send_json(sock, request)
            send_blob(sock, payload, chunk_bytes, rate_bytes_per_second)
            response, response_wire = recv_json(sock)
        self._check(response)
        return response, {
            "request_wire_bytes": request_wire,
            "response_wire_bytes": response_wire,
            "payload_wire_bytes": len(payload),
            "elapsed_ms": (time.perf_counter() - started) * 1000.0,
        }


def lag_area(releases: dict[int, float], installs: list[dict], end_mono: float) -> float:
    events = [(float(wall), 0, int(version)) for version, wall in releases.items()]
    events.extend((float(item["coordinator_mono"]), 1, int(item["version"])) for item in installs)
    events.sort()
    start = min(releases.values())
    current = start
    latest = 0
    installed = 0
    area = 0.0
    for wall, kind, version in events:
        if wall < start:
            continue
        wall = min(wall, end_mono)
        area += max(0.0, wall - current) * max(0, latest - installed)
        current = wall
        if kind == 0:
            latest = max(latest, version)
        else:
            installed = max(installed, version)
        if wall >= end_mono:
            break
    area += max(0.0, end_mono - current) * max(0, latest - installed)
    return area


class Campaign:
    def __init__(self, config: dict, output: Path) -> None:
        self.config = config
        self.output = output
        self.sites = int(config["workload"]["sites"])
        site_map = {int(item["site"]): item for item in config["sites"]}
        self.remote = {
            site: RemoteEndpoint(site, str(site_map[site]["listen_host"]), int(site_map[site]["listen_port"]))
            for site in (0, 2)
        }
        self.local_root = output / "site1"
        self.local_root.mkdir()

    def ping(self) -> dict:
        evidence = {}
        for site, endpoint in self.remote.items():
            last_error = None
            for _ in range(30):
                try:
                    response, stats = endpoint.rpc({"op": "ping"})
                    evidence[str(site)] = {"response": response, "stats": stats}
                    break
                except Exception as exc:
                    last_error = exc
                    time.sleep(0.5)
            else:
                raise RuntimeError(f"site {site} agent unavailable: {last_error}")
        return evidence

    def run_session(self, output: Path, regime: dict, method: str, seed: int) -> dict:
        output.mkdir(parents=True)
        sites = self.sites
        versions = int(self.config["workload"]["versions"])
        key_count = int(self.config["workload"]["key_count"])
        updates_per_version = int(self.config["workload"]["updates_per_version"])
        chunk_bytes = int(self.config["workload"]["chunk_bytes"])
        period = float(regime["period_seconds"])
        rate = float(regime["rate_bytes_per_second_per_logical_link"])
        session_id = f"{regime['id']}_{method}_{seed}"
        prepare_request = {
            "op": "prepare",
            "session_id": session_id,
            "sites": sites,
            "seed": seed,
            "key_count": key_count,
            "updates_per_version": updates_per_version,
        }
        prepared_remote = []
        local_state = None
        finalized = False
        events: list[dict] = []
        event_lock = threading.Lock()
        installs: list[dict] = []
        install_lock = threading.Lock()
        releases: dict[int, float] = {}
        query_records: list[dict] = []
        worker_errors: list[dict] = []
        error_lock = threading.Lock()

        def record(event: dict) -> None:
            with event_lock:
                events.append({"coordinator_wall_ns": time.time_ns(), "coordinator_mono": time.perf_counter(), **event})

        def record_install(site: int, version: int, ack: dict, source: int) -> None:
            if not ack.get("visible_install"):
                return
            item = {
                "site": site,
                "source": source,
                "version": version,
                "coordinator_mono": time.perf_counter(),
                "result_sha256": ack.get("result_sha256"),
                "query_ms": ack.get("query_ms"),
            }
            with install_lock:
                installs.append(item)
            record({"kind": "visible_install_ack", **item})

        counters = {
            "logical_payload_bytes": 0,
            "link_payload_bytes": 0,
            "metadata_wire_bytes": 0,
            "logical_messages": 0,
            "link_transfers": 0,
            "dropped_waiting_messages": 0,
            "dropped_waiting_bytes": 0,
        }
        counter_lock = threading.Lock()
        queues: list[list[dict]] = [[] for _ in range(sites)]
        conditions = [threading.Condition() for _ in range(sites)]
        edge_locks = [threading.Lock(), threading.Lock()]
        frontier_lock = threading.Lock()
        latest_complete = 0
        producer_done = False
        overlap_gate = threading.Event()

        def add_wire(stats: dict) -> None:
            with counter_lock:
                counters["metadata_wire_bytes"] += int(stats.get("request_wire_bytes", 0))
                counters["metadata_wire_bytes"] += int(stats.get("response_wire_bytes", 0))

        def choose_message(source: int) -> dict:
            with frontier_lock:
                target = latest_complete
            if method == "fifo" or (method == "adaptive" and not overlap_gate.is_set()):
                index = min(
                    range(len(queues[source])),
                    key=lambda idx: (queues[source][idx]["version"], queues[source][idx]["destination"]),
                )
            else:
                index = min(
                    range(len(queues[source])),
                    key=lambda idx: (
                        0 if queues[source][idx]["version"] == target else 1,
                        -queues[source][idx]["version"],
                        queues[source][idx]["destination"],
                    ),
                )
            return queues[source].pop(index)

        def fetch_payload(source: int, version: int) -> tuple[bytes, dict, dict | None]:
            if source == 1:
                payload, meta = local_state.fetch(version)
                return payload, meta, None
            edge = 0 if source == 0 else 1
            with edge_locks[edge]:
                payload, meta, stats = self.remote[source].fetch(session_id, version, chunk_bytes, rate)
            add_wire(stats)
            with counter_lock:
                counters["link_payload_bytes"] += len(payload)
                counters["link_transfers"] += 1
            record(
                {
                    "kind": "link_fetch",
                    "edge": [source, 1],
                    "source": source,
                    "version": version,
                    "payload_bytes": len(payload),
                    "elapsed_ms": stats["elapsed_ms"],
                }
            )
            return payload, meta, stats

        def deliver(destination: int, source: int, version: int, payload: bytes) -> tuple[dict, dict | None]:
            if destination == 1:
                return local_state.accept(source, version, payload, "tcp-relay"), None
            edge = 0 if destination == 0 else 1
            with edge_locks[edge]:
                ack, stats = self.remote[destination].accept(
                    session_id, source, version, payload, chunk_bytes, rate
                )
            add_wire(stats)
            with counter_lock:
                counters["link_payload_bytes"] += len(payload)
                counters["link_transfers"] += 1
            record(
                {
                    "kind": "link_deliver",
                    "edge": [1, destination],
                    "source": source,
                    "destination": destination,
                    "version": version,
                    "payload_bytes": len(payload),
                    "elapsed_ms": stats["elapsed_ms"],
                }
            )
            return ack, stats

        def worker(source: int) -> None:
            nonlocal producer_done
            try:
                while True:
                    with conditions[source]:
                        conditions[source].wait_for(lambda: queues[source] or producer_done)
                        if not queues[source] and producer_done:
                            return
                        message = choose_message(source)
                    started = time.perf_counter()
                    payload, meta, _ = fetch_payload(source, int(message["version"]))
                    if len(payload) != int(message["payload_bytes"]):
                        raise AssertionError("descriptor and fetched payload sizes differ")
                    if sha_bytes(payload) != message["payload_sha256"]:
                        raise AssertionError("descriptor and fetched payload digests differ")
                    ack, _ = deliver(
                        int(message["destination"]), source, int(message["version"]), payload
                    )
                    finished = time.perf_counter()
                    record_install(int(message["destination"]), int(message["version"]), ack, source)
                    with counter_lock:
                        counters["logical_payload_bytes"] += len(payload)
                        counters["logical_messages"] += 1
                    record(
                        {
                            "kind": "logical_transfer",
                            "source": source,
                            "destination": int(message["destination"]),
                            "version": int(message["version"]),
                            "path": [source, 1, int(message["destination"])]
                            if source != 1 and int(message["destination"]) != 1
                            else [source, int(message["destination"])],
                            "payload_bytes": len(payload),
                            "payload_sha256": meta["payload_sha256"],
                            "elapsed_ms": (finished - started) * 1000.0,
                            "visible_install": bool(ack.get("visible_install")),
                        }
                    )
            except Exception as exc:
                with error_lock:
                    worker_errors.append(
                        {
                            "source": source,
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                            "traceback": traceback.format_exc(),
                        }
                    )

        try:
            for site, endpoint in self.remote.items():
                response, stats = endpoint.rpc(prepare_request)
                add_wire(stats)
                prepared_remote.append(site)
                record({"kind": "remote_prepared", "site": site, "response": response})
            local_state = SessionState(
                self.local_root,
                session_id,
                1,
                sites,
                seed,
                key_count,
                updates_per_version,
            )
            worker_threads = [threading.Thread(target=worker, args=(source,), daemon=False) for source in range(sites)]
            for thread in worker_threads:
                thread.start()
            session_start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=sites) as pool:
                for version in range(1, versions + 1):
                    due = session_start + (version - 1) * period
                    delay = due - time.perf_counter()
                    if delay > 0:
                        time.sleep(delay)
                    futures = {
                        0: pool.submit(self.remote[0].rpc, {"op": "build", "session_id": session_id, "version": version}),
                        1: pool.submit(local_state.build, version),
                        2: pool.submit(self.remote[2].rpc, {"op": "build", "session_id": session_id, "version": version}),
                    }
                    built: dict[int, dict] = {}
                    for site, future in futures.items():
                        value = future.result()
                        if site == 1:
                            response = value
                            stats = None
                        else:
                            response, stats = value
                            add_wire(stats)
                        built[site] = response
                        local_accept = response.get("local_accept")
                        if local_accept:
                            record_install(site, version, local_accept, site)
                        query_records.append(
                            {
                                "site": site,
                                "version": version,
                                "sql_ms": float(response["sql_ms"]),
                                "payload_bytes": int(response["payload_bytes"]),
                                "payload_sha256": response["payload_sha256"],
                            }
                        )
                    release_mono = time.perf_counter()
                    releases[version] = release_mono
                    for source in range(sites):
                        descriptor = built[source]
                        with conditions[source]:
                            if method in {"vcr", "adaptive"}:
                                has_replaceable = any(
                                    int(waiting["version"]) < version for waiting in queues[source]
                                )
                                if method == "adaptive" and has_replaceable and not overlap_gate.is_set():
                                    overlap_gate.set()
                                    record(
                                        {
                                            "kind": "overlap_gate_activated",
                                            "source": source,
                                            "version": version,
                                        }
                                    )
                                retained = []
                                for waiting in queues[source]:
                                    if int(waiting["version"]) < version:
                                        with counter_lock:
                                            counters["dropped_waiting_messages"] += 1
                                            counters["dropped_waiting_bytes"] += int(waiting["payload_bytes"])
                                        record(
                                            {
                                                "kind": "drop_waiting",
                                                "source": source,
                                                "destination": int(waiting["destination"]),
                                                "version": int(waiting["version"]),
                                                "replaced_by": version,
                                                "payload_bytes": int(waiting["payload_bytes"]),
                                            }
                                        )
                                    else:
                                        retained.append(waiting)
                                queues[source][:] = retained
                            for destination in range(sites):
                                if destination != source:
                                    queues[source].append(
                                        {
                                            "source": source,
                                            "destination": destination,
                                            "version": version,
                                            "payload_bytes": int(descriptor["payload_bytes"]),
                                            "payload_sha256": descriptor["payload_sha256"],
                                        }
                                    )
                    with frontier_lock:
                        latest_complete = version
                    for condition in conditions:
                        with condition:
                            condition.notify_all()
                    record(
                        {
                            "kind": "version_released",
                            "version": version,
                            "source_sql_ms": [float(built[site]["sql_ms"]) for site in range(sites)],
                        }
                    )
                    if worker_errors:
                        raise RuntimeError(worker_errors)
            producer_done = True
            for condition in conditions:
                with condition:
                    condition.notify_all()
            for thread in worker_threads:
                thread.join(timeout=300)
                if thread.is_alive():
                    raise RuntimeError("transfer worker did not finish")
            if worker_errors:
                raise RuntimeError(worker_errors)

            snapshots = {"1": local_state.snapshot()}
            for site, endpoint in self.remote.items():
                response, stats = endpoint.rpc({"op": "snapshot", "session_id": session_id})
                add_wire(stats)
                snapshots[str(site)] = response
            final_events = [item for item in installs if int(item["version"]) == versions]
            by_site = {int(item["site"]): item for item in final_events}
            if sorted(by_site) != list(range(sites)):
                raise AssertionError({"missing_final_install_ack": sorted(by_site)})
            end_mono = max(float(item["coordinator_mono"]) for item in by_site.values())
            view_hashes = {snapshots[str(site)]["view_sha256"] for site in range(sites)}
            if len(view_hashes) != 1:
                raise AssertionError({"final_view_hashes": sorted(view_hashes)})
            install_by_site = {
                site: sorted(
                    [item for item in installs if int(item["site"]) == site],
                    key=lambda item: float(item["coordinator_mono"]),
                )
                for site in range(sites)
            }
            areas = [lag_area(releases, install_by_site[site], end_mono) for site in range(sites)]
            finalized_sites = {}
            for site, endpoint in self.remote.items():
                response, stats = endpoint.rpc({"op": "finalize", "session_id": session_id})
                add_wire(stats)
                finalized_sites[str(site)] = response
            finalized_sites["1"] = local_state.finalize()
            finalized = True
            ordered_events = sorted(events, key=lambda item: float(item["coordinator_mono"]))
            (output / "EVENTS.jsonl").write_text(
                "".join(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n" for item in ordered_events),
                encoding="utf-8",
            )
            (output / "SOURCE_QUERIES.jsonl").write_text(
                "".join(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n" for item in query_records),
                encoding="utf-8",
            )
            result = {
                "state": "SESSION_FINISHED_AWAITING_LOCAL_ARCHIVE_AUDIT",
                "session_id": session_id,
                "regime": regime["id"],
                "method": method,
                "seed": seed,
                "sites": sites,
                "versions": versions,
                "period_seconds": period,
                "rate_bytes_per_second_per_logical_link": rate,
                "release_span_seconds": releases[versions] - releases[1],
                "completion_seconds_from_first_release": end_mono - releases[1],
                "final_install_latency_seconds": max(end_mono - releases[versions], 0.0),
                "mean_version_lag_area": sum(areas) / len(areas),
                "maximum_version_lag_area": max(areas),
                "visible_installs": len(installs),
                "visible_installs_by_site": {
                    str(site): len(install_by_site[site]) for site in range(sites)
                },
                **counters,
                "overlap_observed": counters["dropped_waiting_messages"] > 0,
                "overlap_gate_activated": overlap_gate.is_set(),
                "median_source_sql_ms": median(item["sql_ms"] for item in query_records),
                "max_source_sql_ms": max(item["sql_ms"] for item in query_records),
                "release_mono": {str(version): wall for version, wall in releases.items()},
                "coordinator_installs": installs,
                "snapshots": snapshots,
                "finalized_sites": finalized_sites,
                "scope": self.config["scope_boundary"],
            }
            dump_json(output / "RESULT.json", result)
            return result
        except Exception as exc:
            dump_json(
                output / "FAILURE.json",
                {
                    "state": "SESSION_FAILED",
                    "session_id": session_id,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                    "worker_errors": worker_errors,
                },
            )
            raise
        finally:
            producer_done = True
            for condition in conditions:
                with condition:
                    condition.notify_all()
            if not finalized:
                for site in prepared_remote:
                    try:
                        self.remote[site].rpc({"op": "finalize", "session_id": session_id})
                    except Exception:
                        pass
                if local_state is not None and not local_state.closed:
                    try:
                        local_state.finalize()
                    except Exception:
                        pass

    def shutdown(self) -> dict:
        evidence = {}
        for site, endpoint in self.remote.items():
            response, stats = endpoint.rpc({"op": "shutdown"})
            evidence[str(site)] = {"response": response, "stats": stats}
        return evidence


def write_manifest(output: Path, command: list[str]) -> None:
    files = []
    for path in sorted(output.rglob("*")):
        if path.is_file() and path.name != "FILE_MANIFEST.json":
            files.append(
                {
                    "path": str(path.relative_to(output)).replace("\\", "/"),
                    "bytes": path.stat().st_size,
                    "sha256": sha_file(path),
                }
            )
    dump_json(output / "FILE_MANIFEST.json", {"state": "LOCAL_COORDINATOR_MANIFEST", "command": command, "files": files})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    config_path = args.config.resolve()
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    prepared = output / "prepared"
    prepared.mkdir()
    sources = [
        Path(__file__).resolve(),
        Path(__file__).resolve().with_name("r46_common.py"),
        Path(__file__).resolve().with_name("r46_site_agent.py"),
        config_path,
    ]
    for source in sources:
        shutil.copyfile(source, prepared / source.name)
    config = json.loads((prepared / config_path.name).read_text(encoding="utf-8"))
    command = [sys.executable, str(Path(__file__).resolve()), "--config", str(config_path), "--output", str(output)]
    if args.smoke:
        command.append("--smoke")
    dump_json(
        output / "PREPARATION.json",
        {
            "state": "PREPARED_BEFORE_RUN",
            "command": command,
            "python": sys.version,
            "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version,
            "pid": os.getpid(),
            "frozen": {
                source.name: sha_file(prepared / source.name) for source in sources
            },
        },
    )
    campaign = Campaign(config, output)
    shutdown_evidence = None
    try:
        ping = campaign.ping()
        dump_json(output / "REMOTE_ENVIRONMENTS.json", ping)
        results = []
        regimes = config["workload"]["regimes"]
        methods = config["workload"]["methods"]
        repetitions = config["workload"]["repetitions"]
        if args.smoke:
            regimes = [next(item for item in regimes if item["id"] == "overlap_pressure")]
            methods = ["adaptive"]
            repetitions = [int(repetitions[0])]
        started = time.perf_counter()
        for regime in regimes:
            for method in methods:
                for seed in repetitions:
                    session_id = f"{regime['id']}_{method}_{seed}"
                    result = campaign.run_session(
                        output / "sessions" / session_id,
                        regime,
                        str(method),
                        int(seed),
                    )
                    results.append(result)
                    print(
                        json.dumps(
                            {
                                "session": session_id,
                                "completion_s": result["completion_seconds_from_first_release"],
                                "link_payload_bytes": result["link_payload_bytes"],
                                "drops": result["dropped_waiting_messages"],
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
        shutdown_evidence = campaign.shutdown()
        dump_json(
            output / "EXECUTION_SUMMARY.json",
            {
                "state": "REMOTE_AND_LOCAL_RUN_FINISHED_AWAITING_DOWNLOAD_AND_AUDIT",
                "smoke": args.smoke,
                "sessions": len(results),
                "elapsed_seconds": time.perf_counter() - started,
                "results": results,
                "shutdown": shutdown_evidence,
                "scope": config["scope_boundary"],
            },
        )
    except Exception as exc:
        if shutdown_evidence is None:
            try:
                shutdown_evidence = campaign.shutdown()
            except Exception as shutdown_exc:
                shutdown_evidence = {
                    "state": "SHUTDOWN_FAILED",
                    "error_type": type(shutdown_exc).__name__,
                    "error": str(shutdown_exc),
                }
        dump_json(
            output / "CAMPAIGN_FAILURE.json",
            {
                "state": "CAMPAIGN_FAILED",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "shutdown": shutdown_evidence,
            },
        )
        raise
    finally:
        write_manifest(output, command)


if __name__ == "__main__":
    main()
