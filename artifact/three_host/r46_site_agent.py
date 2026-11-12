#!/usr/bin/env python3
"""SQLite source and consumer agent for the three-host line experiment."""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import re
import shutil
import socket
import sqlite3
import sys
import threading
import time
import traceback
from pathlib import Path

from r46_common import (
    canonical,
    dump_json,
    merge_partials,
    recv_exact,
    recv_json,
    send_blob,
    send_json,
    sha_bytes,
    sha_file,
)


SESSION_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class SessionState:
    def __init__(
        self,
        root: Path,
        session_id: str,
        site: int,
        sites: int,
        seed: int,
        key_count: int,
        updates_per_version: int,
    ) -> None:
        if not SESSION_RE.fullmatch(session_id):
            raise ValueError(f"invalid session id: {session_id!r}")
        self.session_id = session_id
        self.site = site
        self.sites = sites
        self.seed = seed
        self.key_count = key_count
        self.updates_per_version = updates_per_version
        self.root = root / "sessions" / session_id
        self.root.mkdir(parents=True, exist_ok=False)
        self.event_path = self.root / "SITE_EVENTS.jsonl"
        self.event_lock = threading.Lock()
        self.source_lock = threading.Lock()
        self.consumer_lock = threading.Lock()
        self.closed = False

        self.source_path = self.root / f"source_{site}.sqlite"
        self.consumer_path = self.root / f"consumer_{site}.sqlite"
        self.source = sqlite3.connect(self.source_path, check_same_thread=False)
        self.consumer = sqlite3.connect(self.consumer_path, check_same_thread=False)
        self.source.execute("PRAGMA journal_mode=WAL")
        self.consumer.execute("PRAGMA journal_mode=WAL")
        self.source.executescript(
            """
            CREATE TABLE fact(key_id INTEGER NOT NULL,value INTEGER NOT NULL,version INTEGER NOT NULL);
            CREATE TABLE partial(
              version INTEGER PRIMARY KEY,
              payload BLOB NOT NULL,
              payload_sha256 TEXT NOT NULL,
              payload_bytes INTEGER NOT NULL,
              sql_ms REAL NOT NULL
            );
            """
        )
        rng = random.Random(seed * 1009 + site * 9176)
        rows = [(key, 1 + rng.randrange(100), 0) for key in range(key_count)]
        with self.source:
            self.source.executemany("INSERT INTO fact VALUES(?,?,?)", rows)

        self.consumer.executescript(
            """
            CREATE TABLE contribution(
              version INTEGER NOT NULL,
              source INTEGER NOT NULL,
              payload BLOB NOT NULL,
              payload_sha256 TEXT NOT NULL,
              PRIMARY KEY(version,source)
            );
            CREATE TABLE view_rows(
              key_id INTEGER PRIMARY KEY,
              total_value INTEGER NOT NULL,
              row_count INTEGER NOT NULL
            );
            CREATE TABLE history(
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              version INTEGER NOT NULL,
              install_wall_ns INTEGER NOT NULL,
              install_mono_ns INTEGER NOT NULL,
              result_sha256 TEXT NOT NULL,
              result_rows INTEGER NOT NULL,
              query_ms REAL NOT NULL
            );
            CREATE TABLE state(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO state VALUES('installed_version','0');
            """
        )
        self.consumer.commit()
        dump_json(
            self.root / "SESSION_PREPARATION.json",
            {
                "state": "PREPARED_BEFORE_SESSION",
                "session_id": session_id,
                "site": site,
                "sites": sites,
                "seed": seed,
                "key_count": key_count,
                "updates_per_version": updates_per_version,
                "python": sys.version,
                "platform": platform.platform(),
                "sqlite": sqlite3.sqlite_version,
            },
        )
        self.record({"kind": "session_prepared"})

    def record(self, event: dict) -> None:
        value = {
            "session_id": self.session_id,
            "site": self.site,
            "wall_ns": time.time_ns(),
            "mono_ns": time.perf_counter_ns(),
            **event,
        }
        line = json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n"
        with self.event_lock:
            with self.event_path.open("a", encoding="utf-8") as handle:
                handle.write(line)

    def accept(self, source: int, version: int, payload: bytes, path: str) -> dict:
        digest = sha_bytes(payload)
        visible_install = False
        result_hash = None
        result_rows = None
        query_ms = None
        install_wall_ns = None
        install_mono_ns = None
        with self.consumer_lock:
            with self.consumer:
                self.consumer.execute(
                    "INSERT OR REPLACE INTO contribution(version,source,payload,payload_sha256) VALUES(?,?,?,?)",
                    (version, source, payload, digest),
                )
            count = int(
                self.consumer.execute(
                    "SELECT COUNT(*) FROM contribution WHERE version=?", (version,)
                ).fetchone()[0]
            )
            installed = int(
                self.consumer.execute(
                    "SELECT value FROM state WHERE key='installed_version'"
                ).fetchone()[0]
            )
            if count == self.sites and version > installed:
                pieces = [
                    row[0]
                    for row in self.consumer.execute(
                        "SELECT payload FROM contribution WHERE version=? ORDER BY source", (version,)
                    ).fetchall()
                ]
                rows = merge_partials(pieces)
                result_blob = canonical(rows)
                result_hash = sha_bytes(result_blob)
                install_wall_ns = time.time_ns()
                install_mono_ns = time.perf_counter_ns()
                query_started = time.perf_counter()
                with self.consumer:
                    self.consumer.execute("DELETE FROM view_rows")
                    self.consumer.executemany("INSERT INTO view_rows VALUES(?,?,?)", rows)
                    self.consumer.execute(
                        "INSERT OR REPLACE INTO state(key,value) VALUES('installed_version',?)", (str(version),)
                    )
                _ = self.consumer.execute("SELECT SUM(total_value),SUM(row_count) FROM view_rows").fetchone()
                query_ms = (time.perf_counter() - query_started) * 1000.0
                with self.consumer:
                    self.consumer.execute(
                        "INSERT INTO history(version,install_wall_ns,install_mono_ns,result_sha256,result_rows,query_ms) "
                        "VALUES(?,?,?,?,?,?)",
                        (version, install_wall_ns, install_mono_ns, result_hash, len(rows), query_ms),
                    )
                installed = version
                result_rows = len(rows)
                visible_install = True
        event = {
            "kind": "consumer_accept",
            "source": source,
            "version": version,
            "path": path,
            "payload_bytes": len(payload),
            "payload_sha256": digest,
            "visible_install": visible_install,
            "installed_version": installed,
            "install_wall_ns": install_wall_ns,
            "install_mono_ns": install_mono_ns,
            "result_sha256": result_hash,
            "result_rows": result_rows,
            "query_ms": query_ms,
        }
        self.record(event)
        return event

    def build(self, version: int) -> dict:
        with self.source_lock:
            existing = self.source.execute("SELECT 1 FROM partial WHERE version=?", (version,)).fetchone()
            if existing:
                raise ValueError(f"version already built: {version}")
            rng = random.Random(self.seed * 1000003 + self.site * 10007 + version * 101)
            updates = [
                (rng.randrange(self.key_count), 1 + rng.randrange(100), version)
                for _ in range(self.updates_per_version)
            ]
            started = time.perf_counter()
            with self.source:
                self.source.executemany("INSERT INTO fact VALUES(?,?,?)", updates)
            rows = [
                [int(key), int(total), int(count)]
                for key, total, count in self.source.execute(
                    "SELECT key_id,SUM(value),COUNT(*) FROM fact GROUP BY key_id ORDER BY key_id"
                ).fetchall()
            ]
            payload = canonical(rows)
            sql_ms = (time.perf_counter() - started) * 1000.0
            digest = sha_bytes(payload)
            with self.source:
                self.source.execute(
                    "INSERT INTO partial VALUES(?,?,?,?,?)",
                    (version, payload, digest, len(payload), sql_ms),
                )
        local_accept = self.accept(self.site, version, payload, "local")
        result = {
            "kind": "source_build",
            "version": version,
            "payload_bytes": len(payload),
            "payload_sha256": digest,
            "sql_ms": sql_ms,
            "local_accept": local_accept,
        }
        self.record(result)
        return result

    def fetch(self, version: int) -> tuple[bytes, dict]:
        with self.source_lock:
            row = self.source.execute(
                "SELECT payload,payload_sha256,payload_bytes,sql_ms FROM partial WHERE version=?", (version,)
            ).fetchone()
        if row is None:
            raise KeyError(f"missing partial for version {version}")
        payload = bytes(row[0])
        meta = {
            "version": version,
            "payload_sha256": str(row[1]),
            "payload_bytes": int(row[2]),
            "sql_ms": float(row[3]),
        }
        if len(payload) != meta["payload_bytes"] or sha_bytes(payload) != meta["payload_sha256"]:
            raise AssertionError("stored partial failed digest or length check")
        return payload, meta

    def snapshot(self) -> dict:
        with self.consumer_lock:
            installed = int(
                self.consumer.execute(
                    "SELECT value FROM state WHERE key='installed_version'"
                ).fetchone()[0]
            )
            histories = [
                {
                    "version": int(version),
                    "install_wall_ns": int(wall),
                    "install_mono_ns": int(mono),
                    "result_sha256": digest,
                    "result_rows": int(rows),
                    "query_ms": float(query_ms),
                }
                for version, wall, mono, digest, rows, query_ms in self.consumer.execute(
                    "SELECT version,install_wall_ns,install_mono_ns,result_sha256,result_rows,query_ms "
                    "FROM history ORDER BY seq"
                ).fetchall()
            ]
            view_rows = [
                [int(key), int(total), int(count)]
                for key, total, count in self.consumer.execute(
                    "SELECT key_id,total_value,row_count FROM view_rows ORDER BY key_id"
                ).fetchall()
            ]
        with self.source_lock:
            partials = [
                {
                    "version": int(version),
                    "payload_sha256": digest,
                    "payload_bytes": int(size),
                    "sql_ms": float(sql_ms),
                }
                for version, digest, size, sql_ms in self.source.execute(
                    "SELECT version,payload_sha256,payload_bytes,sql_ms FROM partial ORDER BY version"
                ).fetchall()
            ]
        return {
            "session_id": self.session_id,
            "site": self.site,
            "installed_version": installed,
            "history": histories,
            "view_sha256": sha_bytes(canonical(view_rows)),
            "view_rows": len(view_rows),
            "partials": partials,
        }

    def finalize(self) -> dict:
        if self.closed:
            raise RuntimeError("session already finalized")
        result = self.snapshot()
        result["state"] = "SITE_SESSION_FINISHED_AWAITING_LOCAL_ARCHIVE_AUDIT"
        self.source.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.consumer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.source.close()
        self.consumer.close()
        self.closed = True
        dump_json(self.root / "SITE_RESULT.json", result)
        return result


class Agent:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.root = args.root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.sessions: dict[str, SessionState] = {}
        self.sessions_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.handlers: list[threading.Thread] = []
        self.global_event_path = self.root / "AGENT_EVENTS.jsonl"
        self.global_lock = threading.Lock()
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((args.host, args.port))
        self.server.listen(32)
        self.server.settimeout(0.5)

    def global_record(self, event: dict) -> None:
        value = {
            "site": self.args.site,
            "wall_ns": time.time_ns(),
            "mono_ns": time.perf_counter_ns(),
            **event,
        }
        with self.global_lock:
            with self.global_event_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")

    def get_session(self, session_id: str) -> SessionState:
        with self.sessions_lock:
            if session_id not in self.sessions:
                raise KeyError(f"unknown session: {session_id}")
            return self.sessions[session_id]

    def dispatch(self, request: dict, conn: socket.socket) -> None:
        op = request.get("op")
        if op == "ping":
            send_json(
                conn,
                {
                    "state": "AGENT_READY",
                    "site": self.args.site,
                    "python": sys.version,
                    "platform": platform.platform(),
                    "sqlite": sqlite3.sqlite_version,
                    "pid": os.getpid(),
                },
            )
            return
        if op == "prepare":
            session_id = str(request["session_id"])
            with self.sessions_lock:
                if session_id in self.sessions:
                    raise ValueError(f"active session already exists: {session_id}")
                state = SessionState(
                    self.root,
                    session_id,
                    self.args.site,
                    int(request["sites"]),
                    int(request["seed"]),
                    int(request["key_count"]),
                    int(request["updates_per_version"]),
                )
                self.sessions[session_id] = state
            send_json(conn, {"state": "SESSION_PREPARED", "session_id": session_id, "site": self.args.site})
            return
        session_id = str(request.get("session_id", ""))
        state = self.get_session(session_id)
        if op == "build":
            result = state.build(int(request["version"]))
            send_json(conn, {"state": "BUILD_FINISHED", **result})
            return
        if op == "fetch":
            payload, meta = state.fetch(int(request["version"]))
            send_json(conn, {"state": "BLOB_FOLLOWS", **meta})
            send_blob(
                conn,
                payload,
                int(request["chunk_bytes"]),
                float(request["rate_bytes_per_second"]),
            )
            state.record({"kind": "fetch_sent", **meta})
            return
        if op == "accept":
            size = int(request["payload_bytes"])
            payload = recv_exact(conn, size)
            digest = sha_bytes(payload)
            if digest != request["payload_sha256"]:
                raise AssertionError("received payload digest mismatch")
            result = state.accept(int(request["source"]), int(request["version"]), payload, "tcp")
            send_json(conn, {"state": "ACCEPT_FINISHED", **result})
            return
        if op == "snapshot":
            send_json(conn, {"state": "SNAPSHOT", **state.snapshot()})
            return
        if op == "finalize":
            result = state.finalize()
            with self.sessions_lock:
                del self.sessions[session_id]
            send_json(conn, result)
            return
        raise ValueError(f"unknown operation: {op!r}")

    def handle(self, conn: socket.socket, address: tuple[str, int]) -> None:
        peer = address[0]
        try:
            conn.settimeout(self.args.timeout)
            if self.args.expected_peer and peer != self.args.expected_peer:
                send_json(conn, {"state": "PEER_REJECTED", "peer": peer})
                self.global_record({"kind": "peer_rejected", "peer": peer})
                return
            request, wire_bytes = recv_json(conn)
            self.global_record({"kind": "request", "peer": peer, "op": request.get("op"), "wire_bytes": wire_bytes})
            if request.get("op") == "shutdown":
                with self.sessions_lock:
                    if self.sessions:
                        raise RuntimeError(f"active sessions remain: {sorted(self.sessions)}")
                send_json(conn, {"state": "AGENT_SHUTTING_DOWN", "site": self.args.site})
                self.stop_event.set()
                return
            self.dispatch(request, conn)
        except Exception as exc:  # preserve failures in both client reply and server record
            self.global_record(
                {
                    "kind": "handler_error",
                    "peer": peer,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
            try:
                send_json(conn, {"state": "ERROR", "error_type": type(exc).__name__, "error": str(exc)})
            except Exception:
                pass
        finally:
            conn.close()

    def run(self) -> None:
        ready = {
            "state": "AGENT_READY",
            "site": self.args.site,
            "host": self.args.host,
            "port": self.args.port,
            "expected_peer": self.args.expected_peer,
            "root": str(self.root),
        }
        print(json.dumps(ready, sort_keys=True), flush=True)
        self.global_record({"kind": "agent_ready", **ready})
        while not self.stop_event.is_set():
            try:
                conn, address = self.server.accept()
            except socket.timeout:
                continue
            thread = threading.Thread(target=self.handle, args=(conn, address), daemon=False)
            thread.start()
            self.handlers.append(thread)
        self.server.close()
        for thread in self.handlers:
            if thread is not threading.current_thread():
                thread.join(timeout=30)
        self.global_record({"kind": "agent_stopped"})


def write_manifest(root: Path) -> None:
    files = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "FILE_MANIFEST.json":
            files.append(
                {
                    "path": str(path.relative_to(root)).replace("\\", "/"),
                    "bytes": path.stat().st_size,
                    "sha256": sha_file(path),
                }
            )
    dump_json(root / "FILE_MANIFEST.json", {"state": "REMOTE_AGENT_MANIFEST", "files": files})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", type=int, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--expected-peer", required=True)
    parser.add_argument("--timeout", type=float, default=180.0)
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    prepared = root / "prepared"
    prepared.mkdir(exist_ok=True)
    for source in [Path(__file__).resolve(), Path(__file__).resolve().with_name("r46_common.py")]:
        shutil.copyfile(source, prepared / source.name)
    dump_json(
        root / "SERVICE_PREPARATION.json",
        {
            "state": "PREPARED_BEFORE_AGENT",
            "site": args.site,
            "host": args.host,
            "port": args.port,
            "expected_peer": args.expected_peer,
            "python": sys.version,
            "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version,
            "pid": os.getpid(),
            "frozen": {path.name: sha_file(path) for path in prepared.iterdir() if path.is_file()},
        },
    )
    agent = Agent(args)
    try:
        agent.run()
    finally:
        write_manifest(root)


if __name__ == "__main__":
    main()
