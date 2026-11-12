#!/usr/bin/env python3
"""Shared framing and hashing helpers for D01 R46."""

from __future__ import annotations

import hashlib
import json
import socket
import struct
import time
from pathlib import Path


MAX_JSON_FRAME = 16 * 1024 * 1024


def canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        block = sock.recv(size - len(data))
        if not block:
            raise EOFError(f"peer closed after {len(data)} of {size} bytes")
        data.extend(block)
    return bytes(data)


def send_json(sock: socket.socket, value: dict) -> int:
    body = canonical(value)
    if len(body) > MAX_JSON_FRAME:
        raise ValueError(f"JSON frame too large: {len(body)}")
    sock.sendall(struct.pack("!I", len(body)) + body)
    return 4 + len(body)


def recv_json(sock: socket.socket) -> tuple[dict, int]:
    size = struct.unpack("!I", recv_exact(sock, 4))[0]
    if size > MAX_JSON_FRAME:
        raise ValueError(f"JSON frame too large: {size}")
    body = recv_exact(sock, size)
    return json.loads(body), 4 + size


def send_blob(sock: socket.socket, payload: bytes, chunk_bytes: int, rate_bytes_per_second: float) -> int:
    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be positive")
    if rate_bytes_per_second <= 0:
        raise ValueError("rate_bytes_per_second must be positive")
    sent = 0
    while sent < len(payload):
        block = payload[sent : sent + chunk_bytes]
        sock.sendall(block)
        sent += len(block)
        time.sleep(len(block) / rate_bytes_per_second)
    return sent


def merge_partials(payloads: list[bytes]) -> list[list[int]]:
    merged: dict[int, list[int]] = {}
    for payload in payloads:
        for key, total, count in json.loads(payload):
            item = merged.setdefault(int(key), [0, 0])
            item[0] += int(total)
            item[1] += int(count)
    return [[key, values[0], values[1]] for key, values in sorted(merged.items())]
