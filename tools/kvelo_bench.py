#!/usr/bin/env python3
"""Small, dependency-free load client for the Kvelo TCP protocol."""

from __future__ import annotations

import argparse
import concurrent.futures
import random
import socket
import statistics
import threading
import time
from dataclasses import dataclass, field


@dataclass
class Result:
    latencies_ns: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    completed: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0


class ProtocolError(Exception):
    pass


class KveloConnection:
    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.received = bytearray()

    def close(self) -> None:
        self.sock.close()

    def send(self, requests: list[bytes]) -> int:
        payload = b"".join(requests)
        self.sock.sendall(payload)
        return len(payload)

    def receive(self) -> tuple[str, int]:
        header = self._read_line()
        received = len(header) + 1

        if header.startswith(b"V "):
            try:
                length = int(header[2:])
            except ValueError as error:
                raise ProtocolError(f"invalid V header: {header!r}") from error
            self._read_exact(length)
            return "V", received + length

        responses = {
            b"S": "S",
            b"D": "D",
            b"N": "N",
            b"B": "B",
        }
        try:
            return responses[header], received
        except KeyError as error:
            raise ProtocolError(f"unknown response: {header!r}") from error

    def _read_line(self) -> bytes:
        while True:
            index = self.received.find(b"\n")
            if index >= 0:
                line = bytes(self.received[:index])
                del self.received[: index + 1]
                return line
            self._receive_more()

    def _read_exact(self, length: int) -> bytes:
        while len(self.received) < length:
            self._receive_more()
        value = bytes(self.received[:length])
        del self.received[:length]
        return value

    def _receive_more(self) -> None:
        chunk = self.sock.recv(64 * 1024)
        if not chunk:
            raise ConnectionError("server closed the connection")
        self.received.extend(chunk)


def set_request(key: bytes, value: bytes, ttl: int | None) -> bytes:
    ttl_part = b"" if ttl is None else f" {ttl}".encode()
    return b"S %d %d%s\n" % (len(key), len(value), ttl_part) + key + value


def get_request(key: bytes) -> bytes:
    return b"G %d\n" % len(key) + key


def percentile(values: list[int], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * fraction))
    return ordered[index] / 1_000_000


def worker(worker_id: int, args: argparse.Namespace, barrier: threading.Barrier) -> Result:
    result = Result()
    random_source = random.Random(args.seed + worker_id)
    value = bytes([worker_id % 256]) * args.value_size
    remaining = args.requests // args.connections
    if worker_id < args.requests % args.connections:
        remaining += 1

    try:
        connection = KveloConnection(args.host, args.port, args.timeout)
    except OSError as error:
        result.errors.append(f"connect: {error}")
        barrier.wait()
        return result

    try:
        barrier.wait()
        while remaining:
            batch_size = min(args.pipeline, remaining)
            requests: list[bytes] = []
            expected: list[set[str]] = []

            for _ in range(batch_size):
                key = f"kvelo:{random_source.randrange(args.keys)}".encode()
                use_get = args.workload == "get" or (
                    args.workload == "mixed" and random_source.random() < args.get_ratio
                )
                if use_get:
                    requests.append(get_request(key))
                    expected.append({"V", "N"})
                else:
                    requests.append(set_request(key, value, args.ttl))
                    expected.append({"S"})

            started = time.perf_counter_ns()
            result.bytes_sent += connection.send(requests)
            for allowed in expected:
                response, received = connection.receive()
                result.bytes_received += received
                if response not in allowed:
                    raise ProtocolError(f"expected {sorted(allowed)}, received {response}")
            elapsed = time.perf_counter_ns() - started

            # A pipeline has one observable round-trip, so attribute its average
            # latency to each command rather than pretending individual timings.
            result.latencies_ns.extend([elapsed // batch_size] * batch_size)
            result.completed += batch_size
            remaining -= batch_size
    except (OSError, ProtocolError) as error:
        result.errors.append(str(error))
    finally:
        connection.close()

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8356)
    parser.add_argument("-n", "--requests", type=int, default=100_000)
    parser.add_argument("-c", "--connections", type=int, default=16)
    parser.add_argument("-p", "--pipeline", type=int, default=1)
    parser.add_argument("--workload", choices=("get", "set", "mixed"), default="mixed")
    parser.add_argument("--get-ratio", type=float, default=0.8)
    parser.add_argument("--keys", type=int, default=10_000)
    parser.add_argument("--value-size", type=int, default=128)
    parser.add_argument("--ttl", type=int)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()

    if args.requests < 1 or args.connections < 1 or args.pipeline < 1 or args.keys < 1:
        parser.error("requests, connections, pipeline, and keys must be positive")
    if args.value_size < 0:
        parser.error("value-size must not be negative")
    if not 0.0 <= args.get_ratio <= 1.0:
        parser.error("get-ratio must be between 0 and 1")
    if args.ttl is not None and args.ttl < 0:
        parser.error("ttl must not be negative")
    return args


def main() -> int:
    args = parse_args()
    barrier = threading.Barrier(args.connections + 1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.connections) as executor:
        futures = [executor.submit(worker, index, args, barrier) for index in range(args.connections)]
        barrier.wait()
        started = time.perf_counter()
        results = [future.result() for future in futures]
        elapsed = time.perf_counter() - started

    completed = sum(result.completed for result in results)
    latencies = [value for result in results for value in result.latencies_ns]
    errors = [error for result in results for error in result.errors]
    sent = sum(result.bytes_sent for result in results)
    received = sum(result.bytes_received for result in results)

    print(f"completed:   {completed}/{args.requests}")
    print(f"duration:    {elapsed:.3f} s")
    print(f"throughput:  {completed / elapsed:,.0f} requests/s")
    print(f"latency avg: {statistics.fmean(latencies) / 1_000_000:.3f} ms" if latencies else "latency avg: n/a")
    print(f"latency p50: {percentile(latencies, 0.50):.3f} ms")
    print(f"latency p95: {percentile(latencies, 0.95):.3f} ms")
    print(f"latency p99: {percentile(latencies, 0.99):.3f} ms")
    print(f"network:     {sent:,} B sent, {received:,} B received")
    print(f"errors:      {len(errors)}")
    for error in errors[:10]:
        print(f"  - {error}")

    return 1 if errors or completed != args.requests else 0


if __name__ == "__main__":
    raise SystemExit(main())
