"""A trimmed, synchronous stand-in for nanocached-node, speaking just
enough of the wire protocol — ``A``, namespaced ``g``/``s``/``d`` (issue
#105) with their legacy ``G``/``S``/``D`` counterparts, ``c``/``F``
(issue #106), ``i`` (issue #129), ``m``/``o`` (issue #152), and the
``A``/``P``-conditioned forms of ``k`` (issue #141/#414 — the
digest-conditioned form isn't exercised by these adapter tests and isn't
implemented here) — for these adapter tests to exercise NanocachedCache
over a real TCP socket without the Rust binary.
A trimmed re-implementation of
``sdk/python/tests/mock_servers.py``'s ``MockNode`` (that module is
private to the SDK's own test suite, see the shared adapters spec) built
on ``socketserver`` instead of asyncio: this backend's sync/async bridge
already runs the SDK client on its own event-loop thread, so the mock
itself has no need to be asyncio-based — plain blocking sockets on a
thread per connection are simpler here.

No response tags (``T``) support: this adapter's SDK client is never
configured with anything that would request them, so the untagged wire
form is all these tests need.
"""

from __future__ import annotations

import socketserver
import threading


class MockNode:
    def __init__(self, required_secret: bytes | None = None) -> None:
        self.store: dict[bytes, bytes] = {}
        # Namespaced entries (issue #105) live here instead, keyed by
        # (namespace, key) — mirrors the SDK mock's own separation, so a
        # namespaced key never collides with a same-named default-
        # namespace one.
        self.ns_store: dict[tuple[bytes, bytes], bytes] = {}
        # Per-entry TTL as last written on the wire, keyed the same way
        # as the stores above — lets a test assert exactly what TTL a
        # given key reached the mock with (issue #108 spec: "Record
        # per-entry TTL so tests can assert what reached the wire").
        self.ttls: dict[tuple[bytes, bytes], int] = {}
        self.required_secret = required_secret

        self.connection_count = 0
        self.get_count = 0
        self.set_count = 0
        self.delete_count = 0
        self.clear_count = 0
        self.flush_count = 0
        self.incr_count = 0
        self.multi_get_count = 0
        self.multi_set_count = 0
        self.cas_set_count = 0

        self._lock = threading.Lock()
        self._server: socketserver.ThreadingTCPServer | None = None
        self._thread: threading.Thread | None = None
        self.port = 0

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def start(self) -> "MockNode":
        node = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                node._serve(self.request)

        server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        self._server = server
        self.port = server.server_address[1]
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def close(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    # ── storage helpers ──────────────────────────────────────────────
    # A namespace-length of 0 (the default namespace) is routed to
    # `store` — the same entries a legacy G/S/D sees — exactly like the
    # real server's "g 0 ... == G ..." rule; a non-empty namespace goes
    # to `ns_store`, keyed by (namespace, key).

    def _get_entry(self, namespace: bytes, key: bytes) -> bytes | None:
        with self._lock:
            return self.store.get(key) if not namespace else self.ns_store.get((namespace, key))

    def _set_entry(self, namespace: bytes, key: bytes, value: bytes, ttl: int) -> None:
        with self._lock:
            self.ttls[(namespace, key)] = ttl
            if namespace:
                self.ns_store[(namespace, key)] = value
            else:
                self.store[key] = value

    def _delete_entry(self, namespace: bytes, key: bytes) -> bool:
        with self._lock:
            self.ttls.pop((namespace, key), None)
            if namespace:
                return self.ns_store.pop((namespace, key), None) is not None
            return self.store.pop(key, None) is not None

    def _clear_namespace(self, namespace: bytes) -> None:
        with self._lock:
            if namespace:
                for entry_key in [k for k in self.ns_store if k[0] == namespace]:
                    del self.ns_store[entry_key]
                    self.ttls.pop(entry_key, None)
            else:
                for key in list(self.store):
                    del self.store[key]
                    self.ttls.pop((b"", key), None)

    def _flush(self) -> None:
        with self._lock:
            self.store.clear()
            self.ns_store.clear()
            self.ttls.clear()

    # ── the wire protocol ────────────────────────────────────────────

    def _serve(self, sock) -> None:
        self.connection_count += 1
        rfile = sock.makefile("rb")
        wfile = sock.makefile("wb")
        try:
            while True:
                header = rfile.readline()
                if not header or not header.endswith(b"\n"):
                    return
                parts = header[:-1].split(b" ")
                cmd = parts[0]

                if cmd == b"A":
                    secret = rfile.read(int(parts[1]))
                    accepted = (
                        len(secret) > 0
                        if self.required_secret is None
                        else secret == self.required_secret
                    )
                    wfile.write(b"On\n" if accepted else b"En\n")
                    wfile.flush()
                    if not accepted:
                        return

                elif cmd in (b"G", b"g"):
                    if cmd == b"g":
                        namespace = rfile.read(int(parts[1]))
                        key = rfile.read(int(parts[2]))
                    else:
                        namespace = b""
                        key = rfile.read(int(parts[1]))
                    self.get_count += 1
                    value = self._get_entry(namespace, key)
                    if value is not None:
                        wfile.write(b"V %d\n%b" % (len(value), value))
                    else:
                        wfile.write(b"N\n")
                    wfile.flush()

                elif cmd in (b"S", b"s"):
                    if cmd == b"s":
                        namespace = rfile.read(int(parts[1]))
                        key = rfile.read(int(parts[2]))
                        value = rfile.read(int(parts[3]))
                        ttl = int(parts[4]) if len(parts) > 4 else 0
                    else:
                        namespace = b""
                        key = rfile.read(int(parts[1]))
                        value = rfile.read(int(parts[2]))
                        ttl = int(parts[3]) if len(parts) > 3 else 0
                    self.set_count += 1
                    self._set_entry(namespace, key, value, ttl)
                    wfile.write(b"S\n")
                    wfile.flush()

                elif cmd in (b"D", b"d"):
                    if cmd == b"d":
                        namespace = rfile.read(int(parts[1]))
                        key = rfile.read(int(parts[2]))
                    else:
                        namespace = b""
                        key = rfile.read(int(parts[1]))
                    self.delete_count += 1
                    deleted = self._delete_entry(namespace, key)
                    wfile.write((b"D" if deleted else b"N") + b"\n")
                    wfile.flush()

                elif cmd == b"c":
                    # Clear (issue #106): always carries a namespace-length
                    # header field, even 0 for the default namespace — no
                    # legacy uppercase form.
                    namespace = rfile.read(int(parts[1]))
                    self.clear_count += 1
                    self._clear_namespace(namespace)
                    wfile.write(b"C\n")
                    wfile.flush()

                elif cmd == b"F":
                    self.flush_count += 1
                    self._flush()
                    wfile.write(b"C\n")
                    wfile.flush()

                elif cmd == b"i":
                    # Increment/decrement (issue #129): always carries a
                    # namespace-length header field, even 0 for the
                    # default namespace — no legacy uppercase form, same
                    # as `c`. Untagged only, per this module's docstring.
                    namespace = rfile.read(int(parts[1]))
                    key = rfile.read(int(parts[2]))
                    delta = int(parts[3])
                    self.incr_count += 1
                    with self._lock:
                        current = (
                            self.ns_store.get((namespace, key))
                            if namespace
                            else self.store.get(key)
                        )
                        if current is None:
                            reply = b"N\n"
                        else:
                            try:
                                new_value = int(current) + delta
                            except ValueError:
                                reply = b"T\n"
                            else:
                                new_bytes = str(new_value).encode("ascii")
                                if namespace:
                                    self.ns_store[(namespace, key)] = new_bytes
                                else:
                                    self.store[key] = new_bytes
                                ttl = self.ttls.get((namespace, key), 0)
                                ttl_field = b" %d" % ttl if ttl else b""
                                reply = b"I %d%b\n%b" % (
                                    len(new_bytes),
                                    ttl_field,
                                    new_bytes,
                                )
                    wfile.write(reply)
                    wfile.flush()

                elif cmd == b"k":
                    # Compare-and-set (issue #141, docs/protocol.html
                    # "k / x"): <namespace-length> <key-length>
                    # <value-length> <cond> [<ttl-seconds>] — <cond> is a
                    # bare token, not length-prefixed: "A" (absent), "P"
                    # (present), or a 32-character hex digest (not
                    # implemented by this mock — see the module
                    # docstring). Always namespaced, no legacy uppercase
                    # form, same as `i`/`c`.
                    namespace = rfile.read(int(parts[1]))
                    key_length = int(parts[2])
                    value_length = int(parts[3])
                    cond = parts[4]
                    ttl = int(parts[5]) if len(parts) > 5 else 0
                    key = rfile.read(key_length)
                    value = rfile.read(value_length)
                    self.cas_set_count += 1
                    with self._lock:
                        current = self.ns_store.get((namespace, key)) if namespace else self.store.get(key)
                        if cond == b"A":
                            matches = current is None
                        elif cond == b"P":
                            matches = current is not None
                        else:
                            matches = False  # digest form: unsupported by this mock
                        if matches:
                            self.ttls[(namespace, key)] = ttl
                            if namespace:
                                self.ns_store[(namespace, key)] = value
                            else:
                                self.store[key] = value
                    wfile.write((b"S" if matches else b"N") + b"\n")
                    wfile.flush()

                elif cmd == b"m":
                    # Batched get (issue #152, docs/protocol.html "m / o"):
                    # <namespace-length> <n> <key-length-1> ... <key-length-n>
                    # — no legacy uppercase form, no per-key W in a
                    # single-node mock (there's only ever one owner).
                    namespace = rfile.read(int(parts[1]))
                    n = int(parts[2])
                    key_lengths = [int(p) for p in parts[3 : 3 + n]]
                    keys = [rfile.read(length) for length in key_lengths]
                    self.multi_get_count += 1
                    values = [self._get_entry(namespace, key) for key in keys]
                    results = [b"%d" % len(value) if value is not None else b"-" for value in values]
                    header = b"M %d %b\n" % (n, b" ".join(results))
                    body = b"".join(value for value in values if value is not None)
                    wfile.write(header + body)
                    wfile.flush()

                elif cmd == b"o":
                    # Batched set (issue #152, docs/protocol.html "m / o"):
                    # <namespace-length> <n> <key-length-1> <value-length-1>
                    # ... <key-length-n> <value-length-n> [<ttl-seconds>] —
                    # one shared TTL for the whole batch, omitted when 0.
                    namespace = rfile.read(int(parts[1]))
                    n = int(parts[2])
                    pair_fields = parts[3 : 3 + 2 * n]
                    key_lengths = [int(p) for p in pair_fields[0::2]]
                    value_lengths = [int(p) for p in pair_fields[1::2]]
                    ttl = int(parts[3 + 2 * n]) if len(parts) > 3 + 2 * n else 0
                    self.multi_set_count += 1
                    for key_length, value_length in zip(key_lengths, value_lengths):
                        key = rfile.read(key_length)
                        value = rfile.read(value_length)
                        self._set_entry(namespace, key, value, ttl)
                    results = b" ".join([b"S"] * n)
                    wfile.write(b"O %d %b\n" % (n, results))
                    wfile.flush()

                else:
                    return
        except (ConnectionError, OSError):
            return
        finally:
            try:
                rfile.close()
            finally:
                wfile.close()
