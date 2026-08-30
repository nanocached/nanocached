"""In-process stand-ins for nanocached-node and nanocached-discovery,
speaking just enough of the wire protocol (``A``, ``G``/``S``/``D`` and
their namespaced ``g``/``s``/``d`` counterparts — issue #105 —
``c``/``F`` to clear a namespace or flush everything — issue #106 —
``i`` to atomically increment/decrement a counter — issue #129 —
``k``/``x`` for compare-and-set — issue #141 — ``L`` and, for SDK proxy
mode (issue #122), ``Q``) for the client tests to exercise
NanocachedClient end-to-end over real TCP sockets without the Rust
binaries. Mirrors the TypeScript SDK's mocks.

SDK proxy mode (issue #122): a mock "proxy" needs no dedicated class — a
proxy looks exactly like a single node that owns every key, and that is
literally what MockNode already is. MockDiscovery just gains a second
roster (``proxies``, served by ``Q``) alongside its existing node roster
(``nodes``, served by ``L``)."""

from __future__ import annotations

import asyncio
import hashlib


def _digest(value: bytes) -> str:
    """Compare-and-set (issue #141): the same digest algorithm
    docs/protocol.html "k / x" and nanocached._digest.content_digest()
    specify — reimplemented independently here (rather than imported)
    so a k/x test genuinely exercises wire-level agreement between the
    client's encoder and this mock's decoder, not just shared code."""
    return hashlib.sha256(value).digest()[:16].hex()


class MockNode:
    def __init__(
        self,
        required_secret: bytes | None = None,
        support_tags: bool = False,
        close_on_extended_auth: bool = False,
        close_on_retryable_auth: bool = False,
    ) -> None:
        self.store: dict[bytes, bytes] = {}
        # Namespaces (issue #105): entries under a non-empty namespace live
        # here instead, keyed by (namespace, key) — separate from `store`
        # so a namespaced key never collides with a same-named default-
        # namespace one, proving isolation the way the real server's
        # namespace-scoped storage does. A `g`/`s`/`d` with namespace-
        # length 0 (the default namespace) still goes to `store` above —
        # see _store_key/_get_entry/_set_entry/_delete_entry — so existing
        # tests that poke `node.store[...]` directly keep working
        # unchanged.
        self.ns_store: dict[tuple[bytes, bytes], bytes] = {}
        # Counters (issue #129): the TTL (whole seconds, 0 = none) each
        # stored entry currently has — set/refreshed on every `S`/`s`,
        # dropped on `D`/`d` and `c`/`F`. `incr`/`decr` never change a
        # key's TTL, but the `I` response must still echo the entry's
        # *remaining* TTL, so this is tracked here the same way the real
        # server tracks it, separately from `store`/`ns_store` which only
        # holds the raw bytes. Keyed identically to those two (namespace
        # b"" lands in the "default" bucket, matching _get_entry/
        # _set_entry/_delete_entry below).
        self.entry_ttl: dict[tuple[bytes, bytes], int] = {}
        self.required_secret = required_secret
        # Echoed response tags: acknowledge `A ... T` with `OnT\n` and echo tags on
        # that connection's replies. Off by default so the bulk of the
        # suite keeps exercising the legacy untagged path.
        self.support_tags = support_tags
        # Behave like a legacy pre-tag server: an extended `A ... T` is a
        # parse error — close the connection without replying.
        self.close_on_extended_auth = close_on_extended_auth
        # Retryable-error status `R` (issue #125): behave like a server
        # that understands `T` but predates the `R` capability token — it
        # accepts `A <len> T` normally but closes on the further-extended
        # `A <len> T R` without replying, exercising the middle fallback
        # stage (T R -> T) as opposed to close_on_extended_auth's oldest-
        # server stage (T -> plain).
        self.close_on_retryable_auth = close_on_retryable_auth
        self.connection_count = 0
        # Retryable-error status `R` (issue #125): the exact `A` header
        # line received on the most recent connection (without the
        # trailing `\n`), and the full history across every connection
        # this node has accepted — lets a test assert the probe form
        # (`A <len> T R`) as well as the fallback sequence across
        # multiple dials to the same mock.
        self.last_auth_header: bytes | None = None
        self.auth_headers: list[bytes] = []
        self.get_count = 0
        # Namespaces (issue #105): counts every `g`/`s`/`d` frame received,
        # regardless of outcome — lets a test prove the default (empty)
        # namespace never leaves this connection as anything but the
        # legacy `G`/`S`/`D` it must stay byte-for-byte compatible with.
        self.namespaced_command_count = 0
        # Clear / flush (issue #106): counts every `c`/`F` frame received,
        # regardless of outcome — mirrors get_count/namespaced_command_count
        # above, so a test can assert a clear's fan-out actually reached
        # every mock node.
        self.clear_count = 0
        self.flush_count = 0
        # Counters (issue #129): counts every `i` frame this node
        # receives, regardless of outcome — the key proof, alongside
        # get_count/namespaced_command_count/clear_count above, that a
        # replica leg of a cluster incr() really did receive a `set`
        # (bumping the store, never this counter) and never an `i` of
        # its own.
        self.incr_count = 0
        # Compare-and-set (issue #141): counts every `k`/`x` frame this
        # node receives, regardless of outcome — the key proof, alongside
        # incr_count above, that a replica leg of a cluster
        # put_if_absent()/replace()/delete_if_matches() call really did
        # receive a plain `set`/`delete` (bumping the store, never these
        # counters) and never a `k`/`x` of its own.
        self.cas_set_count = 0
        self.cas_delete_count = 0
        # Batched get/set (issues #128/#150/#151): counts every `m`/`o`
        # frame this node receives, regardless of outcome — the key
        # proof, alongside incr_count/cas_set_count above, that a batch
        # split across owners really did land exactly one sub-frame per
        # involved node (including a node that is primary for one key
        # and replica for another — it must receive exactly one `o`,
        # never two).
        self.multi_get_count = 0
        self.multi_set_count = 0
        # Issue #222: the exact wire size (header line + namespace + key
        # (+ value) bytes) of every m/o frame received, in send order —
        # lets a test assert a byte-bound-driven split actually keeps
        # every sub-frame under the server's 1 MiB cap, not just that
        # more than one frame went out.
        self.multi_get_frame_sizes: list[int] = []
        self.multi_set_frame_sizes: list[int] = []
        self._fail_clear_replies = 0
        self._wrong_node_replies = 0
        self._wrong_node_on_set_replies = 0
        # Batched get/set (issues #128/#150/#151): unlike
        # answer_wrong_node_once() (whole-frame), M/O need a *per-key* W
        # inside an otherwise-normal roster — see
        # answer_wrong_node_for_keys().
        self._multi_wrong_node_keys: set[bytes] = set()
        self._multi_wrong_node_times = 0
        # Entry-count desync (issue #181): when set, the next `m`/`o`
        # reply's roster is forced to this many entries regardless of how
        # many keys were actually in the request — simulating a corrupt
        # or desynced wire reply, independent of _multi_wrong_node_*
        # above (which keeps the count correct and only changes per-key
        # outcomes).
        self._multi_get_reply_count_override: int | None = None
        self._multi_set_reply_count_override: int | None = None
        self._wrong_tag_replies = 0
        self._swallowed_gets = 0
        self._malformed_value_replies = 0
        self._unterminated_value_replies = 0
        self.unterminated_value_bytes_sent = 0
        self._stored_to_get_replies = 0
        self._malformed_status_replies = 0
        self._missing_tag_replies = 0
        self._invalid_tag_value_replies = 0
        # Retryable-error status `R` (issue #125): answers the next N data
        # requests (G/S/D/g/s/d/c/F) with `R` (tagged correctly) instead
        # of their normal reply — see answer_retryable().
        self._retryable_replies = 0
        self._get_delay = 0.0
        self._gets_delay = 0.0
        self._set_delay = 0.0
        # Node-list refresh dialing new nodes (issue #190): holds the
        # handshake's own `On`/`OnT`/`En` reply, so a test can prove a
        # slow-to-accept node no longer stalls a refresh's dial of every
        # other new node behind it.
        self._auth_delay = 0.0
        self._silent = False
        self.last_set_ttl = 0
        self._server: asyncio.Server | None = None
        self._sockets: set[asyncio.StreamWriter] = set()
        self.port = 0

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def answer_wrong_node_once(self) -> None:
        self._wrong_node_replies += 1

    def answer_wrong_node_on_set_once(self) -> None:
        """Reply ``W`` to the next S specifically (not G/D) — for tests
        that need a node to keep answering GET normally while a later
        SET against it (e.g. a read-repair write-back) fails. Mirrors
        the .NET mock's hook of the same name."""
        self._wrong_node_on_set_replies += 1

    def answer_wrong_tag_once(self) -> None:
        """Queue a one-off reply for the next G request on a tagged
        connection that echoes the WRONG tag (the request's tag + 1) —
        the desync a pre-tag stream misalignment would produce."""
        self._wrong_tag_replies += 1

    def swallow_get_once(self) -> None:
        """Swallow the next G request entirely (no reply) — the
        off-by-one stream desync where every later response answers the
        previous request."""
        self._swallowed_gets += 1

    def answer_malformed_value_once(self) -> None:
        self._malformed_value_replies += 1

    def answer_unterminated_value_once(self) -> None:
        """Reply `V` to the next G, then stream chunks of non-newline
        bytes (until the socket is closed, or a large safety cap is hit)
        instead of ever completing the header — simulating a malicious or
        corrupted server withholding the terminating LF forever."""
        self._unterminated_value_replies += 1

    def answer_stored_to_get_once(self) -> None:
        """Reply `S` to the next G — a well-formed frame of the wrong kind,
        as a desynced (off-by-one) stream would produce."""
        self._stored_to_get_replies += 1

    def answer_malformed_status_once(self) -> None:
        """Reply with a tagged-shaped fixed response (e.g. `S1\\n`) to the
        next G on an untagged connection — the byte after the marker is
        never LF, as a server that tagged a response on an untagged
        connection (or some other desync) would produce."""
        self._malformed_status_replies += 1

    def answer_missing_tag_once(self) -> None:
        """Reply to the next G on a *tagged* connection with the untagged
        fixed form (`N\\n`, no trailing tag field) — a server that forgot
        to tag its reply, or some other desync, would produce this."""
        self._missing_tag_replies += 1

    def answer_invalid_tag_value_once(self) -> None:
        """Reply to the next G on a *tagged* connection with a `V` whose
        tag field is non-numeric (`V 1 abc\\n1`) — exercises
        Connection._parse_tag's invalid-value path (issue #47 audit
        item 5), as opposed to answer_missing_tag_once's missing-field
        desync above."""
        self._invalid_tag_value_replies += 1

    def answer_retryable(self, times: int = 1) -> None:
        """Answers the next ``times`` data requests (G/S/D/g/s/d/c/F,
        whichever arrives next, in any mix) with `R` (issue #125) instead
        of their normal reply — `R <tag>` on a tagged connection, plain
        `R` otherwise. Simulates nanocached-proxy answering a transiently
        failed request without closing the connection."""
        self._retryable_replies += times

    def answer_wrong_node_for_keys(self, keys: set[bytes], times: int = 1) -> None:
        """The next ``times`` m/o requests answer `W` for any key in
        ``keys`` inside their roster, and normally for every other key
        in the same batch (issues #128/#150/#151) — unlike
        answer_wrong_node_once(), which is whole-frame, M/O need a
        per-key W. ``times`` counts down per m/o request received, not
        per key."""
        self._multi_wrong_node_keys = set(keys)
        self._multi_wrong_node_times = times

    def answer_multi_get_bad_count_once(self, count: int) -> None:
        """The next `m` reply reports exactly ``count`` roster entries no
        matter how many keys the request actually had (issue #181) —
        proves Connection.multi_get() rejects a short/long reply as a
        desync instead of silently misaligning every later key via the
        slice assignment onto ``entries``."""
        self._multi_get_reply_count_override = count

    def answer_multi_set_bad_count_once(self, count: int) -> None:
        """Same as answer_multi_get_bad_count_once() but for the next `o`
        reply (issue #181)."""
        self._multi_set_reply_count_override = count

    def fail_next_clear_once(self) -> None:
        """Closes the connection instead of acking the next `c`/`F` this
        node receives (issue #106) — simulates a node that's unreachable
        when a clear's fan-out reaches it, so a test can exercise the
        client's refresh-once-and-retry path. Call it more than once
        (e.g. twice) to also fail the retry, for the persistent-failure
        path."""
        self._fail_clear_replies += 1

    def delay_next_get(self, seconds: float) -> None:
        """Hold the next G's response, so a test can abandon the request
        mid-flight (asyncio.wait_for) and probe cancellation safety."""
        self._get_delay = seconds

    def delay_gets(self, seconds: float) -> None:
        """Hold every future G's response — a slow-but-alive node, for
        hedged-read tests (issue #64)."""
        self._gets_delay = seconds

    def delay_sets(self, seconds: float) -> None:
        """Hold every future S's response — for tests proving a caller
        isn't blocked on a slow replica leg (fire-and-forget replica writes)."""
        self._set_delay = seconds

    def delay_next_auth(self, seconds: float) -> None:
        """Hold the next connection's `A` handshake reply — simulates a
        newly-joined node that is slow to accept/authenticate, so a test
        can prove a node-list refresh dials new nodes concurrently
        (issue #190) instead of letting one slow dial stall every other
        new node behind it."""
        self._auth_delay = seconds

    def go_silent_after_handshake(self) -> None:
        """Makes this node a half-open server from this point on: it
        still accepts and completes the ``A`` handshake, and still reads
        every request frame off the wire (so the TCP stream stays
        well-formed), but never writes a reply — regression coverage for
        the request timeout (issue #42), mirroring the Go suite's hook
        of the same name."""
        self._silent = True

    async def start(self, port: int = 0) -> "MockNode":
        """``port`` pins the listener — for a node that comes back on the
        address discovery already advertised (issue #67 tests)."""
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", port)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    def drop_connections(self) -> None:
        """Server-side FIN on every open connection, like the idle timeout."""
        for writer in list(self._sockets):
            writer.close()

    async def close(self) -> None:
        self.drop_connections()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    # Namespaces (issue #105): a namespace-length of 0 (default namespace)
    # is routed to `store` — the same entries `G`/`S`/`D` see — exactly
    # like the real server's "g 0 ... == G ..." rule; a non-empty
    # namespace is routed to `ns_store`, keyed by (namespace, key), so it
    # never collides with a same-named default-namespace entry.
    def _get_entry(self, namespace: bytes, key: bytes) -> bytes | None:
        return self.store.get(key) if not namespace else self.ns_store.get((namespace, key))

    def _set_entry(self, namespace: bytes, key: bytes, value: bytes) -> None:
        if namespace:
            self.ns_store[(namespace, key)] = value
        else:
            self.store[key] = value

    def _delete_entry(self, namespace: bytes, key: bytes) -> bool:
        if namespace:
            return self.ns_store.pop((namespace, key), None) is not None
        return self.store.pop(key, None) is not None

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connection_count += 1
        self._sockets.add(writer)
        # Echoed response tags: set when this connection's `A ... T` was acknowledged
        # — its requests then carry a trailing tag the replies must echo.
        tagged = False
        try:
            while True:
                try:
                    header = await reader.readuntil(b"\n")
                except (asyncio.IncompleteReadError, ConnectionError):
                    return
                parts = header[:-1].split(b" ")
                if parts[0] in (b"g", b"s", b"d"):
                    self.namespaced_command_count += 1
                # On a tagged connection every request's last header field
                # is its tag, echoed back as each reply's own last field.
                tag_suffix = b" " + parts[-1] if tagged else b""

                if parts[0] == b"A":
                    # Retryable-error status `R` (issue #125): recorded
                    # before any close-on-legacy-mode branch below, so a
                    # test can assert the exact probe form even against a
                    # mock that then slams the door on it.
                    self.last_auth_header = header[:-1]
                    self.auth_headers.append(header[:-1])

                    if len(parts) > 2 and self.close_on_extended_auth:
                        writer.close()
                        return
                    if len(parts) > 3 and self.close_on_retryable_auth:
                        writer.close()
                        return

                    secret = await reader.readexactly(int(parts[1]))
                    accepted = (
                        len(secret) > 0
                        if self.required_secret is None
                        else secret == self.required_secret
                    )
                    tagged = accepted and self.support_tags and len(parts) > 2 and parts[2] == b"T"
                    if self._auth_delay > 0:
                        delay, self._auth_delay = self._auth_delay, 0.0
                        await asyncio.sleep(delay)
                    writer.write(b"OnT\n" if tagged else (b"On\n" if accepted else b"En\n"))
                    await writer.drain()
                    if not accepted:
                        return

                elif parts[0] in (b"G", b"g"):
                    # Namespaces (issue #105): `g` gains one leading
                    # <namespace-length> header field, with namespace
                    # bytes leading the body — everything else about the
                    # request/response is identical to `G`, tag included.
                    if parts[0] == b"g":
                        namespace = await reader.readexactly(int(parts[1]))
                        key = await reader.readexactly(int(parts[2]))
                    else:
                        namespace = b""
                        key = await reader.readexactly(int(parts[1]))
                    self.get_count += 1
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._get_delay > 0:
                        delay, self._get_delay = self._get_delay, 0.0
                        await asyncio.sleep(delay)
                    if self._gets_delay > 0:
                        await asyncio.sleep(self._gets_delay)
                    if self._swallowed_gets > 0:
                        self._swallowed_gets -= 1
                        continue
                    if self._wrong_tag_replies > 0 and tagged:
                        self._wrong_tag_replies -= 1
                        writer.write(b"N %d\n" % (int(parts[-1]) + 1))
                        await writer.drain()
                        continue
                    if self._stored_to_get_replies > 0:
                        self._stored_to_get_replies -= 1
                        writer.write(b"S" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._malformed_status_replies > 0:
                        self._malformed_status_replies -= 1
                        writer.write(b"S1\n")
                        await writer.drain()
                        continue
                    if self._malformed_value_replies > 0:
                        self._malformed_value_replies -= 1
                        writer.write(b"V x\n")
                        await writer.drain()
                        continue
                    if self._unterminated_value_replies > 0:
                        self._unterminated_value_replies -= 1
                        writer.write(b"V")
                        try:
                            while self.unterminated_value_bytes_sent <= 512 * 1024:
                                filler = b"9" * 1024
                                writer.write(filler)
                                await writer.drain()
                                self.unterminated_value_bytes_sent += len(filler)
                        except (ConnectionError, OSError):
                            pass
                        return
                    if self._missing_tag_replies > 0 and tagged:
                        self._missing_tag_replies -= 1
                        writer.write(b"N\n")
                        await writer.drain()
                        continue
                    if self._invalid_tag_value_replies > 0 and tagged:
                        self._invalid_tag_value_replies -= 1
                        writer.write(b"V 1 abc\n1")
                        await writer.drain()
                        continue
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                    else:
                        value = self._get_entry(namespace, key)
                        if value is not None:
                            writer.write(b"V %d%b\n%b" % (len(value), tag_suffix, value))
                        else:
                            writer.write(b"N" + tag_suffix + b"\n")
                    await writer.drain()

                elif parts[0] in (b"S", b"s"):
                    # Namespaces (issue #105): `s`'s header is `s
                    # <ns-len> <key-len> <val-len> [<ttl>] [<tag>]` —
                    # otherwise identical to `S`.
                    if parts[0] == b"s":
                        namespace = await reader.readexactly(int(parts[1]))
                        key = await reader.readexactly(int(parts[2]))
                        value = await reader.readexactly(int(parts[3]))
                        # parts[4], when present (and not the tag itself),
                        # is the TTL — one field later than S's own,
                        # because of the leading ns-len field above.
                        base_field_count = 5 if tagged else 4
                        self.last_set_ttl = int(parts[4]) if len(parts) > base_field_count else 0
                    else:
                        namespace = b""
                        key = await reader.readexactly(int(parts[1]))
                        value = await reader.readexactly(int(parts[2]))
                        # parts[3], when present (and not the tag itself),
                        # is the TTL (omitted on the wire means "no
                        # expiry", i.e. 0 — see _encode_set's doc comment
                        # in _connection.py). On a tagged connection the
                        # tag sits after it as the last field.
                        base_field_count = 4 if tagged else 3
                        self.last_set_ttl = int(parts[3]) if len(parts) > base_field_count else 0
                    # Counters (issue #129): captured as a local right
                    # after parsing, not read back off self.last_set_ttl
                    # below — an intervening await (self._set_delay) could
                    # otherwise race a concurrent connection's own S/s and
                    # record the wrong TTL for this entry.
                    ttl_for_entry = self.last_set_ttl
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._set_delay > 0:
                        await asyncio.sleep(self._set_delay)
                    if self._wrong_node_on_set_replies > 0:
                        self._wrong_node_on_set_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                    elif self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                    else:
                        self._set_entry(namespace, key, value)
                        self.entry_ttl[(namespace, key)] = ttl_for_entry
                        writer.write(b"S" + tag_suffix + b"\n")
                    await writer.drain()

                elif parts[0] in (b"D", b"d"):
                    if parts[0] == b"d":
                        namespace = await reader.readexactly(int(parts[1]))
                        key = await reader.readexactly(int(parts[2]))
                    else:
                        namespace = b""
                        key = await reader.readexactly(int(parts[1]))
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                    else:
                        deleted = self._delete_entry(namespace, key)
                        self.entry_ttl.pop((namespace, key), None)
                        writer.write((b"D" if deleted else b"N") + tag_suffix + b"\n")
                    await writer.drain()

                elif parts[0] in (b"c", b"F"):
                    # Clear / flush (issue #106): `c` always carries a
                    # namespace-length header field, even 0 for the
                    # default namespace — unlike g/s/d it has no legacy
                    # uppercase form to fall back to. `F` carries no key
                    # or namespace at all, just the optional tag.
                    if parts[0] == b"c":
                        namespace = await reader.readexactly(int(parts[1]))
                        self.clear_count += 1
                    else:
                        self.flush_count += 1
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._fail_clear_replies > 0:
                        self._fail_clear_replies -= 1
                        writer.close()
                        return
                    if parts[0] == b"c":
                        if namespace:
                            for ns, ns_key in list(self.ns_store):
                                if ns == namespace:
                                    del self.ns_store[(ns, ns_key)]
                            for ns, ns_key in list(self.entry_ttl):
                                if ns == namespace:
                                    del self.entry_ttl[(ns, ns_key)]
                        else:
                            self.store.clear()
                            for ns, ns_key in list(self.entry_ttl):
                                if not ns:
                                    del self.entry_ttl[(ns, ns_key)]
                    else:
                        self.store.clear()
                        self.ns_store.clear()
                        self.entry_ttl.clear()
                    writer.write(b"C" + tag_suffix + b"\n")
                    await writer.drain()

                elif parts[0] == b"i":
                    # Counters (issue #129): `i <ns-len> <key-len>
                    # <delta> [<tag>]` — always namespaced (namespace-
                    # length 0 for the default namespace), no legacy
                    # uppercase form, mirroring `c` above.
                    namespace = await reader.readexactly(int(parts[1]))
                    key = await reader.readexactly(int(parts[2]))
                    delta = int(parts[3])
                    self.incr_count += 1
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    current = self._get_entry(namespace, key)
                    if current is None:
                        writer.write(b"N" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    try:
                        new_value = int(current) + delta
                    except ValueError:
                        writer.write(b"T" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if not (-(2**63) <= new_value <= 2**63 - 1):
                        writer.write(b"T" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    new_bytes = str(new_value).encode()
                    self._set_entry(namespace, key, new_bytes)
                    ttl = self.entry_ttl.get((namespace, key), 0)
                    ttl_field = b" %d" % ttl if ttl else b""
                    writer.write(b"I %d%b%b\n%b" % (len(new_bytes), ttl_field, tag_suffix, new_bytes))
                    await writer.drain()

                elif parts[0] == b"k":
                    # Compare-and-set (issue #141): `k <ns-len> <key-len>
                    # <val-len> <cond> [<ttl-seconds>] [<tag>]` — like `i`
                    # above, always namespaced, no legacy uppercase form.
                    # <cond> is a bare token: `A`/`P`, or a 32-character
                    # lowercase hex digest.
                    namespace = await reader.readexactly(int(parts[1]))
                    key = await reader.readexactly(int(parts[2]))
                    value = await reader.readexactly(int(parts[3]))
                    cond = parts[4]
                    base_field_count = 6 if tagged else 5
                    ttl_for_entry = int(parts[5]) if len(parts) > base_field_count else 0
                    self.cas_set_count += 1
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    current = self._get_entry(namespace, key)
                    if cond == b"A":
                        matches = current is None
                    elif cond == b"P":
                        matches = current is not None
                    else:
                        matches = current is not None and _digest(current) == cond.decode()
                    if matches:
                        self._set_entry(namespace, key, value)
                        self.entry_ttl[(namespace, key)] = ttl_for_entry
                        writer.write(b"S" + tag_suffix + b"\n")
                    else:
                        writer.write(b"N" + tag_suffix + b"\n")
                    await writer.drain()

                elif parts[0] == b"x":
                    # Compare-and-set (issue #141): `x <ns-len> <key-len>
                    # <cond> [<tag>]` — <cond> here is always a digest,
                    # never `A`/`P` (an absent- or present-only
                    # conditioned delete is already the plain `D`/`d`).
                    namespace = await reader.readexactly(int(parts[1]))
                    key = await reader.readexactly(int(parts[2]))
                    cond = parts[3]
                    self.cas_delete_count += 1
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    current = self._get_entry(namespace, key)
                    if current is not None and _digest(current) == cond.decode():
                        self._delete_entry(namespace, key)
                        self.entry_ttl.pop((namespace, key), None)
                        writer.write(b"D" + tag_suffix + b"\n")
                    else:
                        writer.write(b"N" + tag_suffix + b"\n")
                    await writer.drain()

                elif parts[0] == b"m":
                    # Batched get (issues #128/#150/#151): `m <ns-len>
                    # <n> <key-len-1>...<key-len-n> [<tag>]\n<ns><key-1>
                    # ...<key-n>` — always namespaced, no legacy
                    # uppercase form, mirroring `i`/`k`/`x` above.
                    namespace = await reader.readexactly(int(parts[1]))
                    count = int(parts[2])
                    key_lengths = [int(x) for x in parts[3 : 3 + count]]
                    keys = [await reader.readexactly(length) for length in key_lengths]
                    self.multi_get_count += 1
                    self.multi_get_frame_sizes.append(len(header) + len(namespace) + sum(key_lengths))
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    consume_wrong = self._multi_wrong_node_times > 0
                    if consume_wrong:
                        self._multi_wrong_node_times -= 1
                    results: list[tuple[bytes, bytes | None]] = []
                    for key in keys:
                        if consume_wrong and key in self._multi_wrong_node_keys:
                            results.append((b"W", None))
                            continue
                        value = self._get_entry(namespace, key)
                        if value is None:
                            results.append((b"-", None))
                        else:
                            results.append((b"%d" % len(value), value))
                    # Entry-count desync (issue #181): force the reply to
                    # a caller-chosen entry count, trimming or padding
                    # with clean-miss tokens regardless of what was
                    # actually requested/found above.
                    if self._multi_get_reply_count_override is not None:
                        override = self._multi_get_reply_count_override
                        self._multi_get_reply_count_override = None
                        if override <= len(results):
                            results = results[:override]
                        else:
                            results = results + [(b"-", None)] * (override - len(results))
                    roster_tokens = [token for token, _ in results]
                    body = b"".join(value for _, value in results if value is not None)
                    header = (
                        b" ".join([b"M", b"%d" % len(roster_tokens), *roster_tokens]) + tag_suffix + b"\n"
                    )
                    writer.write(header + body)
                    await writer.drain()

                elif parts[0] == b"o":
                    # Batched set (issues #128/#150/#151): `o <ns-len>
                    # <n> <key-len-1> <value-len-1>...<key-len-n>
                    # <value-len-n> [<ttl>] [<tag>]\n<ns><k1><v1>...
                    # <kn><vn>` — one shared TTL for the whole batch.
                    namespace = await reader.readexactly(int(parts[1]))
                    count = int(parts[2])
                    length_fields = parts[3 : 3 + 2 * count]
                    key_lengths = [int(x) for x in length_fields[0::2]]
                    value_lengths = [int(x) for x in length_fields[1::2]]
                    base_field_count = (3 + 2 * count) + (1 if tagged else 0)
                    ttl = int(parts[3 + 2 * count]) if len(parts) > base_field_count else 0
                    self.last_set_ttl = ttl
                    keys = []
                    values = []
                    for key_len, value_len in zip(key_lengths, value_lengths):
                        keys.append(await reader.readexactly(key_len))
                        values.append(await reader.readexactly(value_len))
                    self.multi_set_count += 1
                    self.multi_set_frame_sizes.append(
                        len(header) + len(namespace) + sum(key_lengths) + sum(value_lengths)
                    )
                    if self._silent:
                        continue
                    if self._retryable_replies > 0:
                        self._retryable_replies -= 1
                        writer.write(b"R" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    consume_wrong = self._multi_wrong_node_times > 0
                    if consume_wrong:
                        self._multi_wrong_node_times -= 1
                    roster_tokens = []
                    for key, value in zip(keys, values):
                        if consume_wrong and key in self._multi_wrong_node_keys:
                            roster_tokens.append(b"W")
                            continue
                        self._set_entry(namespace, key, value)
                        self.entry_ttl[(namespace, key)] = ttl
                        roster_tokens.append(b"S")
                    # Entry-count desync (issue #181): see the `m` handler
                    # above for why/how.
                    if self._multi_set_reply_count_override is not None:
                        override = self._multi_set_reply_count_override
                        self._multi_set_reply_count_override = None
                        if override <= len(roster_tokens):
                            roster_tokens = roster_tokens[:override]
                        else:
                            roster_tokens = roster_tokens + [b"S"] * (override - len(roster_tokens))
                    header = (
                        b" ".join([b"O", b"%d" % len(roster_tokens), *roster_tokens]) + tag_suffix + b"\n"
                    )
                    writer.write(header)
                    await writer.drain()

                else:
                    return
        finally:
            self._sockets.discard(writer)
            writer.close()


class MockDiscovery:
    def __init__(
        self,
        nodes: list[tuple[str, str]],
        replication: int = 1,
        proxies: list[tuple[str, str]] | None = None,
    ) -> None:
        self.nodes = nodes
        self.replication = replication
        # SDK proxy mode (issue #122): the roster `Q` serves, kept
        # separate from `nodes` — a client in via_proxy mode must never
        # be routed to (or even ask for) the node roster. `set_proxies`
        # lets a test update it mid-run (a proxy dying/restarting).
        self.proxies = list(proxies) if proxies else []
        self.warming_up = False
        self._unterminated_list_replies = 0
        self.unterminated_list_bytes_sent = 0
        # Counts every `L`/`Q` this discovery receives, regardless of
        # outcome (even a `B` refusal) — lets a test assert the node
        # roster was never asked for in via_proxy mode, or vice versa.
        self.list_count = 0
        self.list_proxies_count = 0
        self._server: asyncio.Server | None = None
        self.port = 0

    def set_proxies(self, proxies: list[tuple[str, str]]) -> None:
        """Updates the roster `Q` serves from here on — for reconnect
        tests that kill one proxy and need discovery to then hand back
        only the survivor(s)."""
        self.proxies = list(proxies)

    def answer_unterminated_list_once(self) -> None:
        """Reply `N` to the next L, then stream chunks of non-newline
        bytes (until the socket is closed, or a large safety cap is hit)
        instead of ever completing the header — mirrors MockNode's
        answer_unterminated_value_once on the cache-node path."""
        self._unterminated_list_replies += 1

    async def start(self) -> "MockDiscovery":
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                try:
                    header = await reader.readuntil(b"\n")
                except (asyncio.IncompleteReadError, ConnectionError):
                    return
                parts = header[:-1].split(b" ")

                if parts[0] == b"A":
                    await reader.readexactly(int(parts[1]))
                    # Echoed response tags: echo the tag capability — clients send the
                    # extended A before knowing which kind of server
                    # answered. Discovery's `L` exchange never uses tags
                    # itself, so nothing else here depends on this.
                    writer.write(b"OdT\n" if len(parts) > 2 and parts[2] == b"T" else b"Od\n")
                    await writer.drain()
                elif parts[0] == b"L":
                    self.list_count += 1
                    if self.warming_up:
                        writer.write(b"B\n")
                        await writer.drain()
                        return
                    if self._unterminated_list_replies > 0:
                        self._unterminated_list_replies -= 1
                        writer.write(b"N")
                        try:
                            while self.unterminated_list_bytes_sent <= 512 * 1024:
                                filler = b"9" * 1024
                                writer.write(filler)
                                await writer.drain()
                                self.unterminated_list_bytes_sent += len(filler)
                        except (ConnectionError, OSError):
                            pass
                        return
                    frame = b"N %d %d\n" % (len(self.nodes), self.replication)
                    for name, address in self.nodes:
                        name_b, addr_b = name.encode(), address.encode()
                        frame += b"%d %d\n%b%b\n" % (len(name_b), len(addr_b), name_b, addr_b)
                    writer.write(frame)
                    await writer.drain()
                elif parts[0] == b"Q":
                    # SDK proxy mode (issue #122): `L`'s entry shape with
                    # no replication field — same startup-grace `B`
                    # refusal, served from the separate `proxies` roster.
                    self.list_proxies_count += 1
                    if self.warming_up:
                        writer.write(b"B\n")
                        await writer.drain()
                        return
                    frame = b"N %d\n" % len(self.proxies)
                    for name, address in self.proxies:
                        name_b, addr_b = name.encode(), address.encode()
                        frame += b"%d %d\n%b%b\n" % (len(name_b), len(addr_b), name_b, addr_b)
                    writer.write(frame)
                    await writer.drain()
                else:
                    return
        finally:
            writer.close()


async def unused_port() -> int:
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    return port
