#!/usr/bin/env python3
"""Emit the case file the conformance drivers read: ``make_cases.py <run-id>``.

The cases are defined once, here; every SDK driver reads the JSON this prints
and reports what it observed for each step. ``compare.py`` checks the reports
against the ``expect`` in each step (the protocol/SDK-doc oracle) and against
each other (six-way agreement).

Value specs (what a driver must materialise):
    {"text": "..."}              UTF-8 bytes
    {"b64": "..."}               raw bytes
    {"len": N}                   N bytes, byte i = i % 251 (the sha is checked)
Keys are always {"b64": ...}; every key carries the run id, so re-runs against
a long-lived node never inherit state.

Step ops: set | get | get_text | delete | sleep | probe
    set   {key, value, ttl}      ttl: null (omit) | number | numeric string
    get   {key}                  reports hit / len / sha256 of the bytes
    get_text {key}               same through the SDK's string API
    delete {key}                 reports deleted
    sleep {ms}
    probe                        set+get a tiny key: is the connection usable?
Expectations (``expect``):
    {"ok": true} | {"hit": true, "value": spec} | {"hit": false}
    | {"deleted": bool} | "any" (implementation-defined; six-way agreement only)
"""

import base64
import hashlib
import json
import sys

MAX_SDK_BYTES = 1024 * 1024 - 256  # the SDK-side bound on namespace+key+value


def b64(data):
    return base64.b64encode(data).decode()


def value_bytes(spec):
    if "text" in spec:
        return spec["text"].encode()
    if "b64" in spec:
        return base64.b64decode(spec["b64"])
    return bytes(i % 251 for i in range(spec["len"]))


def sha(spec):
    return hashlib.sha256(value_bytes(spec)).hexdigest()


def build_basic(run):
    cases = []
    n = [0]

    def key(label, size=None, raw=None):
        """A key unique to this run+case. ``size`` pads to an exact length."""
        n[0] += 1
        if raw is not None:
            return {"b64": b64(raw)}
        base = f"{run}:{label}".encode()
        if size is not None:
            assert size >= 1
            base = (base + b"k" * size)[:size] if size > len(base) else base[:size]
        return {"b64": b64(base)}

    def set_(k, v, ttl=None, expect=None):
        return {"op": "set", "key": k, "value": v, "ttl": ttl, "expect": expect if expect is not None else {"ok": True}}

    def get(k, v=None, hit=True, expect=None, op="get"):
        if expect is None:
            expect = {"hit": True, "value": v} if hit else {"hit": False}
        return {"op": op, "key": k, "expect": expect}

    def delete(k, existed):
        return {"op": "delete", "key": k, "expect": {"deleted": existed}}

    def sleep(ms):
        return {"op": "sleep", "ms": ms}

    def case(cid, title, steps):
        cases.append({"id": cid, "title": title, "steps": steps})

    # 1. Round trip -------------------------------------------------------
    special = b"\n\0\r \xff\xfe" + "日本語🙂".encode()
    for cid, spec in [
        ("rt.empty", {"text": ""}),
        ("rt.1B", {"text": "x"}),
        ("rt.special", {"b64": b64(special)}),
        ("rt.64KiB", {"len": 64 * 1024}),
    ]:
        k = key(cid)
        case(cid, f"set/get round trip: {cid}", [set_(k, spec), get(k, spec)])
    k = key("rt.max", None)
    # key + value == MAX_SDK_BYTES exactly
    klen = len(base64.b64decode(k["b64"]))
    spec = {"len": MAX_SDK_BYTES - klen}
    case("rt.max", f"largest allowed entry (key {klen} B + value {spec['len']} B = {MAX_SDK_BYTES})",
         [set_(k, spec), get(k, spec)])

    # 2. Miss vs empty value ---------------------------------------------
    km, ke = key("miss.absent"), key("miss.empty")
    case("miss_vs_empty", "an absent key and a stored empty value are distinguishable",
         [get(km, hit=False), set_(ke, {"text": ""}), get(ke, {"text": ""})])

    # 3. Over the limit ---------------------------------------------------
    k = key("over.value")
    klen = len(base64.b64decode(k["b64"]))
    over = {"len": MAX_SDK_BYTES - klen + 1}
    case("over.value", f"key {klen} B + value {over['len']} B = {MAX_SDK_BYTES + 1}: one byte over the SDK bound",
         [set_(k, over, expect="any"), get(k, expect="any"), {"op": "probe"}])
    k = key("over.key", size=MAX_SDK_BYTES)
    case("over.key_max", f"a key of exactly {MAX_SDK_BYTES} B with an empty value",
         [set_(k, {"text": ""}, expect="any"), get(k, expect="any"), {"op": "probe"}])
    k = key("over.key1", size=MAX_SDK_BYTES + 1)
    case("over.key_max+1", f"a key of {MAX_SDK_BYTES + 1} B",
         [set_(k, {"text": ""}, expect="any"), get(k, expect="any"), {"op": "probe"}])

    # 4. Keys ---------------------------------------------------------------
    for cid, raw in [
        ("key.space", f"{run}:a b".encode()),
        ("key.newline", f"{run}:a\nb".encode()),
        ("key.nonascii", f"{run}:キー🙂".encode()),
        ("key.binary", f"{run}:".encode() + b"\x00\xff\x80"),
    ]:
        k = key(cid, raw=raw)
        v = {"text": "v-" + cid}
        case(cid, f"key with {cid.split('.')[1]} bytes round trips", [set_(k, v), get(k, v)])
    k = key("key.1KiB", size=1024)
    case("key.1KiB", "a 1 KiB key round trips", [set_(k, {"text": "v"}), get(k, {"text": "v"})])
    k = key("key.64KiB", size=64 * 1024)
    case("key.64KiB", "a 64 KiB key round trips", [set_(k, {"text": "v"}), get(k, {"text": "v"})])
    k = key("key.empty", raw=b"")
    case("key.empty", "the empty key (implementation-defined)",
         [set_(k, {"text": "v"}, expect="any"), get(k, expect="any"), delete(k, False) | {"expect": "any"}, {"op": "probe"}])

    # 5. Strings through the SDK text API ----------------------------------
    k = key("text.ascii")
    case("text.ascii", "the text getter returns ASCII text", [set_(k, {"text": "hello"}), get(k, {"text": "hello"}, op="get_text")])
    k = key("text.utf8")
    case("text.utf8", "the text getter returns multi-byte UTF-8", [set_(k, {"text": "日本語🙂"}), get(k, {"text": "日本語🙂"}, op="get_text")])
    k = key("text.invalid")
    bad = {"b64": b64(b"\xff\xfe\x80abc")}
    case("text.invalid_utf8", "text getter on a value that is not valid UTF-8 (implementation-defined)",
         [set_(k, bad), get(k, bad), get(k, expect="any", op="get_text"), {"op": "probe"}])

    # 6. TTL -----------------------------------------------------------------
    v = {"text": "ttl"}
    k = key("ttl.none")
    case("ttl.none", "no TTL: still there after 2.5 s", [set_(k, v), sleep(2500), get(k, v)])
    k = key("ttl.zero")
    case("ttl.zero", "explicit ttl 0 means no expiry at the SDK level (docs)", [set_(k, v, 0), get(k, v), sleep(2500), get(k, v)])
    k = key("ttl.1s")
    case("ttl.1s", "ttl 1: present at first, gone after 2.5 s", [set_(k, v, 1), get(k, v), sleep(2500), get(k, hit=False)])
    k = key("ttl.3s")
    case("ttl.3s", "ttl 3: still there after 1 s", [set_(k, v, 3), sleep(1000), get(k, v)])
    k = key("ttl.clear")
    case("ttl.rewrite_clears", "rewriting without a TTL clears the old expiry", [set_(k, v, 1), set_(k, v, None), sleep(2500), get(k, v)])
    k = key("ttl.set")
    case("ttl.rewrite_sets", "rewriting with a TTL sets an expiry on a previously permanent key", [set_(k, v), set_(k, v, 1), sleep(2500), get(k, hit=False)])
    for cid, ttl in [
        ("ttl.negative", -1),
        ("ttl.fractional", 1.5),
        ("ttl.u32+1", "4294967296"),
        ("ttl.i64max", "9223372036854775807"),
        ("ttl.u64max", "18446744073709551615"),
    ]:
        k = key(cid)
        case(cid, f"TTL argument {ttl} (implementation-defined; value afterwards recorded)",
             [set_(k, v, ttl, expect="any"), get(k, expect="any"), {"op": "probe"}])

    # 7. Overwrite -----------------------------------------------------------
    k = key("ow.last")
    case("overwrite.last_wins", "the last write wins", [set_(k, {"text": "one"}), set_(k, {"text": "two"}), get(k, {"text": "two"})])
    k = key("ow.shrink")
    case("overwrite.shrink", "a long value overwritten by a short one", [set_(k, {"len": 70000}), set_(k, {"text": "s"}), get(k, {"text": "s"})])

    # 8. Delete -------------------------------------------------------------
    k, kmiss = key("del.k"), key("del.missing")
    v = {"text": "d"}
    case("delete.basic", "delete: existing -> true, again -> false, missing -> false, get after delete misses",
         [set_(k, v), delete(k, True), delete(k, False), delete(kmiss, False), get(k, hit=False)])
    k = key("del.reuse")
    case("delete.then_set", "a deleted key can be set again", [set_(k, v), delete(k, True), set_(k, {"text": "e"}), get(k, {"text": "e"})])

    # 9. Cross-SDK: each SDK writes under its own name, every SDK reads all.
    xs = []
    for name, spec, ttl in [
        ("empty", {"text": ""}, None),
        ("text", {"text": "hello"}, None),
        ("special", {"b64": b64(special)}, None),
        ("64KiB", {"len": 64 * 1024}, None),
        ("ttl300", {"text": "t"}, 300),
        ("1MiB", {"len": 1024 * 1024 - 256 - 80}, None),
    ]:
        xs.append({"name": name, "key_template": f"xs:{run}:{{sdk}}:{name}", "value": spec, "ttl": ttl,
                   "expect_sha": sha(spec), "expect_len": len(value_bytes(spec))})

    # attach the expected sha so the driver output can be checked without values
    for c in cases:
        for s in c["steps"]:
            e = s.get("expect")
            if isinstance(e, dict) and "value" in e:
                e["sha256"] = sha(e["value"])
                e["len"] = len(value_bytes(e["value"]))
                del e["value"]
    return {"run": run, "max_sdk_bytes": MAX_SDK_BYTES, "cases": cases, "xsdk": xs}


def build(run, group="basic"):
    if group == "basic":
        return build_basic(run)
    doc = {"run": run, "max_sdk_bytes": MAX_SDK_BYTES, "cases": GROUPS[group](run), "xsdk": []}
    finish(doc["cases"])
    return doc



# ---------------------------------------------------------------------------
# More groups: ns (namespaces), ctr (incr/decr + compare-and-set), many (batches)
# ---------------------------------------------------------------------------

def gen_value(i, length):
    """Deterministic per-index value for the generated batch cases."""
    return bytes((i + j) % 251 for j in range(length))


def finish(cases):
    """Replace {"value": spec} in expectations by sha/len (and the CAS token)."""
    for c in cases:
        for s in c["steps"]:
            e = s.get("expect")
            if isinstance(e, dict) and "value" in e:
                data = value_bytes(e["value"])
                e["sha256"] = hashlib.sha256(data).hexdigest()
                e["len"] = len(data)
                if e.get("token") is True:
                    e["token"] = e["sha256"][:32]  # CAS token = first 16 bytes of SHA-256, hex
                del e["value"]


def _kit(run):
    def key(label):
        return {"b64": b64(f"{run}:{label}".encode())}

    def nsb(name):
        return b64(name.encode() if isinstance(name, str) else name)

    def step(op, **kw):
        d = {"op": op}
        d.update(kw)
        d.setdefault("expect", {"ok": True})
        return d

    def case(cases, cid, title, steps):
        cases.append({"id": cid, "title": title, "steps": steps})

    return key, nsb, step, case


def build_ns(run):
    key, nsb, step, case = _kit(run)
    cases = []
    A, B = nsb(f"{run}-A"), nsb(f"{run}-B")
    v = lambda t: {"text": t}
    get = lambda k, ns, val=None, **kw: step("get", key=k, ns=ns, expect={"hit": True, "value": val} if val is not None else {"hit": False}, **kw)

    k = key("iso")
    case(cases, "ns.isolation", "the same key in the default namespace and two named ones holds three independent values",
         [step("set", key=k, ns=None, value=v("d"), ttl=None), step("set", key=k, ns=A, value=v("a"), ttl=None), step("set", key=k, ns=B, value=v("b"), ttl=None),
          get(k, None, v("d")), get(k, A, v("a")), get(k, B, v("b")),
          step("delete", key=k, ns=A, expect={"deleted": True}), get(k, A), get(k, None, v("d")), get(k, B, v("b")),
          step("delete", key=k, ns=A, expect={"deleted": False})])

    names = [("1B", "x"), ("1000B", "n" * 1000), ("utf8", "日本語🙂"), ("delim", "a/b:c"), ("nul", "a\0b"), ("space", " lead trail ")]
    for label, name in names:
        k = key("nm." + label)
        nb = nsb(name)
        case(cases, "ns.name." + label, f"namespace name {label}: round trip, invisible from the default namespace",
             [step("set", key=k, ns=nb, value=v("v-" + label), ttl=None), get(k, nb, v("v-" + label)), get(k, None)])
    k = key("nm.empty")
    case(cases, "ns.name.empty", "the empty namespace name (implementation-defined: equivalent to the default namespace?)",
         [step("set", key=k, ns="", value=v("e"), ttl=None, expect="any"), step("get", key=k, ns="", expect="any"), step("get", key=k, ns=None, expect="any"), {"op": "probe", "expect": {"ok": True}}])

    # namespace + key + value is bounded together
    nm = nsb("N" * 1000)
    k = key("bound")
    klen = len(base64.b64decode(k["b64"]))
    ok_len = MAX_SDK_BYTES - 1000 - klen
    case(cases, "ns.bound.max", "namespace 1000 B + key + value == the SDK bound: stored",
         [step("set", key=k, ns=nm, value={"len": ok_len}, ttl=None), get(k, nm, {"len": ok_len})])
    k2 = key("bound2")
    ok2 = MAX_SDK_BYTES - 1000 - len(base64.b64decode(k2["b64"])) + 1
    case(cases, "ns.bound.over", "one byte over the bound: rejected, connection still usable",
         [step("set", key=k2, ns=nm, value={"len": ok2}, ttl=None, expect="any"), step("get", key=k2, ns=nm, expect="any"), {"op": "probe", "expect": {"ok": True}}])

    k1, k2, k3, k4 = key("c1"), key("c2"), key("c3"), key("c4")
    case(cases, "ns.clear", "clear removes only that namespace; clearing an unknown namespace is harmless; a cleared namespace is writable",
         [step("set", key=k1, ns=A, value=v("1"), ttl=None), step("set", key=k2, ns=A, value=v("2"), ttl=None), step("set", key=k3, ns=B, value=v("3"), ttl=None), step("set", key=k4, ns=None, value=v("4"), ttl=None),
          step("clear", ns=A), get(k1, A), get(k2, A), get(k3, B, v("3")), get(k4, None, v("4")),
          step("clear", ns=nsb(f"{run}-never-used"), expect="any"),
          step("set", key=k1, ns=A, value=v("again"), ttl=None), get(k1, A, v("again"))])

    ks = [key(f"f{i}") for i in range(3)]
    case(cases, "ns.flush", "flush removes every namespace and the default one",
         [step("set", key=ks[0], ns=A, value=v("a"), ttl=None), step("set", key=ks[1], ns=B, value=v("b"), ttl=None), step("set", key=ks[2], ns=None, value=v("d"), ttl=None),
          step("flush"), get(ks[0], A), get(ks[1], B), get(ks[2], None),
          step("set", key=ks[0], ns=A, value=v("after"), ttl=None), get(ks[0], A, v("after"))])

    k = key("ttl")
    case(cases, "ns.ttl", "TTL inside a namespace: 1 s expires, no TTL stays",
         [step("set", key=k, ns=A, value=v("t"), ttl=1), step("set", key=key("ttl.keep"), ns=A, value=v("k"), ttl=None), {"op": "sleep", "ms": 2500, "expect": {"ok": True}}, get(k, A), get(key("ttl.keep"), A, v("k"))])

    k = key("big")
    case(cases, "ns.big", "a 64 KiB value in a namespace", [step("set", key=k, ns=A, value={"len": 65536}, ttl=None), get(k, A, {"len": 65536})])
    return cases


def build_ctr(run):
    key, nsb, step, case = _kit(run)
    cases = []
    A = nsb(f"{run}-ctr")
    v = lambda t: {"text": t}
    I64MAX, I64MIN = 2**63 - 1, -(2**63)
    for label, ns in (("", None), (".ns", A)):
        k = key("i.missing" + label)
        case(cases, "incr.missing" + label, "incr / decr on an absent key report a miss (not an error) and create nothing",
             [step("incr", key=k, ns=ns, delta=1, expect={"miss": True}), step("decr", key=k, ns=ns, delta=1, expect={"miss": True}), step("get", key=k, ns=ns, expect={"hit": False})])
        k = key("i.basic" + label)
        case(cases, "incr.basic" + label, "set 10, +5, -20, +0, -3 (negative delta): 15, -5, -5, -8; the stored text is the number",
             [step("set", key=k, ns=ns, value=v("10"), ttl=None), step("incr", key=k, ns=ns, delta=5, expect={"n": 15}), step("decr", key=k, ns=ns, delta=20, expect={"n": -5}),
              step("incr", key=k, ns=ns, delta=0, expect={"n": -5}), step("incr", key=k, ns=ns, delta=-3, expect={"n": -8}), step("get", key=k, ns=ns, expect={"hit": True, "value": v("-8")})])
    k = key("i.text")
    case(cases, "incr.nonnumeric", "a non-numeric value: error, stored value unchanged",
         [step("set", key=k, value=v("abc"), ttl=None, ns=None), step("incr", key=k, ns=None, delta=1, expect={"error": True}), step("get", key=k, ns=None, expect={"hit": True, "value": v("abc")}), {"op": "probe", "expect": {"ok": True}}])
    k = key("i.huge")
    big = "123456789012345678901234567890"
    case(cases, "incr.too_large", "a 30-digit value is not an i64: error, unchanged",
         [step("set", key=k, ns=None, value=v(big), ttl=None), step("incr", key=k, ns=None, delta=1, expect={"error": True}), step("get", key=k, ns=None, expect={"hit": True, "value": v(big)})])
    k = key("i.ovf")
    case(cases, "incr.overflow", "i64 max + 1 and i64 min - 1 are errors and leave the value; reaching i64 max exactly works",
         [step("set", key=k, ns=None, value=v(str(I64MAX - 1)), ttl=None), step("incr", key=k, ns=None, delta=1, expect={"n": I64MAX}), step("incr", key=k, ns=None, delta=1, expect={"error": True}),
          step("get", key=k, ns=None, expect={"hit": True, "value": v(str(I64MAX))}),
          step("set", key=k, ns=None, value=v(str(I64MIN + 1)), ttl=None), step("decr", key=k, ns=None, delta=1, expect={"n": I64MIN}), step("decr", key=k, ns=None, delta=1, expect={"error": True}),
          step("get", key=k, ns=None, expect={"hit": True, "value": v(str(I64MIN))})])
    k = key("i.delta")
    case(cases, "incr.delta_extremes", "delta of i64 max / i64 min on 0 (implementation-defined for the SDK, recorded)",
         [step("set", key=k, ns=None, value=v("0"), ttl=None), step("incr", key=k, ns=None, delta=I64MAX, expect="any"), step("incr", key=k, ns=None, delta=I64MIN, expect="any"), step("get", key=k, ns=None, expect="any")])
    for i, text in enumerate(["007", " 5", "5 ", "+5", "1e3", "0x10", "-0", "5\n", "٣"]):
        k = key(f"i.fmt{i}")
        case(cases, f"incr.format.{i}", f"value {text!r}: is it a counter? (the server decides; recorded and compared across SDKs)",
             [step("set", key=k, ns=None, value=v(text), ttl=None), step("incr", key=k, ns=None, delta=1, expect="any"), step("get", key=k, ns=None, expect="any")])
    k = key("i.ttl")
    case(cases, "incr.ttl", "an increment on an entry with TTL 3 made at t=2.2 s: still there at t=3.7 s? (keeps / refreshes / clears the TTL; recorded)",
         [step("set", key=k, ns=None, value=v("1"), ttl=3), {"op": "sleep", "ms": 2200, "expect": {"ok": True}}, step("incr", key=k, ns=None, delta=1, expect={"n": 2}), {"op": "sleep", "ms": 1500, "expect": {"ok": True}}, step("get", key=k, ns=None, expect="any")])
    k = key("i.par")
    case(cases, "incr.parallel", "8 concurrent workers x 50 increments on one key: exactly 400",
         [step("set", key=k, ns=None, value=v("0"), ttl=None), step("incr_par", key=k, ns=None, w=8, m=50, expect={"n": 400}), step("get", key=k, ns=None, expect={"hit": True, "value": v("400")})])

    # compare-and-set -------------------------------------------------------
    for label, ns in (("", None), (".ns", A)):
        k = key("cas.basic" + label)
        va, vb, vc = v("one"), v("two"), v("three")
        case(cases, "cas.basic" + label, "get_with_token -> replace succeeds, replaying the old token fails; the token is the first 16 bytes of the value's SHA-256, hex",
             [step("set", key=k, ns=ns, value=va, ttl=None), step("cas_get", key=k, ns=ns, reg="t", expect={"hit": True, "value": va, "token": True}),
              step("cas_replace", key=k, ns=ns, reg="t", value=vb, ttl=None, expect={"result": True}), step("get", key=k, ns=ns, expect={"hit": True, "value": vb}),
              step("cas_replace", key=k, ns=ns, reg="t", value=vc, ttl=None, expect={"result": False}), step("get", key=k, ns=ns, expect={"hit": True, "value": vb})])
        k = key("cas.cond" + label)
        case(cases, "cas.conditions" + label, "put_if_absent / replace_if_present / delete_if_matches on absent and present keys",
             [step("replace_if_present", key=k, ns=ns, value=v("x"), ttl=None, expect={"result": False}), step("put_if_absent", key=k, ns=ns, value=v("first"), ttl=None, expect={"result": True}),
              step("put_if_absent", key=k, ns=ns, value=v("second"), ttl=None, expect={"result": False}), step("get", key=k, ns=ns, expect={"hit": True, "value": v("first")}),
              step("replace_if_present", key=k, ns=ns, value=v("third"), ttl=None, expect={"result": True}), step("get", key=k, ns=ns, expect={"hit": True, "value": v("third")}),
              step("cas_replace", key=k, ns=ns, reg="lit:00000000000000000000000000000000", value=v("no"), ttl=None, expect={"result": False}),
              step("cas_delete", key=k, ns=ns, reg="lit:00000000000000000000000000000000", expect={"result": False}),
              step("cas_get", key=k, ns=ns, reg="t", expect={"hit": True, "value": v("third"), "token": True}), step("cas_delete", key=k, ns=ns, reg="t", expect={"result": True}),
              step("get", key=k, ns=ns, expect={"hit": False}), step("cas_delete", key=k, ns=ns, reg="t", expect={"result": False})])
    k = key("cas.missing")
    case(cases, "cas.get_missing", "get_with_token on an absent key reports a miss; replace on an absent key is false",
         [step("cas_get", key=k, ns=None, reg="t", expect={"hit": False}), step("cas_replace", key=k, ns=None, reg="lit:00000000000000000000000000000000", value=v("x"), ttl=None, expect={"result": False})])
    k = key("cas.aba")
    case(cases, "cas.aba", "the value is changed and changed back between the read and the replace (the token is a digest of the bytes, so the replace may succeed; recorded)",
         [step("set", key=k, ns=None, value=v("A"), ttl=None), step("cas_get", key=k, ns=None, reg="t", expect={"hit": True, "value": v("A"), "token": True}),
          step("set", key=k, ns=None, value=v("B"), ttl=None), step("set", key=k, ns=None, value=v("A"), ttl=None), step("cas_replace", key=k, ns=None, reg="t", value=v("C"), ttl=None, expect="any"), step("get", key=k, ns=None, expect="any")])
    k = key("cas.race")
    case(cases, "cas.race", "8 workers replace on one key with the same token at once: exactly one wins",
         [step("set", key=k, ns=None, value=v("v0"), ttl=None), step("cas_race", key=k, ns=None, w=8, expect={"n": 1})])
    k = key("cas.ttl")
    case(cases, "cas.ttl", "a successful replace with ttl 1 expires; without a ttl the entry becomes permanent",
         [step("set", key=k, ns=None, value=v("a"), ttl=None), step("cas_get", key=k, ns=None, reg="t", expect={"hit": True, "value": v("a"), "token": True}), step("cas_replace", key=k, ns=None, reg="t", value=v("b"), ttl=1, expect={"result": True}),
          {"op": "sleep", "ms": 2500, "expect": {"ok": True}}, step("get", key=k, ns=None, expect={"hit": False})])
    return cases


def build_many(run):
    key, nsb, step, case = _kit(run)
    cases = []
    A = nsb(f"{run}-many")
    kb = lambda name: b64(f"{run}:{name}".encode())

    def items(names, vals):
        return {kb(n): hashlib.sha256(vals[n]).hexdigest() for n in names}

    for label, ns in (("", None), (".ns", A)):
        names = [f"m{i}" for i in range(5)]
        vals = {n: f"value-{n}".encode() * (i + 1) for i, n in enumerate(names)}
        miss = ["gone1", "gone2"]
        case(cases, "many.basic" + label, "set_many 5 keys; get_many those plus 2 absent: the 5 hits, the misses simply absent",
             [step("many_set", kv=[(kb(n), {"b64": b64(vals[n])}) for n in names], ns=ns, ttl=None),
              step("many_get", keys=[kb(n) for n in names] + [kb(m) for m in miss], ns=ns, expect={"items": items(names, vals)})])
    names = ["d1", "d2"]
    vals = {n: n.encode() for n in names}
    case(cases, "many.duplicates", "duplicate keys in the request (recorded; the result is a map)",
         [step("many_set", kv=[(kb(n), {"b64": b64(vals[n])}) for n in names], ns=None, ttl=None), step("many_get", keys=[kb("d1"), kb("d1"), kb("d2")], ns=None, expect="any")])
    case(cases, "many.single", "a batch of one key",
         [step("many_set", kv=[(kb("s1"), {"text": "solo"})], ns=None, ttl=None), step("many_get", keys=[kb("s1")], ns=None, expect={"items": {kb("s1"): hashlib.sha256(b"solo").hexdigest()}})])
    case(cases, "many.empty", "an empty batch (implementation-defined: error or empty result), connection still usable",
         [step("many_get", keys=[], ns=None, expect="any"), step("many_set", kv=[], ns=None, ttl=None, expect="any"), {"op": "probe", "expect": {"ok": True}}])
    case(cases, "many.empty_key", "a batch containing the empty key (rejected as a single get would be)",
         [step("many_get", keys=[kb("ek"), "="], ns=None, expect="any"), {"op": "probe", "expect": {"ok": True}}])
    for n, vlen in [(100, 64), (1000, 64), (10000, 64)]:
        pre = f"{run}:g{n}:"
        pb = b64(pre.encode())
        agg = hashlib.sha256()
        for key_ in sorted(pre.encode() + str(i).encode() for i in range(n)):
            i = int(key_[len(pre):])
            agg.update(key_ + b"\0" + hashlib.sha256(gen_value(i, vlen)).hexdigest().encode())
        case(cases, f"many.gen.{n}", f"set_many then get_many of {n} keys with 64 B values: all present and byte-identical",
             [step("many_set_gen", prefix=pb, n=n, vlen=vlen, ns=None, ttl=None), step("many_get_gen", prefix=pb, n=n, ns=None, expect={"count": n, "checksum": agg.hexdigest()})])
    # sizes: more than one request worth, below and above the cumulative reply bound (16 MiB)
    for n, vlen, tag, expect in [(20, 100_000, "2MB", None), (12, 1_000_000, "12MB", None), (17, 1_000_000, "17MB", "any")]:
        pre = f"{run}:big{tag}:"
        pb = b64(pre.encode())
        agg = hashlib.sha256()
        for key_ in sorted(pre.encode() + str(i).encode() for i in range(n)):
            i = int(key_[len(pre):])
            agg.update(key_ + b"\0" + hashlib.sha256(gen_value(i, vlen)).hexdigest().encode())
        steps = [step("many_set_gen", prefix=pb, n=n, vlen=vlen, ns=None, ttl=None),
                 step("many_get_gen", prefix=pb, n=n, ns=None, expect=expect or {"count": n, "checksum": agg.hexdigest()})]
        if expect == "any":
            steps.append({"op": "probe", "expect": {"ok": True}})
        case(cases, f"many.big.{tag}", f"{n} values of {vlen} B ({tag} in total): spans several requests" + ("; above the 16 MiB reply bound (error or partial, recorded)" if expect else ""), steps)
    pre = f"{run}:ttl:"
    case(cases, "many.ttl", "set_many with ttl 1 expires, with no ttl stays",
         [step("many_set", kv=[(kb("t1"), {"text": "x"})], ns=None, ttl=1), step("many_set", kv=[(kb("t2"), {"text": "y"})], ns=None, ttl=None), {"op": "sleep", "ms": 2500, "expect": {"ok": True}},
          step("many_get", keys=[kb("t1"), kb("t2")], ns=None, expect={"items": {kb("t2"): hashlib.sha256(b"y").hexdigest()}})])
    names = [f"e{i}" for i in range(50)]
    vals = {n: n.encode() * 3 for n in names}
    case(cases, "many.equivalence", "50 keys written by set_many are read back one by one with get",
         [step("many_set", kv=[(kb(n), {"b64": b64(vals[n])}) for n in names], ns=None, ttl=None)] +
         [step("get", key={"b64": kb(n)}, ns=None, expect={"hit": True, "value": {"b64": b64(vals[n])}}) for n in names])
    return cases


GROUPS = {"ns": build_ns, "ctr": build_ctr, "many": build_many}

def build_rep(run, K=30):
    """Cases for the replica-convergence verification: one SDK, its own keys (the run id carries the SDK)."""
    key, nsb, step, case = _kit(run)
    cases = []
    v = lambda t: {"text": t}
    any_get = lambda k: step("get", key=k, ns=None, expect="any")

    k = key("serial_set")
    case(cases, "rep.serial_set", "50 sequential sets on one key", [step("set", key=k, ns=None, value=v(f"s{i}"), ttl=None) for i in range(50)] + [step("get", key=k, ns=None, expect={"hit": True, "value": v("s49")})])
    steps = []
    for r in range(K):
        k = key(f"set_race{r}")
        steps += [step("set", key=k, ns=None, value=v("init"), ttl=None), step("set_par", key=k, ns=None, w=8, m=25, ttl=None), any_get(k)]
    case(cases, "rep.set_race", f"{K} keys, each: 8 concurrent writers x 25 sets with distinct values (the last arrival is free, the owners must agree)", steps)
    k = key("incr_serial")
    case(cases, "rep.incr_serial", "200 sequential increments", [step("set", key=k, ns=None, value=v("0"), ttl=None)] + [step("incr", key=k, ns=None, delta=1, expect={"n": i + 1}) for i in range(200)] + [step("get", key=k, ns=None, expect={"hit": True, "value": v("200")})])
    steps = []
    for r in range(K):
        k = key(f"incr_par{r}")
        steps += [step("set", key=k, ns=None, value=v("0"), ttl=None), step("incr_par", key=k, ns=None, w=8, m=50, expect={"n": 400}), step("get", key=k, ns=None, expect={"hit": True, "value": v("400")})]
    case(cases, "rep.incr_par", f"{K} counters, each: 8 concurrent workers x 50 increments", steps)
    steps = []
    for r in range(K):
        k = key(f"cas_race{r}")
        steps += [step("set", key=k, ns=None, value=v("v0"), ttl=None), step("cas_race", key=k, ns=None, w=8, expect={"n": 1}), any_get(k)]
    case(cases, "rep.cas_race", f"{K} keys, each: 8 concurrent replaces with one token", steps)
    k = key("cas_chain")
    steps = [step("set", key=k, ns=None, value=v("0"), ttl=None)]
    for i in range(1, 21):
        steps += [step("cas_get", key=k, ns=None, reg="t", expect="any"), step("cas_replace", key=k, ns=None, reg="t", value=v(str(i)), ttl=None, expect={"result": True})]
    steps.append(step("get", key=k, ns=None, expect={"hit": True, "value": v("20")}))
    case(cases, "rep.cas_chain", "20 sequential compare-and-set replaces", steps)
    k = key("delete")
    case(cases, "rep.delete", "set then delete: absent on every owner", [step("set", key=k, ns=None, value=v("x"), ttl=None), step("delete", key=k, ns=None, expect={"deleted": True}), step("get", key=k, ns=None, expect={"hit": False})])
    k = key("del_resurrect")
    case(cases, "rep.del_resurrect", "set, delete, set again: the new value everywhere", [step("set", key=k, ns=None, value=v("a"), ttl=None), step("delete", key=k, ns=None, expect={"deleted": True}), step("set", key=k, ns=None, value=v("b"), ttl=None), step("get", key=k, ns=None, expect={"hit": True, "value": v("b")})])
    steps = []
    for r in range(K):
        k = key(f"setdel_race{r}")
        steps += [step("set", key=k, ns=None, value=v("init"), ttl=None), step("setdel_par", key=k, ns=None, w=8, m=25, ttl=None), any_get(k)]
    case(cases, "rep.setdel_race", f"{K} keys, each: 4 writers set and 4 writers delete concurrently", steps)
    pre = f"{run}:many:"
    pb = b64(pre.encode())
    agg = hashlib.sha256()
    for key_ in sorted(pre.encode() + str(i).encode() for i in range(100)):
        i = int(key_[len(pre):])
        agg.update(key_ + b"\0" + hashlib.sha256(gen_value(i, 64)).hexdigest().encode())
    case(cases, "rep.many", "set_many of 100 keys", [step("many_set_gen", prefix=pb, n=100, vlen=64, ns=None, ttl=None), step("many_get_gen", prefix=pb, n=100, ns=None, expect={"count": 100, "checksum": agg.hexdigest()})])
    k = key("ttl_set")
    case(cases, "rep.ttl_set", "set with TTL 300 (numeric value so that a delta-0 incr can read the remaining TTL)", [step("set", key=k, ns=None, value=v("42"), ttl=300), any_get(k)])
    k = key("ttl_incr")
    case(cases, "rep.ttl_incr", "an increment on an entry with TTL 300 keeps it", [step("set", key=k, ns=None, value=v("10"), ttl=300), step("incr", key=k, ns=None, delta=5, expect={"n": 15}), any_get(k)])
    k = key("ttl_cas")
    case(cases, "rep.ttl_cas", "a CAS replace with TTL 300", [step("set", key=k, ns=None, value=v("1"), ttl=300), step("cas_get", key=k, ns=None, reg="t", expect="any"), step("cas_replace", key=k, ns=None, reg="t", value=v("2"), ttl=300, expect={"result": True}), any_get(k)])
    return cases


def build_repmix_init(run, K=30):
    """Keys the six-SDK parallel run starts from (INCR never creates a key; a CAS needs one)."""
    key, nsb, step, case = _kit(run)
    cases = []
    steps = []
    for r in range(K):
        steps += [step("set", key=key(f"incr{r}"), ns=None, value={"text": "0"}, ttl=None), step("set", key=key(f"cas{r}"), ns=None, value={"text": "v0"}, ttl=None)]
    case(cases, "mix.init", f"{K} counters at 0 and {K} CAS keys", steps)
    return cases


def build_repmix(run, K=30):
    """Run by all six SDKs at once on the SAME keys."""
    key, nsb, step, case = _kit(run)
    cases = []
    any_get = lambda k: step("get", key=k, ns=None, expect="any")
    for kind, title, mk in [
        ("set", "every SDK: 8 workers x 25 sets with its own distinct values", lambda k: [step("set_par", key=k, ns=None, w=8, m=25, ttl=None), any_get(k)]),
        ("incr", "every SDK: 8 workers x 50 increments on one counter", lambda k: [step("incr_par", key=k, ns=None, w=8, m=50, expect="any"), any_get(k)]),
        ("cas", "every SDK: 8 concurrent replaces with the token it read", lambda k: [step("cas_race", key=k, ns=None, w=8, expect="any"), any_get(k)]),
        ("setdel", "every SDK: 4 setters and 4 deleters", lambda k: [step("setdel_par", key=k, ns=None, w=8, m=25, ttl=None), any_get(k)]),
    ]:
        steps = []
        for r in range(K):
            steps += mk(key(f"{kind}{r}"))
        case(cases, f"mix.{kind}", f"{K} shared keys; {title}", steps)
    return cases


GROUPS.update({"rep": build_rep, "repmix_init": build_repmix_init, "repmix": build_repmix})


def tsv(doc):
    """The same cases as tab-separated lines, for drivers without a JSON library.

    S <case> <step> <op> <key-b64|-> <vkind> <varg> <ttl|-> <ms>
        vkind: b64 (varg = base64 bytes) | len (varg = N) | -
    X <name> <key-template-b64> <vkind> <varg> <ttl|->    (cross-SDK items)
    """
    def value(spec):
        if spec is None:
            return "-", "-"
        if "len" in spec:
            return "len", str(spec["len"])
        return "b64", b64(value_bytes(spec))

    lines = []
    for case in doc["cases"]:
        for index, step in enumerate(case["steps"]):
            vkind, varg = value(step.get("value"))
            ttl = step.get("ttl")
            ttl = "-" if ttl is None else (json.dumps(ttl) if not isinstance(ttl, str) else ttl)
            key = step["key"]["b64"] if "key" in step else "-"
            extra = []
            if step.get("ns") is not None:
                extra.append("ns=" + (step["ns"] or "="))
            for field in ("delta", "reg", "w", "m", "n", "vlen", "prefix", "tokenreg"):
                if field in step:
                    extra.append(f"{field}={step[field]}")
            if "keys" in step:
                extra.append("keys=" + ",".join(k or "=" for k in step["keys"]))
            if "kv" in step:
                extra.append("kv=" + ",".join(f"{k}:{':'.join(value(v))}" for k, v in step["kv"]))
            lines.append("\t".join(["S", case["id"], str(index), step["op"], key or "=", vkind, varg, ttl, str(step.get("ms", 0)), ";".join(extra)]))
    for item in doc["xsdk"]:
        vkind, varg = value(item["value"])
        ttl = "-" if item["ttl"] is None else str(item["ttl"])
        lines.append("\t".join(["X", item["name"], b64(item["key_template"].encode()), vkind, varg, ttl]))
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    group = sys.argv[sys.argv.index("--group") + 1] if "--group" in sys.argv else "basic"
    doc = build(sys.argv[1] if len(sys.argv) > 1 else "r0", group)
    if "--tsv" in sys.argv:
        sys.stdout.write(tsv(doc))
    else:
        json.dump(doc, sys.stdout, separators=(",", ":"))
