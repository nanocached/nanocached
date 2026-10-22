// Scenario driver for the Java SDK (churn, connection checks, lifecycle).
// java Scenario <scenario> [args]; same scenarios and JSON events as drivers/python/scenario.py.
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import org.nanocached.NanocachedClient;
import org.nanocached.NanocachedClient.Address;

public final class Scenario {
    static final String SDK = "java";
    static final long T0 = System.currentTimeMillis();
    static final long LIMIT_MS = (long) (Double.parseDouble(System.getenv().getOrDefault("CONN_TIMEOUT_S", "60")) * 1000);
    static final ExecutorService POOL = Executors.newCachedThreadPool(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
    static NanocachedClient.Options options;

    static String esc(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (char ch : s.toCharArray()) {
            if (ch == '"' || ch == '\\') b.append('\\').append(ch);
            else if (ch < 0x20) b.append(String.format("\\u%04x", (int) ch));
            else b.append(ch);
        }
        return b.append('"').toString();
    }

    static String json(Object v) {
        if (v == null) return "null";
        if (v instanceof String s) return esc(s);
        if (v instanceof Map<?, ?> m) {
            StringBuilder b = new StringBuilder("{");
            for (var e : m.entrySet()) { if (b.length() > 1) b.append(','); b.append(esc(String.valueOf(e.getKey()))).append(':').append(json(e.getValue())); }
            return b.append('}').toString();
        }
        return String.valueOf(v);
    }

    static Map<String, Object> ev(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    static void emit(Map<String, Object> m) { m.put("sdk", SDK); m.put("t_ms", System.currentTimeMillis() - T0); System.out.println(json(m)); }

    static Map<String, Object> err(Throwable e) {
        while (e instanceof ExecutionException && e.getCause() != null) e = e.getCause();
        String msg = String.valueOf(e.getMessage());
        return ev("outcome", "error", "error_class", e.getClass().getSimpleName(), "error_message", msg.length() > 300 ? msg.substring(0, 300) : msg);
    }

    static NanocachedClient connect() { return NanocachedClient.connect(options); }

    interface Call<T> { T run() throws Exception; }

    /** Runs c with a bound; returns the value, or null after emitting the failure event. */
    static <T> T step(String name, long ms, Call<T> c) {
        long t = System.currentTimeMillis();
        Future<T> f = POOL.submit(c::run);
        try { T r = f.get(ms, TimeUnit.MILLISECONDS); emit(ev("event", name, "outcome", "ok", "ms", System.currentTimeMillis() - t)); return r; }
        catch (TimeoutException e) { f.cancel(true); emit(ev("event", name, "outcome", "timeout", "ms", System.currentTimeMillis() - t)); }
        catch (Throwable e) { Map<String, Object> m = err(e); m.put("event", name); m.put("ms", System.currentTimeMillis() - t); emit(m); }
        return null;
    }

    static String stats() throws Exception {
        String st = Files.readString(Paths.get("/proc/self/status"));
        int threads = 0, rss = 0;
        for (String l : st.split("\n")) { if (l.startsWith("Threads:")) threads = Integer.parseInt(l.split("\\s+")[1]); if (l.startsWith("VmRSS:")) rss = Integer.parseInt(l.split("\\s+")[1]); }
        return json(ev("fds", Files.list(Paths.get("/proc/self/fd")).count(), "threads", threads, "rss_kb", rss));
    }

    static Object cgroupPeakKb() {
        try { return Long.parseLong(Files.readString(Paths.get("/sys/fs/cgroup/memory.peak")).trim()) / 1024; } catch (Exception e) { return null; }
    }

    /** One request against a hostile fake node (fixtures/hostile_cases.json); reports the outcome, the wall time and the container's peak memory. */
    static void host(String kase, List<Address> addrs, String[] a) throws Exception {
        NanocachedClient.Options o = NanocachedClient.builder().addresses(addrs).compress(a.length > 0 && a[0].equals("compress"));
        NanocachedClient c = step("connect", LIMIT_MS, () -> NanocachedClient.connect(o));
        if (c == null) { emit(ev("event", "peak", "cgroup_peak_kb", cgroupPeakKb())); return; }
        long t = System.currentTimeMillis();
        if (kase.equals("backpressure")) {
            int n = Integer.parseInt(a[0]);
            byte[] value = new byte[Integer.parseInt(a[1])]; // one buffer, owned by the caller and shared by every set
            List<Future<?>> calls = new ArrayList<>();
            for (int i = 0; i < n; i++) { final int k = i; calls.add(POOL.submit(() -> { c.set(("host:bp:" + k).getBytes(), value); return null; })); }
            int failed = 0; boolean timedOut = false;
            for (Future<?> f : calls) {
                try { f.get(Math.max(1, LIMIT_MS - (System.currentTimeMillis() - t)), TimeUnit.MILLISECONDS); }
                catch (TimeoutException e) { timedOut = true; break; }
                catch (Throwable e) { failed++; }
            }
            if (timedOut) emit(ev("event", "op", "outcome", "timeout", "ms", System.currentTimeMillis() - t));
            else emit(ev("event", "op", "outcome", "settled", "ok", n - failed, "failed", failed, "ms", System.currentTimeMillis() - t));
        } else {
            Future<java.util.Optional<byte[]>> f = POOL.submit(() -> c.getBytes("host:k"));
            try {
                var got = f.get(LIMIT_MS, TimeUnit.MILLISECONDS);
                emit(ev("event", "op", "outcome", "ok", "len", got.map(v -> v.length).orElse(null), "ms", System.currentTimeMillis() - t));
            } catch (TimeoutException e) { emit(ev("event", "op", "outcome", "timeout", "ms", System.currentTimeMillis() - t)); }
            catch (Throwable e) { Map<String, Object> m = err(e); m.put("event", "op"); m.put("ms", System.currentTimeMillis() - t); emit(m); }
        }
        emit(ev("event", "peak", "cgroup_peak_kb", cgroupPeakKb()));
        step("close", 10_000, () -> { c.close(); return true; });
        System.exit(0); // a stalled socket would keep a non-daemon thread alive after the case is judged
    }

    public static void main(String[] args) throws Exception {
        List<Address> addrs = new ArrayList<>();
        for (String part : System.getenv("NANOTEST_ADDRESSES").split(",")) { int i = part.lastIndexOf(':'); addrs.add(new Address(part.substring(0, i), Integer.parseInt(part.substring(i + 1)))); }
        options = NanocachedClient.builder().addresses(addrs).viaProxy("1".equals(System.getenv("NANOTEST_VIA_PROXY")));
        String secret = System.getenv("NANOTEST_SECRET");
        if (secret != null && !secret.isEmpty()) options = options.authSecret(secret);
        String name = args[0];
        if (name.equals("host")) { host(args[1], addrs, java.util.Arrays.copyOfRange(args, 2, args.length)); return; }
        switch (name) {
            case "conn": {
                NanocachedClient c = step("connect", LIMIT_MS, Scenario::connect);
                if (c == null) break;
                step("set", LIMIT_MS, () -> { c.set("conn:k", "v"); return true; });
                var got = step("get", LIMIT_MS, () -> c.get("conn:k").orElse(null));
                emit(ev("event", "get_value", "value", got));
                emit(ev("event", "describe", "text", options + " | " + c));
                step("close", LIMIT_MS, () -> { c.close(); return true; });
                break;
            }
            case "churn": {
                double secs = Double.parseDouble(args[1]); String label = args[2];
                NanocachedClient c = connect();
                int[] acked = new int[200]; int ops = 0, errors = 0, stale = 0, miss = 0, corrupt = 0; Long first = null, last = null;
                Map<String, Object> classes = new LinkedHashMap<>();
                long end = System.currentTimeMillis() + (long) (secs * 1000);
                for (int i = 1; System.currentTimeMillis() < end; i++) {
                    int n = (i - 1) % 200; String key = "ch:" + label + ":" + SDK + ":" + n; ops++;
                    try {
                        if (i % 2 == 1) { int v = acked[n] + 1; Future<?> f = POOL.submit(() -> c.set(key, "v" + v)); f.get(10, TimeUnit.SECONDS); acked[n] = v; }
                        else {
                            Future<java.util.Optional<String>> f = POOL.submit(() -> c.get(key));
                            var got = f.get(10, TimeUnit.SECONDS);
                            if (acked[n] > 0) { if (got.isEmpty()) miss++; else if (!got.get().startsWith("v")) corrupt++; else if (Integer.parseInt(got.get().substring(1)) < acked[n]) stale++; }
                        }
                    } catch (Throwable e) {
                        errors++; Throwable r = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
                        classes.merge(r.getClass().getSimpleName(), 1, (x, y) -> (Integer) x + 1);
                        long now = System.currentTimeMillis() - T0; if (first == null) first = now; last = now;
                    }
                    Thread.sleep(5);
                }
                emit(ev("event", "summary", "ops", ops, "errors", errors, "stale", stale, "miss", miss, "corrupt", corrupt, "classes", classes, "first_err_ms", first, "last_err_ms", last));
                c.close();
                break;
            }
            case "life": life(args[1], args.length > 2 ? args[2] : null); break;
        }
        if (!(args.length > 1 && args[1].equals("exit_noclose"))) System.exit(0);
    }

    static void life(String test, String a) throws Exception {
        switch (test) {
            case "close_inflight": {
                NanocachedClient c = connect();
                c.set("life:big", "b".repeat(900000));
                List<Future<Integer>> pend = new ArrayList<>();
                for (int i = 0; i < 4; i++) pend.add(POOL.submit(() -> c.getBytes("life:big").map(b -> b.length).orElse(-1)));
                Thread.sleep(200);
                long t = System.currentTimeMillis(); step("close", LIMIT_MS, () -> { c.close(); return true; }); emit(ev("event", "close_ms", "ms", System.currentTimeMillis() - t));
                for (int i = 0; i < pend.size(); i++) {
                    try { emit(ev("event", "pending", "i", i, "outcome", "ok", "len", pend.get(i).get(20, TimeUnit.SECONDS))); }
                    catch (TimeoutException e) { emit(ev("event", "pending", "i", i, "outcome", "timeout")); }
                    catch (Throwable e) { Map<String, Object> m = err(e); m.put("event", "pending"); m.put("i", i); emit(m); }
                }
                break;
            }
            case "after_close": {
                NanocachedClient c = connect(); c.set("life:k", "v"); c.close();
                step("after_close.get", LIMIT_MS, () -> c.get("life:k"));
                step("after_close.set", LIMIT_MS, () -> { c.set("life:k", "v"); return true; });
                step("after_close.delete", LIMIT_MS, () -> c.delete("life:k"));
                step("after_close.incr", LIMIT_MS, () -> c.incr("life:k", 1));
                step("after_close.get_many", LIMIT_MS, () -> c.getMany(List.of("life:k")));
                break;
            }
            case "double_close": case "concurrent_close": {
                NanocachedClient c = connect();
                step("close1", LIMIT_MS, () -> { c.close(); return true; });
                step("close2", LIMIT_MS, () -> { c.close(); return true; });
                NanocachedClient d = connect();
                step("concurrent_close", LIMIT_MS, () -> {
                    List<Future<?>> fs = new ArrayList<>(); for (int i = 0; i < 3; i++) fs.add(POOL.submit(d::close));
                    for (Future<?> f : fs) f.get(); return true; });
                break;
            }
            case "abandon": {
                NanocachedClient c = connect();
                c.set("life:a", "value-a"); c.set("life:b", "value-b");
                Future<?> pending = POOL.submit(() -> c.get("life:a"));
                Thread.sleep(300); pending.cancel(true); // thread interrupt: the closest Java has to cancelling the call
                emit(ev("event", "cancelled"));
                Map<String, String> r = new LinkedHashMap<>();
                for (String[] kv : new String[][]{{"b", "life:b"}, {"a", "life:a"}}) {
                    Future<String> f = POOL.submit(() -> c.get(kv[1]).orElse(null));
                    try { r.put(kv[0], f.get(30, TimeUnit.SECONDS)); }
                    catch (TimeoutException e) { r.put(kv[0], "timeout"); }
                    catch (Throwable e) { Throwable x = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e; r.put(kv[0], x.getClass().getSimpleName() + ": " + x.getMessage()); }
                }
                emit(ev("event", "after_cancel", "b", r.get("b"), "a", r.get("a"), "ok", "value-b".equals(r.get("b")) && "value-a".equals(r.get("a"))));
                c.close();
                break;
            }
            case "cycles": {
                int n = Integer.parseInt(a); String base = stats();
                for (int i = 0; i < n; i++) { NanocachedClient c = connect(); c.set("life:c", "v"); c.close(); }
                Thread.sleep(1000);
                System.out.println("{\"sdk\":\"java\",\"event\":\"cycles\",\"n\":" + n + ",\"before\":" + base + ",\"after\":" + stats() + ",\"t_ms\":" + (System.currentTimeMillis() - T0) + "}");
                break;
            }
            case "reconnect": {
                NanocachedClient c = connect(); long end = System.currentTimeMillis() + (long) (Double.parseDouble(a) * 1000);
                int ok = 0, fail = 0; Long ff = null, lf = null, fo = null;
                while (System.currentTimeMillis() < end) {
                    try { POOL.submit(() -> c.getBytes("life:r")).get(10, TimeUnit.SECONDS); ok++; if (ff != null && fo == null) fo = System.currentTimeMillis() - T0; }
                    catch (Throwable e) { fail++; long now = System.currentTimeMillis() - T0; if (ff == null) ff = now; lf = now; }
                    Thread.sleep(100);
                }
                emit(ev("event", "reconnect", "ok", ok, "fail", fail, "first_fail_ms", ff, "last_fail_ms", lf, "first_ok_after_ms", fo));
                c.close();
                break;
            }
            case "storm": {
                NanocachedClient c = connect(); long end = System.currentTimeMillis() + (long) (Double.parseDouble(a) * 1000); int attempts = 0;
                while (System.currentTimeMillis() < end) { attempts++; try { POOL.submit(() -> c.getBytes("life:s")).get(5, TimeUnit.SECONDS); } catch (Throwable e) { } Thread.sleep(50); }
                emit(ev("event", "storm", "attempts", attempts)); c.close();
                break;
            }
            case "exit_noclose": {
                NanocachedClient c = connect(); c.set("life:e", "v"); emit(ev("event", "exiting_without_close"));
                // returning from main without close(): System.exit is NOT called by this path in the caller below, see main
                break;
            }
        }
    }
}
