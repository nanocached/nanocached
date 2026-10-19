// Conformance driver for the Java SDK: java Driver <cases.tsv> <phase> <path> <topology>
// One JSON line per step on stdout (case format: tools/conformance/make_cases.py).
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.HashMap;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.nanocached.NanocachedClient;
import org.nanocached.NanocachedClient.Address;

public final class Driver {
    static final String SDK = "java";
    static final String[] ALL = {"python", "typescript", "go", "java", "dotnet", "rust"};
    static final long STEP_TIMEOUT_S = 10;
    static String path, topology;

    record Step(String c, int i, String op, String key, String vkind, String varg, String ttl, int ms, Map<String, String> x) {}
    record Item(String name, String tmpl, String vkind, String varg, String ttl) {}

    static Map<String, String> parseX(String x) {
        Map<String, String> m = new HashMap<>();
        for (String kv : x.split(";")) { int i = kv.indexOf('='); if (i > 0) m.put(kv.substring(0, i), kv.substring(i + 1)); }
        return m;
    }

    static String esc(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (char ch : s.toCharArray()) {
            if (ch == '"' || ch == '\\') b.append('\\').append(ch);
            else if (ch < 0x20) b.append(String.format("\\u%04x", (int) ch));
            else b.append(ch);
        }
        return b.append('"').toString();
    }

    static void emit(String c, int step, String op, Map<String, Object> r) {
        Map<String, Object> all = new LinkedHashMap<>();
        all.put("case", c); all.put("step", step); all.put("op", op);
        all.putAll(r);
        all.put("sdk", SDK); all.put("path", path); all.put("topology", topology);
        StringBuilder b = new StringBuilder("{");
        for (var e : all.entrySet()) {
            if (b.length() > 1) b.append(',');
            b.append(esc(e.getKey())).append(':');
            Object v = e.getValue();
            b.append(v instanceof String s ? esc(s) : String.valueOf(v));
        }
        System.out.println(b.append('}'));
    }

    static byte[] value(String kind, String arg) {
        if (kind.equals("len")) {
            byte[] b = new byte[Integer.parseInt(arg)];
            for (int i = 0; i < b.length; i++) b[i] = (byte) (i % 251);
            return b;
        }
        return Base64.getDecoder().decode(arg);
    }

    static byte[] keyOf(String b64) { return b64.equals("=") ? new byte[0] : Base64.getDecoder().decode(b64); }

    static Map<String, Object> hit(byte[] v) throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("outcome", "ok");
        if (v == null) { m.put("hit", false); return m; }
        m.put("hit", true); m.put("len", v.length);
        m.put("sha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(v)));
        return m;
    }

    static Map<String, Object> ok() { Map<String, Object> m = new LinkedHashMap<>(); m.put("outcome", "ok"); return m; }

    static final Map<String, String> REGS = new HashMap<>();

    static Object call(Object target, String name, Class<?>[] types, Object... args) throws Throwable {
        try { return target.getClass().getMethod(name, types).invoke(target, args); }
        catch (InvocationTargetException e) { throw e.getCause(); }
    }

    static byte[] b64d(String b) { return b.equals("=") || b.isEmpty() ? new byte[0] : Base64.getDecoder().decode(b); }
    static String latin(String b64) { return new String(b64d(b64), StandardCharsets.ISO_8859_1); }
    static byte[] genValue(int i, int n) { byte[] b = new byte[n]; for (int j = 0; j < n; j++) b[j] = (byte) ((i + j) % 251); return b; }
    static String hex(byte[] v) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(v)); }

    static Map<String, Object> many(Map<String, byte[]> found, boolean aggregate) throws Exception {
        TreeMap<String, byte[]> sorted = new TreeMap<>(found); // keys are ASCII here, so String order == byte order
        MessageDigest agg = MessageDigest.getInstance("SHA-256");
        List<List<String>> items = new ArrayList<>();
        for (var e : sorted.entrySet()) {
            String h = hex(e.getValue());
            items.add(List.of(Base64.getEncoder().encodeToString(e.getKey().getBytes(StandardCharsets.ISO_8859_1)), h));
            agg.update(e.getKey().getBytes(StandardCharsets.ISO_8859_1)); agg.update((byte) 0); agg.update(h.getBytes(StandardCharsets.US_ASCII));
        }
        Map<String, Object> m = ok();
        if (aggregate) { m.put("count", sorted.size()); m.put("checksum", HexFormat.of().formatHex(agg.digest())); }
        else {
            StringBuilder b = new StringBuilder("[");
            for (var it : items) { if (b.length() > 1) b.append(','); b.append('[').append(esc(it.get(0))).append(',').append(esc(it.get(1))).append(']'); }
            m.put("items", new Raw(b.append(']').toString()));
        }
        return m;
    }

    record Raw(String json) { public String toString() { return json; } }

    static Map<String, Object> one(NanocachedClient client, Step s) throws Throwable {
        switch (s.op) {
            case "sleep": Thread.sleep(s.ms); return ok();
            case "probe": {
                String k = "probe:" + Long.toHexString(System.nanoTime());
                client.set(k, "p");
                Map<String, Object> m = ok();
                m.put("usable", client.get(k).filter("p"::equals).isPresent());
                return m;
            }
            case "flush": client.clearAll(); return ok();
        }
        Object t = s.x.containsKey("ns") ? client.namespace(b64d(s.x.get("ns"))) : client;
        if (s.op.equals("clear")) { call(t, "clear", new Class<?>[0]); return ok(); }
        byte[] key = s.key.equals("-") ? null : keyOf(s.key);
        long ttl = 0;
        if (!s.ttl.equals("-")) {
            try { ttl = Long.parseLong(s.ttl); }
            catch (NumberFormatException e) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("outcome", "na"); m.put("error_message", "TTL " + s.ttl + " cannot be expressed as long");
                return m;
            }
        }
        Class<?>[] kvl = {byte[].class, byte[].class, long.class};
        switch (s.op) {
            case "set": call(t, "set", kvl, key, value(s.vkind, s.varg), ttl); return ok();
            case "get": return hit(((Optional<byte[]>) call(t, "getBytes", new Class<?>[]{byte[].class}, key)).orElse(null));
            case "get_text": { Optional<String> v = (Optional<String>) call(t, "get", new Class<?>[]{byte[].class}, key); return hit(v.map(x -> x.getBytes(StandardCharsets.UTF_8)).orElse(null)); }
            case "delete": { Map<String, Object> m = ok(); m.put("deleted", call(t, "delete", new Class<?>[]{byte[].class}, key)); return m; }
            case "incr": case "decr": {
                OptionalLong v = (OptionalLong) call(t, s.op, new Class<?>[]{byte[].class, long.class}, key, Long.parseLong(s.x.get("delta")));
                Map<String, Object> m = ok();
                if (v.isPresent()) m.put("n", v.getAsLong()); else m.put("miss", true);
                return m;
            }
            case "incr_par": {
                int w = Integer.parseInt(s.x.get("w")), mm = Integer.parseInt(s.x.get("m"));
                Thread[] ts = new Thread[w]; Throwable[] err = new Throwable[1];
                for (int i = 0; i < w; i++) { ts[i] = new Thread(() -> { try { for (int j = 0; j < mm; j++) call(t, "incr", new Class<?>[]{byte[].class, long.class}, key, 1L); } catch (Throwable e) { err[0] = e; } }); ts[i].start(); }
                for (Thread th : ts) th.join();
                if (err[0] != null) throw err[0];
                Map<String, Object> m = ok(); m.put("n", ((OptionalLong) call(t, "incr", new Class<?>[]{byte[].class, long.class}, key, 0L)).getAsLong()); return m;
            }
            case "cas_get": {
                Optional<NanocachedClient.CasEntry> g = (Optional<NanocachedClient.CasEntry>) call(t, "getWithToken", new Class<?>[]{byte[].class}, key);
                if (g.isEmpty()) { Map<String, Object> m = ok(); m.put("hit", false); return m; }
                REGS.put(s.x.get("reg"), g.get().token());
                Map<String, Object> m = hit(g.get().value()); m.put("token", g.get().token()); return m;
            }
            case "cas_replace": { Map<String, Object> m = ok(); m.put("result", call(t, "replace", new Class<?>[]{byte[].class, String.class, byte[].class, long.class}, key, tokenOf(s), value(s.vkind, s.varg), ttl)); return m; }
            case "cas_delete": { Map<String, Object> m = ok(); m.put("result", call(t, "deleteIfMatches", new Class<?>[]{byte[].class, String.class}, key, tokenOf(s))); return m; }
            case "put_if_absent": case "replace_if_present": {
                Map<String, Object> m = ok();
                m.put("result", call(t, s.op.equals("put_if_absent") ? "putIfAbsent" : "replaceIfPresent", kvl, key, value(s.vkind, s.varg), ttl)); return m;
            }
            case "cas_race": {
                String tok = ((Optional<NanocachedClient.CasEntry>) call(t, "getWithToken", new Class<?>[]{byte[].class}, key)).get().token();
                int w = Integer.parseInt(s.x.get("w")); int[] won = {0}; Thread[] ts = new Thread[w];
                for (int i = 0; i < w; i++) { final int n = i; ts[i] = new Thread(() -> { try { if ((boolean) call(t, "replace", new Class<?>[]{byte[].class, String.class, byte[].class, long.class}, key, tok, ("w" + n).getBytes(), 0L)) synchronized (won) { won[0]++; } } catch (Throwable e) { } }); ts[i].start(); }
                for (Thread th : ts) th.join();
                Map<String, Object> m = ok(); m.put("n", won[0]); return m;
            }
            case "set_par": case "setdel_par": {
                int w = Integer.parseInt(s.x.get("w")), mm = Integer.parseInt(s.x.get("m")); boolean sd = s.op.equals("setdel_par"); long ttl2 = ttl;
                Thread[] ts = new Thread[w]; Throwable[] err = new Throwable[1];
                for (int i = 0; i < w; i++) { final int n = i; ts[i] = new Thread(() -> { try { for (int j = 0; j < mm; j++) { if (sd && n % 2 == 1) call(t, "delete", new Class<?>[]{byte[].class}, key); else call(t, "set", kvl, key, (SDK + "-" + n + "-" + j).getBytes(), ttl2); } } catch (Throwable e) { err[0] = e; } }); ts[i].start(); }
                for (Thread th : ts) th.join();
                if (err[0] != null) throw err[0];
                return ok();
            }
            case "many_set": {
                Map<String, byte[]> rec = new HashMap<>();
                if (s.x.containsKey("kv") && !s.x.get("kv").isEmpty()) for (String item : s.x.get("kv").split(",")) { String[] p = item.split(":"); rec.put(latin(p[0]), value(p[1], p[2])); }
                call(t, "setManyBytes", new Class<?>[]{Map.class, long.class}, rec, ttl); return ok();
            }
            case "many_get": {
                List<String> keys = new ArrayList<>();
                if (s.x.containsKey("keys") && !s.x.get("keys").isEmpty()) for (String item : s.x.get("keys").split(",", -1)) keys.add(latin(item));
                return many((Map<String, byte[]>) call(t, "getManyBytes", new Class<?>[]{List.class}, keys), false);
            }
            case "many_set_gen": {
                int n = Integer.parseInt(s.x.get("n")), vlen = Integer.parseInt(s.x.get("vlen")); String prefix = latin(s.x.get("prefix"));
                Map<String, byte[]> rec = new HashMap<>(); for (int i = 0; i < n; i++) rec.put(prefix + i, genValue(i, vlen));
                call(t, "setManyBytes", new Class<?>[]{Map.class, long.class}, rec, ttl); return ok();
            }
            case "many_get_gen": {
                int n = Integer.parseInt(s.x.get("n")); String prefix = latin(s.x.get("prefix"));
                List<String> keys = new ArrayList<>(); for (int i = 0; i < n; i++) keys.add(prefix + i);
                return many((Map<String, byte[]>) call(t, "getManyBytes", new Class<?>[]{List.class}, keys), true);
            }
        }
        throw new IllegalArgumentException(s.op);
    }

    static String tokenOf(Step s) { String reg = s.x.get("reg"); return reg.startsWith("lit:") ? reg.substring(4) : REGS.get(reg); }

    static Map<String, Object> timed(ExecutorService pool, NanocachedClient client, Step s) {
        long t0 = System.nanoTime();
        Map<String, Object> r;
        try {
            if (s.op.equals("sleep")) r = one(client, s);
            else {
                Future<Map<String, Object>> f = pool.submit(() -> { try { return one(client, s); } catch (Throwable e) { throw e instanceof Exception ex ? ex : new RuntimeException(e); } });
                try { r = f.get(STEP_TIMEOUT_S, TimeUnit.SECONDS); }
                catch (TimeoutException e) { f.cancel(true); r = new LinkedHashMap<>(); r.put("outcome", "timeout"); }
            }
        } catch (Throwable e) {
            if (e instanceof java.util.concurrent.ExecutionException && e.getCause() != null) e = e.getCause();
            r = new LinkedHashMap<>();
            r.put("outcome", "error"); r.put("error_class", e.getClass().getSimpleName());
            String m = String.valueOf(e.getMessage());
            r.put("error_message", m.length() > 300 ? m.substring(0, 300) : m);
        }
        r.put("ms", (System.nanoTime() - t0) / 1_000_000);
        return r;
    }

    static void statsLine(NanocachedClient client, String label) {
        Map<String, Object> m = ok();
        try {
            var st = client.stats();
            m.put("replica_write_failures", st.replicaWriteFailures()); m.put("read_repair_failures", st.readRepairFailures());
            m.put("refresh_failures", st.refreshFailures()); m.put("transient_retries", st.transientRetries()); m.put("background_write_bugs", st.backgroundWriteBugs());
        } catch (Throwable e) { m.put("outcome", "error"); m.put("error_message", String.valueOf(e.getMessage())); }
        emit(label, 0, "stats", m);
    }

    public static void main(String[] args) throws Exception {
        path = args[2]; topology = args[3]; String phase = args[1];
        List<Step> steps = new ArrayList<>(); List<Item> items = new ArrayList<>();
        for (String line : Files.readAllLines(Paths.get(args[0]))) {
            if (line.isEmpty()) continue;
            String[] p = line.split("\t", -1);
            if (p[0].equals("S")) steps.add(new Step(p[1], Integer.parseInt(p[2]), p[3], p[4], p[5], p[6], p[7], Integer.parseInt(p[8]), parseX(p.length > 9 ? p[9] : "")));
            else items.add(new Item(p[1], new String(Base64.getDecoder().decode(p[2]), StandardCharsets.UTF_8), p[3], p[4], p[5]));
        }
        List<Address> addrs = new ArrayList<>();
        for (String part : System.getenv("NANOTEST_ADDRESSES").split(",")) {
            int idx = part.lastIndexOf(':');
            addrs.add(new Address(part.substring(0, idx), Integer.parseInt(part.substring(idx + 1))));
        }
        NanocachedClient.Options options = NanocachedClient.builder().addresses(addrs).viaProxy("1".equals(System.getenv("NANOTEST_VIA_PROXY")));
        String secret = System.getenv("NANOTEST_SECRET");
        if (secret != null && !secret.isEmpty()) options = options.authSecret(secret);

        ExecutorService pool = Executors.newCachedThreadPool(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        NanocachedClient client = NanocachedClient.connect(options);
        try {
            if (phase.equals("main")) {
                String skip = null;
                for (Step s : steps) {
                    if (s.c.equals(skip)) continue;
                    Map<String, Object> r = timed(pool, client, s);
                    emit(s.c, s.i, s.op, r);
                    if ("timeout".equals(r.get("outcome"))) skip = s.c;
                }
                for (Item it : items) {
                    String key = Base64.getEncoder().encodeToString(it.tmpl.replace("{sdk}", SDK).getBytes(StandardCharsets.UTF_8));
                    emit("xsdk.write." + it.name, 0, "set", timed(pool, client, new Step("", 0, "set", key, it.vkind, it.varg, it.ttl, 0, new HashMap<>())));
                }
            } else {
                for (String w : ALL) for (Item it : items) {
                    String key = Base64.getEncoder().encodeToString(it.tmpl.replace("{sdk}", w).getBytes(StandardCharsets.UTF_8));
                    emit("xsdk.read." + w + "." + it.name, 0, "get", timed(pool, client, new Step("", 0, "get", key, "-", "-", "-", 0, new HashMap<>())));
                }
            }
        } finally {
            if (phase.equals("main")) statsLine(client, "_stats_before_close");
            Future<?> f = pool.submit(client::close);
            try { f.get(STEP_TIMEOUT_S, TimeUnit.SECONDS); } catch (Exception ignored) { }
            if (phase.equals("main")) statsLine(client, "_stats");
        }
        System.exit(0);
    }
}
