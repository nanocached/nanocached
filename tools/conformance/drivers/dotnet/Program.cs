// Conformance driver for the .NET SDK: conformance <cases.tsv> <phase> <path> <topology>
// One JSON line per step on stdout (case format: tools/conformance/make_cases.py).
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Nanocached;

var regs = new Dictionary<string, string>();
const string Sdk = "dotnet";
string[] All = { "python", "typescript", "go", "java", "dotnet", "rust" };
var stepTimeout = TimeSpan.FromSeconds(10);
string casesPath = args[0], phase = args[1], path = args[2], topology = args[3];

var steps = new List<Step>();
var items = new List<Item>();
foreach (var line in File.ReadAllLines(casesPath))
{
    if (line.Length == 0) continue;
    var p = line.Split('\t');
    if (p[0] == "S") steps.Add(new Step(p[1], int.Parse(p[2]), p[3], p[4], p[5], p[6], p[7], int.Parse(p[8]), ParseX(p.Length > 9 ? p[9] : "")));
    else items.Add(new Item(p[1], Encoding.UTF8.GetString(Convert.FromBase64String(p[2])), p[3], p[4], p[5]));
}

var options = new NanocachedClient.Options();
foreach (var part in Environment.GetEnvironmentVariable("NANOTEST_ADDRESSES")!.Split(','))
{
    var idx = part.LastIndexOf(':');
    options.Addresses.Add((part[..idx], int.Parse(part[(idx + 1)..])));
}
options.ViaProxy = Environment.GetEnvironmentVariable("NANOTEST_VIA_PROXY") == "1";
var secret = Environment.GetEnvironmentVariable("NANOTEST_SECRET");
if (!string.IsNullOrEmpty(secret)) options.AuthSecret = secret;

var client = await NanocachedClient.ConnectAsync(options);
void StatsLine(string label)
{
    var st = client.Stats();
    Emit(label, 0, "stats", new() { ["outcome"] = "ok", ["replica_write_failures"] = st.ReplicaWriteFailures, ["read_repair_failures"] = st.ReadRepairFailures, ["refresh_failures"] = st.RefreshFailures, ["transient_retries"] = st.TransientRetries });
}

if (phase == "main")
{
    string? skip = null;
    foreach (var s in steps)
    {
        if (s.C == skip) continue;
        var r = await Timed(s);
        Emit(s.C, s.I, s.Op, r);
        if ((string)r["outcome"] == "timeout") skip = s.C;
    }
    foreach (var it in items)
    {
        var key = Convert.ToBase64String(Encoding.UTF8.GetBytes(it.Tmpl.Replace("{sdk}", Sdk)));
        Emit("xsdk.write." + it.Name, 0, "set", await Timed(new Step("", 0, "set", key, it.VKind, it.VArg, it.Ttl, 0, new())));
    }
}
else
{
    foreach (var w in All)
        foreach (var it in items)
        {
            var key = Convert.ToBase64String(Encoding.UTF8.GetBytes(it.Tmpl.Replace("{sdk}", w)));
            Emit($"xsdk.read.{w}.{it.Name}", 0, "get", await Timed(new Step("", 0, "get", key, "-", "-", "-", 0, new())));
        }
}
if (phase == "main") StatsLine("_stats_before_close");
client.Dispose();
if (phase == "main") StatsLine("_stats");
return 0;

void Emit(string c, int step, string op, Dictionary<string, object> r)
{
    var all = new Dictionary<string, object> { ["case"] = c, ["step"] = step, ["op"] = op };
    foreach (var kv in r) all[kv.Key] = kv.Value;
    all["sdk"] = Sdk; all["path"] = path; all["topology"] = topology;
    Console.WriteLine(JsonSerializer.Serialize(all));
    Console.Out.Flush();
}

static byte[] Value(string kind, string arg)
{
    if (kind == "len")
    {
        var b = new byte[int.Parse(arg)];
        for (var i = 0; i < b.Length; i++) b[i] = (byte)(i % 251);
        return b;
    }
    return Convert.FromBase64String(arg);
}

static byte[] KeyOf(string b64) => b64 == "=" ? Array.Empty<byte>() : Convert.FromBase64String(b64);

static Dictionary<string, object> Hit(byte[]? v) =>
    v is null
        ? new() { ["outcome"] = "ok", ["hit"] = false }
        : new() { ["outcome"] = "ok", ["hit"] = true, ["len"] = v.Length, ["sha256"] = Convert.ToHexString(SHA256.HashData(v)).ToLowerInvariant() };

static Dictionary<string, string> ParseX(string x)
{
    var m = new Dictionary<string, string>();
    foreach (var kv in x.Split(';', StringSplitOptions.RemoveEmptyEntries))
    {
        var i = kv.IndexOf('=');
        if (i > 0) m[kv[..i]] = kv[(i + 1)..];
    }
    return m;
}

static byte[] B64d(string b) => b == "=" || b == "" ? Array.Empty<byte>() : Convert.FromBase64String(b);
static string Latin(string b64) => Encoding.Latin1.GetString(B64d(b64));
static byte[] GenValue(int i, int n) { var b = new byte[n]; for (var j = 0; j < n; j++) b[j] = (byte)((i + j) % 251); return b; }
static string Hex(byte[] v) => Convert.ToHexString(SHA256.HashData(v)).ToLowerInvariant();

string TokenOf(Step s) => s.X["reg"].StartsWith("lit:") ? s.X["reg"][4..] : regs[s.X["reg"]];

Dictionary<string, object> Many(Dictionary<string, byte[]> found, bool aggregate)
{
    var keys = found.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList();
    using var agg = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    var items = new List<string[]>();
    foreach (var k in keys)
    {
        var h = Hex(found[k]);
        items.Add(new[] { Convert.ToBase64String(Encoding.Latin1.GetBytes(k)), h });
        agg.AppendData(Encoding.Latin1.GetBytes(k)); agg.AppendData(new byte[] { 0 }); agg.AppendData(Encoding.ASCII.GetBytes(h));
    }
    return aggregate
        ? new() { ["outcome"] = "ok", ["count"] = keys.Count, ["checksum"] = Convert.ToHexString(agg.GetHashAndReset()).ToLowerInvariant() }
        : new() { ["outcome"] = "ok", ["items"] = items };
}

async Task<Dictionary<string, object>> One(Step s)
{
    switch (s.Op)
    {
        case "sleep":
            await Task.Delay(s.Ms);
            return new() { ["outcome"] = "ok" };
        case "probe":
        {
            var k = "probe:" + Guid.NewGuid().ToString("N");
            await client.SetAsync(k, "p");
            return new() { ["outcome"] = "ok", ["usable"] = await client.GetAsync(k) == "p" };
        }
        case "flush":
            await client.ClearAllAsync();
            return new() { ["outcome"] = "ok" };
    }
    dynamic t = s.X.ContainsKey("ns") ? client.Namespace(B64d(s.X["ns"])) : client;
    if (s.Op == "clear")
    {
        await t.ClearAsync();
        return new() { ["outcome"] = "ok" };
    }
    var key = s.Key == "-" ? Array.Empty<byte>() : KeyOf(s.Key);
    long ttl = 0;
    if (s.Ttl != "-" && !long.TryParse(s.Ttl, out ttl))
        return new() { ["outcome"] = "na", ["error_message"] = $"TTL {s.Ttl} cannot be expressed as long" };
    switch (s.Op)
    {
        case "set": await t.SetAsync(key, Value(s.VKind, s.VArg), ttl); return new() { ["outcome"] = "ok" };
        case "get": return Hit((byte[]?)await t.GetBytesAsync(key));
        case "get_text": { string? v = await t.GetAsync(key); return Hit(v is null ? null : Encoding.UTF8.GetBytes(v)); }
        case "delete": return new() { ["outcome"] = "ok", ["deleted"] = (bool)await t.DeleteAsync(key) };
        case "incr": case "decr":
        {
            long delta = long.Parse(s.X["delta"]);
            long? v = s.Op == "incr" ? await t.IncrAsync(key, delta) : await t.DecrAsync(key, delta);
            return v is null ? new() { ["outcome"] = "ok", ["miss"] = true } : new() { ["outcome"] = "ok", ["n"] = v.Value };
        }
        case "incr_par":
        {
            var w = int.Parse(s.X["w"]); var m = int.Parse(s.X["m"]);
            await Task.WhenAll(Enumerable.Range(0, w).Select(async _ => { for (var j = 0; j < m; j++) await t.IncrAsync(key, 1L); }));
            long? last = await t.IncrAsync(key, 0L);
            return new() { ["outcome"] = "ok", ["n"] = last!.Value };
        }
        case "cas_get":
        {
            var g = await t.GetWithTokenAsync(key);
            if (g is null) return new() { ["outcome"] = "ok", ["hit"] = false };
            regs[s.X["reg"]] = (string)g.Item2;
            var r = Hit(Encoding.UTF8.GetBytes((string)g.Item1));
            r["token"] = (string)g.Item2;
            return r;
        }
        case "cas_replace": return new() { ["outcome"] = "ok", ["result"] = (bool)await t.ReplaceAsync(key, TokenOf(s), Value(s.VKind, s.VArg), ttl) };
        case "cas_delete": return new() { ["outcome"] = "ok", ["result"] = (bool)await t.DeleteIfMatchesAsync(key, TokenOf(s)) };
        case "put_if_absent": return new() { ["outcome"] = "ok", ["result"] = (bool)await t.PutIfAbsentAsync(key, Value(s.VKind, s.VArg), ttl) };
        case "replace_if_present": return new() { ["outcome"] = "ok", ["result"] = (bool)await t.ReplaceIfPresentAsync(key, Value(s.VKind, s.VArg), ttl) };
        case "cas_race":
        {
            var g = await t.GetWithTokenAsync(key);
            string tok = (string)g.Item2;
            var rs = await Task.WhenAll(Enumerable.Range(0, int.Parse(s.X["w"])).Select(async i => (bool)await t.ReplaceAsync(key, tok, Encoding.UTF8.GetBytes($"w{i}"), 0L)));
            return new() { ["outcome"] = "ok", ["n"] = rs.Count(x => x) };
        }
        case "set_par": case "setdel_par":
        {
            int w = int.Parse(s.X["w"]), m = int.Parse(s.X["m"]); bool sd = s.Op == "setdel_par"; long ttl2 = ttl;
            await Task.WhenAll(Enumerable.Range(0, w).Select(async i =>
            {
                for (var j = 0; j < m; j++)
                {
                    if (sd && i % 2 == 1) await t.DeleteAsync(key);
                    else await t.SetAsync(key, Encoding.UTF8.GetBytes($"{Sdk}-{i}-{j}"), ttl2);
                }
            }));
            return new() { ["outcome"] = "ok" };
        }
        case "many_set":
        {
            var rec = new Dictionary<string, byte[]>();
            if (s.X.TryGetValue("kv", out var kv) && kv.Length > 0)
                foreach (var item in kv.Split(',')) { var p = item.Split(':'); rec[Latin(p[0])] = Value(p[1], p[2]); }
            await t.SetManyBytesAsync(rec, ttl);
            return new() { ["outcome"] = "ok" };
        }
        case "many_get":
        {
            var keys = s.X.TryGetValue("keys", out var ks) && ks.Length > 0 ? ks.Split(',').Select(Latin).ToList() : new List<string>();
            return Many((Dictionary<string, byte[]>)await t.GetManyBytesAsync(keys), false);
        }
        case "many_set_gen":
        {
            int n = int.Parse(s.X["n"]), vlen = int.Parse(s.X["vlen"]); var prefix = Latin(s.X["prefix"]);
            var rec = new Dictionary<string, byte[]>();
            for (var i = 0; i < n; i++) rec[prefix + i] = GenValue(i, vlen);
            await t.SetManyBytesAsync(rec, ttl);
            return new() { ["outcome"] = "ok" };
        }
        case "many_get_gen":
        {
            int n = int.Parse(s.X["n"]); var prefix = Latin(s.X["prefix"]);
            var keys = Enumerable.Range(0, n).Select(i => prefix + i).ToList();
            return Many((Dictionary<string, byte[]>)await t.GetManyBytesAsync(keys), true);
        }
    }
    throw new InvalidOperationException(s.Op);
}

async Task<Dictionary<string, object>> Timed(Step s)
{
    var sw = System.Diagnostics.Stopwatch.StartNew();
    Dictionary<string, object> r;
    try
    {
        r = s.Op == "sleep" ? await One(s) : await One(s).WaitAsync(stepTimeout);
    }
    catch (TimeoutException)
    {
        r = new() { ["outcome"] = "timeout" };
    }
    catch (Exception e)
    {
        var m = e.Message;
        r = new() { ["outcome"] = "error", ["error_class"] = e.GetType().Name, ["error_message"] = m.Length > 300 ? m[..300] : m };
    }
    r["ms"] = (int)sw.ElapsedMilliseconds;
    return r;
}

record Step(string C, int I, string Op, string Key, string VKind, string VArg, string Ttl, int Ms, Dictionary<string, string> X);
record Item(string Name, string Tmpl, string VKind, string VArg, string Ttl);
