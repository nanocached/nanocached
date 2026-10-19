// Scenario driver for the .NET SDK (churn, connection checks, lifecycle).
// scenario <scenario> [args]; same scenarios and JSON events as drivers/python/scenario.py.
using System.Diagnostics;
using System.Text.Json;
using Nanocached;

const string Sdk = "dotnet";
var t0 = Stopwatch.StartNew();
var limit = TimeSpan.FromSeconds(double.Parse(Environment.GetEnvironmentVariable("CONN_TIMEOUT_S") ?? "60"));
var options = new NanocachedClient.Options();
foreach (var part in Environment.GetEnvironmentVariable("NANOTEST_ADDRESSES")!.Split(','))
{
    var i = part.LastIndexOf(':');
    options.Addresses.Add((part[..i], int.Parse(part[(i + 1)..])));
}
options.ViaProxy = Environment.GetEnvironmentVariable("NANOTEST_VIA_PROXY") == "1";
var secret = Environment.GetEnvironmentVariable("NANOTEST_SECRET");
if (!string.IsNullOrEmpty(secret)) options.AuthSecret = secret;

void Emit(Dictionary<string, object?> m)
{
    m["sdk"] = Sdk; m["t_ms"] = t0.ElapsedMilliseconds;
    Console.WriteLine(JsonSerializer.Serialize(m));
    Console.Out.Flush();
}
Dictionary<string, object?> Err(Exception e)
{
    while (e is AggregateException ae && ae.InnerException != null) e = ae.InnerException;
    var msg = e.Message;
    return new() { ["outcome"] = "error", ["error_class"] = e.GetType().Name, ["error_message"] = msg.Length > 300 ? msg[..300] : msg };
}
Task<NanocachedClient> Connect() => NanocachedClient.ConnectAsync(options);

async Task<T?> Step<T>(string name, Func<Task<T>> f, TimeSpan? bound = null)
{
    var sw = Stopwatch.StartNew();
    try
    {
        var r = await f().WaitAsync(bound ?? limit);
        Emit(new() { ["event"] = name, ["outcome"] = "ok", ["ms"] = sw.ElapsedMilliseconds });
        return r;
    }
    catch (TimeoutException) { Emit(new() { ["event"] = name, ["outcome"] = "timeout", ["ms"] = sw.ElapsedMilliseconds }); }
    catch (Exception e) { var m = Err(e); m["event"] = name; m["ms"] = sw.ElapsedMilliseconds; Emit(m); }
    return default;
}

Dictionary<string, object?> ProcStats()
{
    var st = File.ReadAllText("/proc/self/status");
    int Field(string k) => int.Parse(st.Split('\n').First(l => l.StartsWith(k)).Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries)[1]);
    return new() { ["fds"] = Directory.GetFileSystemEntries("/proc/self/fd").Length, ["threads"] = Field("Threads:"), ["rss_kb"] = Field("VmRSS:") };
}

var name = args[0];
var a = args.Skip(1).ToArray();
switch (name)
{
    case "conn":
    {
        var c = await Step("connect", Connect);
        if (c is null) break;
        await Step("set", async () => { await c.SetAsync("conn:k", "v"); return true; });
        var got = await Step("get", () => c.GetAsync("conn:k"));
        Emit(new() { ["event"] = "get_value", ["value"] = got });
        Emit(new() { ["event"] = "describe", ["text"] = options.ToString() + " | " + c.ToString() + " | " + JsonSerializer.Serialize(options) });
        await Step("close", async () => { c.Dispose(); return true; });
        break;
    }
    case "churn":
    {
        var secs = double.Parse(a[0]); var label = a[1];
        using var c = await Connect();
        var acked = new int[200]; int ops = 0, errors = 0, stale = 0, miss = 0, corrupt = 0; long? first = null, last = null;
        var classes = new Dictionary<string, int>();
        var end = DateTime.UtcNow.AddSeconds(secs);
        for (var i = 1; DateTime.UtcNow < end; i++)
        {
            var n = (i - 1) % 200; var key = $"ch:{label}:{Sdk}:{n}"; ops++;
            try
            {
                if (i % 2 == 1) { var v = acked[n] + 1; await c.SetAsync(key, $"v{v}").WaitAsync(TimeSpan.FromSeconds(10)); acked[n] = v; }
                else
                {
                    var got = await c.GetAsync(key).WaitAsync(TimeSpan.FromSeconds(10));
                    if (acked[n] > 0) { if (got is null) miss++; else if (!got.StartsWith('v')) corrupt++; else if (int.Parse(got[1..]) < acked[n]) stale++; }
                }
            }
            catch (Exception e)
            {
                errors++; var cls = e.GetType().Name; classes[cls] = classes.GetValueOrDefault(cls) + 1;
                var now = t0.ElapsedMilliseconds; first ??= now; last = now;
            }
            await Task.Delay(5);
        }
        Emit(new() { ["event"] = "summary", ["ops"] = ops, ["errors"] = errors, ["stale"] = stale, ["miss"] = miss, ["corrupt"] = corrupt, ["classes"] = classes, ["first_err_ms"] = first, ["last_err_ms"] = last });
        break;
    }
    case "life":
    {
        var test = a[0]; var arg = a.Length > 1 ? a[1] : "";
        switch (test)
        {
            case "close_inflight":
            {
                var c = await Connect();
                await c.SetAsync("life:big", new string('b', 900000));
                var pend = Enumerable.Range(0, 4).Select(_ => c.GetBytesAsync("life:big")).ToList();
                await Task.Delay(200);
                var sw = Stopwatch.StartNew();
                await Step("close", async () => { c.Dispose(); return true; });
                Emit(new() { ["event"] = "close_ms", ["ms"] = sw.ElapsedMilliseconds });
                for (var i = 0; i < pend.Count; i++)
                {
                    try { var v = await pend[i].WaitAsync(TimeSpan.FromSeconds(20)); Emit(new() { ["event"] = "pending", ["i"] = i, ["outcome"] = "ok", ["len"] = v?.Length }); }
                    catch (TimeoutException) { Emit(new() { ["event"] = "pending", ["i"] = i, ["outcome"] = "timeout" }); }
                    catch (Exception e) { var m = Err(e); m["event"] = "pending"; m["i"] = i; Emit(m); }
                }
                break;
            }
            case "after_close":
            {
                var c = await Connect(); await c.SetAsync("life:k", "v"); c.Dispose();
                await Step("after_close.get", () => c.GetAsync("life:k"));
                await Step("after_close.set", async () => { await c.SetAsync("life:k", "v"); return true; });
                await Step("after_close.delete", () => c.DeleteAsync("life:k"));
                await Step("after_close.incr", () => c.IncrAsync("life:k", 1));
                await Step("after_close.get_many", () => c.GetManyAsync(new[] { "life:k" }));
                break;
            }
            case "double_close": case "concurrent_close":
            {
                var c = await Connect();
                await Step("close1", async () => { c.Dispose(); return true; });
                await Step("close2", async () => { c.Dispose(); return true; });
                var d = await Connect();
                await Step("concurrent_close", async () => { await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => Task.Run(d.Dispose))); return true; });
                break;
            }
            case "abandon":
            {
                var c = await Connect();
                await c.SetAsync("life:a", "value-a"); await c.SetAsync("life:b", "value-b");
                var pending = c.GetAsync("life:a");
                try { await pending.WaitAsync(TimeSpan.FromMilliseconds(300)); } catch (TimeoutException) { } // the caller stops waiting; the request stays in flight
                Emit(new() { ["event"] = "cancelled" });
                var r = new Dictionary<string, string?>();
                foreach (var (k, key) in new[] { ("b", "life:b"), ("a", "life:a") })
                {
                    try { r[k] = await c.GetAsync(key).WaitAsync(TimeSpan.FromSeconds(30)); }
                    catch (TimeoutException) { r[k] = "timeout"; }
                    catch (Exception e) { r[k] = e.GetType().Name + ": " + e.Message; }
                }
                Emit(new() { ["event"] = "after_cancel", ["b"] = r["b"], ["a"] = r["a"], ["ok"] = r["b"] == "value-b" && r["a"] == "value-a" });
                c.Dispose();
                break;
            }
            case "cycles":
            {
                var n = int.Parse(arg); var before = ProcStats();
                for (var i = 0; i < n; i++) { using var c = await Connect(); await c.SetAsync("life:c", "v"); }
                await Task.Delay(1000);
                Emit(new() { ["event"] = "cycles", ["n"] = n, ["before"] = before, ["after"] = ProcStats() });
                break;
            }
            case "reconnect":
            {
                using var c = await Connect(); var end = DateTime.UtcNow.AddSeconds(double.Parse(arg));
                int ok = 0, fail = 0; long? ff = null, lf = null, fo = null;
                while (DateTime.UtcNow < end)
                {
                    try { await c.GetBytesAsync("life:r").WaitAsync(TimeSpan.FromSeconds(10)); ok++; if (ff != null && fo == null) fo = t0.ElapsedMilliseconds; }
                    catch { fail++; var now = t0.ElapsedMilliseconds; ff ??= now; lf = now; }
                    await Task.Delay(100);
                }
                Emit(new() { ["event"] = "reconnect", ["ok"] = ok, ["fail"] = fail, ["first_fail_ms"] = ff, ["last_fail_ms"] = lf, ["first_ok_after_ms"] = fo });
                break;
            }
            case "storm":
            {
                using var c = await Connect(); var end = DateTime.UtcNow.AddSeconds(double.Parse(arg)); var attempts = 0;
                while (DateTime.UtcNow < end) { attempts++; try { await c.GetBytesAsync("life:s").WaitAsync(TimeSpan.FromSeconds(5)); } catch { } await Task.Delay(50); }
                Emit(new() { ["event"] = "storm", ["attempts"] = attempts });
                break;
            }
            case "exit_noclose":
            {
                var c = await Connect(); await c.SetAsync("life:e", "v"); Emit(new() { ["event"] = "exiting_without_close" });
                break;
            }
        }
        break;
    }
}
return 0;
