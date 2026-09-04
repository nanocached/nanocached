// .NET-SDK smoke for the AWS live tests: nanotest <write|read> <label> <count>
// Addresses via NANOTEST_ADDRESSES ("host:port,host:port").
using Nanocached;

var options = new NanocachedClient.Options();
foreach (var part in Environment.GetEnvironmentVariable("NANOTEST_ADDRESSES")!.Split(','))
{
    var idx = part.LastIndexOf(':');
    options.Addresses.Add((part[..idx], int.Parse(part[(idx + 1)..])));
}

// Checked before connecting: an invalid invocation should fail loudly
// with a usage message, not crash with an IndexOutOfRangeException on a
// missing argument or an uncaught FormatException / a non-positive count
// that silently makes every loop below a no-op, reporting a false
// "success" as if 0 iterations were intended.
if (args.Length != 3)
{
    Console.Error.WriteLine("usage: nanotest <write|read> <label> <count>");
    Environment.Exit(1);
    return;
}

var cmd = args[0];
var label = args[1];
if (!int.TryParse(args[2], out var count) || count <= 0)
{
    Console.Error.WriteLine($"usage: nanotest <write|read> <label> <count>: invalid count '{args[2]}'");
    Environment.Exit(1);
    return;
}

var exitCode = 0;

using (NanocachedClient client = await NanocachedClient.ConnectAsync(options))
{
    if (cmd == "write")
    {
        for (var i = 0; i < count; i++)
        {
            await client.SetAsync($"x:{label}:{i}", $"v-{label}-{i}");
        }
        Console.WriteLine($"wrote {count} keys for label {label}");
    }
    else if (cmd == "read")
    {
        var bad = 0;
        for (var i = 0; i < count; i++)
        {
            var value = await client.GetAsync($"x:{label}:{i}");
            if (value is null || value != $"v-{label}-{i}")
            {
                bad++;
            }
        }
        if (bad > 0)
        {
            Console.WriteLine($"label {label}: {bad}/{count} BAD");
            exitCode = 1;
        }
        else
        {
            Console.WriteLine($"label {label}: {count}/{count} OK");
        }
    }
    else
    {
        Console.WriteLine($"unknown command {cmd}");
        exitCode = 2;
    }
}

if (exitCode != 0)
{
    Environment.Exit(exitCode);
}
