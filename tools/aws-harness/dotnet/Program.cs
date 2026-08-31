// .NET-SDK smoke for the AWS live tests: nanotest <write|read> <label> <count>
// Addresses via NANOTEST_ADDRESSES ("host:port,host:port").
using Nanocached;

var options = new NanocachedClient.Options();
foreach (var part in Environment.GetEnvironmentVariable("NANOTEST_ADDRESSES")!.Split(','))
{
    var idx = part.LastIndexOf(':');
    options.Addresses.Add((part[..idx], int.Parse(part[(idx + 1)..])));
}

var cmd = args[0];
var label = args[1];
var count = int.Parse(args[2]);

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
