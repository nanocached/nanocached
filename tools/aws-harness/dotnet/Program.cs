// .NET-SDK smoke for the AWS live tests: nanotest <write|read> <label> <count>
// Seeds via NANOTEST_SEEDS ("host:port,host:port").
using Nanocached;

var options = new NanocachedClient.Options();
foreach (var part in Environment.GetEnvironmentVariable("NANOTEST_SEEDS")!.Split(','))
{
    var idx = part.LastIndexOf(':');
    options = options.Host(part[..idx], int.Parse(part[(idx + 1)..]));
}

var cmd = args[0];
var label = args[1];
var count = int.Parse(args[2]);

using NanocachedClient client = await NanocachedClient.ConnectAsync(options);

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
        if (value is null || System.Text.Encoding.UTF8.GetString(value) != $"v-{label}-{i}")
        {
            bad++;
        }
    }
    if (bad > 0)
    {
        Console.WriteLine($"label {label}: {bad}/{count} BAD");
        Environment.Exit(1);
    }
    Console.WriteLine($"label {label}: {count}/{count} OK");
}
else
{
    Console.WriteLine($"unknown command {cmd}");
    Environment.Exit(2);
}
