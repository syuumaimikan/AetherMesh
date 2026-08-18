// Publishes a dataset, then hashes it repeatedly. The first task pays for
// moving the data; the rest run where it already is.
//
//   dotnet run --project sdk/dotnet/Examples/Hash -- --tasks 10

using System.Diagnostics;
using System.Text;
using AetherMesh;

var host = Argument(args, "--host") ?? "127.0.0.1";
var port = int.Parse(Argument(args, "--port") ?? "7100");
var tasks = int.Parse(Argument(args, "--tasks") ?? "5");
var mib = int.Parse(Argument(args, "--mib") ?? "8");
var token = Argument(args, "--token");

await using var mesh = await MeshClient.ConnectAsync(new MeshOptions
{
    Host = host,
    Port = port,
    Token = token,
});

var nodes = await mesh.NodesAsync();
Console.WriteLine($"{nodes.Count} node(s): {string.Join(", ", nodes.Select(Describe))}");
if (nodes.Count == 0)
{
    Console.WriteLine("nothing to run on — start an agent");
    return 1;
}

var payload = new byte[mib * 1024 * 1024];
Random.Shared.NextBytes(payload);

var clock = Stopwatch.StartNew();
var published = await mesh.PublishAsync(payload);
Console.WriteLine(
    $"published {mib} MiB in {clock.Elapsed.TotalMilliseconds:F0} ms as {published.DataId[..16]}…");

for (var index = 0; index < tasks; index++)
{
    var result = await mesh.RunAsync(
        "hash",
        Encoding.UTF8.GetBytes(index.ToString()),
        inputs: [published.DataId]);

    if (!result.Success)
    {
        Console.WriteLine($"  task {index,3}: failed — {result.Error}");
        continue;
    }

    Console.WriteLine(
        $"  task {index,3}: {Convert.ToHexString(result.Output)[..16].ToLowerInvariant()}… " +
        $"on {result.NodeId[..8]} in {result.DurationMs,6:F1} ms");
}

return 0;

static string Describe(NodeSummary node)
{
    var labels = node.Labels.Count == 0
        ? ""
        : " [" + string.Join(" ", node.Labels.Select(pair => $"{pair.Key}={pair.Value}")) + "]";
    return node.Hostname + labels;
}

static string? Argument(string[] args, string name)
{
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
}
