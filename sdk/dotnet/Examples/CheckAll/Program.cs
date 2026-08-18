// Exercises everything the .NET SDK can now ask a controller for.
using AetherMesh;
using System.Text;

await using var mesh = await MeshClient.ConnectAsync(new MeshOptions());

var node = (await mesh.NodesAsync())[0];
Console.WriteLine($"node fields: held={node.DatasetsHeld} bytes={node.BytesHeld} " +
                  $"connected={node.Connected} address={node.Address}");

var steps = new[]
{
    new Step("echo", Encoding.UTF8.GetBytes("seed")),
    new Step("hash", DependsOn: new[] { 0 }),
    new Step("no-such-kind", DependsOn: new[] { 1 }),
};

var first = await mesh.WorkflowAsync(steps, "dotnet-check");
Console.WriteLine($"run 1: ran=[{string.Join(", ", first.Steps.Select(s => s.Step))}] " +
                  $"resumed=[{string.Join(", ", first.Resumed)}]");
var second = await mesh.WorkflowAsync(steps, "dotnet-check");
Console.WriteLine($"run 2: ran=[{string.Join(", ", second.Steps.Select(s => s.Step))}] " +
                  $"resumed=[{string.Join(", ", second.Resumed)}]");

try
{
    await mesh.WorkflowAsync(new[] { new Step("echo") }, "dotnet-check");
    Console.WriteLine("FAIL: a different workflow was accepted under the same name");
}
catch (MeshException error)
{
    Console.WriteLine($"wrong workflow refused: {error.Message[..Math.Min(58, error.Message.Length)]}…");
}

var urgent = await mesh.RunAsync("echo", Encoding.UTF8.GetBytes("urgent"), priority: Priority.Critical);
Console.WriteLine($"priority run: {Encoding.UTF8.GetString(urgent.Output)} on {urgent.NodeId[..8]}");

var stats = await mesh.StatsAsync();
Console.WriteLine($"stats keys: {stats.EnumerateObject().Count() - 1}");

foreach (var task in await mesh.RecentAsync(3))
{
    Console.WriteLine($"  recent: {task.Kind,-6} {task.DurationMs,5:F1} ms " +
                      $"{task.SecondsAgo:F0}s ago \"{task.Preview}\"");
}
