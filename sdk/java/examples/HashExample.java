// Publishes a dataset, then hashes it repeatedly. The first task pays for
// moving the data; the rest run where it already is.
//
//   javac -d out $(find sdk/java/src -name '*.java') sdk/java/examples/HashExample.java
//   java -cp out HashExample --tasks 5 --mib 8

import dev.aethermesh.AetherMesh;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;

public final class HashExample {

    public static void main(String[] args) throws Exception {
        String host = argument(args, "--host", "127.0.0.1");
        int port = Integer.parseInt(argument(args, "--port", "7100"));
        int tasks = Integer.parseInt(argument(args, "--tasks", "5"));
        int mib = Integer.parseInt(argument(args, "--mib", "8"));
        String token = argument(args, "--token", null);

        AetherMesh.Options options = new AetherMesh.Options().host(host).port(port);
        if (token != null) {
            options.token(token);
        }

        try (AetherMesh mesh = AetherMesh.connect(options)) {
            List<AetherMesh.NodeSummary> nodes = mesh.nodes();
            System.out.printf("%d node(s): %s%n", nodes.size(),
                    nodes.stream().map(HashExample::describe).toList());
            if (nodes.isEmpty()) {
                System.out.println("nothing to run on — start an agent");
                return;
            }

            byte[] payload = new byte[mib * 1024 * 1024];
            new Random(7).nextBytes(payload);

            long started = System.nanoTime();
            AetherMesh.Published published = mesh.publish(payload);
            System.out.printf("published %d MiB in %.0f ms as %s…%n",
                    mib, (System.nanoTime() - started) / 1e6, published.dataId().substring(0, 16));

            for (int index = 0; index < tasks; index++) {
                AetherMesh.TaskResult result = mesh.run(
                        "hash",
                        String.valueOf(index).getBytes(),
                        List.of(published.dataId()),
                        List.of());

                if (!result.success()) {
                    System.out.printf("  task %3d: failed — %s%n", index, result.error());
                    continue;
                }
                System.out.printf("  task %3d: %s… on %s in %6.1f ms%n",
                        index,
                        HexFormat.of().formatHex(result.output()).substring(0, 16),
                        result.nodeId().substring(0, 8),
                        result.durationMs());
            }
        }
    }

    private static String describe(AetherMesh.NodeSummary node) {
        return node.labels().isEmpty()
                ? node.hostname()
                : node.hostname() + " " + node.labels();
    }

    private static String argument(String[] args, String name, String fallback) {
        for (int index = 0; index + 1 < args.length; index++) {
            if (args[index].equals(name)) {
                return args[index + 1];
            }
        }
        return fallback;
    }
}
