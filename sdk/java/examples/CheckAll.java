import dev.aethermesh.AetherMesh;
import dev.aethermesh.AetherMesh.*;
import java.util.List;

public class CheckAll {
    public static void main(String[] args) throws Exception {
        try (AetherMesh mesh = AetherMesh.connect()) {
            NodeSummary node = mesh.nodes().get(0);
            System.out.printf("node fields: held=%d bytes=%d connected=%b address=%s%n",
                    node.datasetsHeld(), node.bytesHeld(), node.connected(), node.address());

            List<Step> steps = List.of(
                    Step.of("echo", "seed".getBytes()),
                    Step.of("hash", new byte[0], 0),
                    Step.of("no-such-kind", new byte[0], 1));

            WorkflowResult first = mesh.workflow(steps, "java-check");
            System.out.println("run 1: ran=" + first.steps().stream().map(StepOutcome::step).toList()
                    + " resumed=" + first.resumed());
            WorkflowResult second = mesh.workflow(steps, "java-check");
            System.out.println("run 2: ran=" + second.steps().stream().map(StepOutcome::step).toList()
                    + " resumed=" + second.resumed());

            try {
                mesh.workflow(List.of(Step.of("echo", "different".getBytes())), "java-check");
                System.out.println("FAIL: a different workflow was accepted under the same name");
            } catch (Exception error) {
                System.out.println("wrong workflow refused: "
                        + error.getMessage().substring(0, Math.min(58, error.getMessage().length())) + "…");
            }

            TaskResult urgent = mesh.run("echo", "urgent".getBytes(), Priority.CRITICAL, List.of(), List.of());
            System.out.printf("priority run: %s on %.8s%n", new String(urgent.output()), urgent.nodeId());

            System.out.println("stats keys: " + mesh.stats().size());
            for (FinishedTask task : mesh.recent(3)) {
                System.out.printf("  recent: %-6s %5.1f ms %.0fs ago %s%n",
                        task.kind(), task.durationMs(), task.secondsAgo(), task.preview());
            }
        }
    }
}
