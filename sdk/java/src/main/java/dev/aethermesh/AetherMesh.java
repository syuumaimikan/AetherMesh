package dev.aethermesh;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

/**
 * A connection to an AetherMesh controller.
 *
 * <p>The wire format is four bytes of big-endian length followed by one JSON
 * object, in both directions. That is the entire protocol, so this class needs
 * nothing outside the JDK.
 *
 * <pre>{@code
 * try (AetherMesh mesh = AetherMesh.connect(new Options().port(7100))) {
 *     Published data = mesh.publish(Files.readAllBytes(Path.of("input.bin")));
 *     TaskResult result = mesh.run("hash", "seed".getBytes(), List.of(data.dataId()), List.of());
 *     System.out.println(HexFormat.of().formatHex(result.output()));
 * }
 * }</pre>
 *
 * <p>One instance is not safe for concurrent use: replies are matched to
 * requests by order. Open one connection per thread.
 */
public final class AetherMesh implements AutoCloseable {

    /** Refuses to allocate more than this for one response. */
    public static final int MAX_FRAME_BYTES = 256 * 1024 * 1024;

    private final Socket socket;
    private final DataInputStream in;
    private final OutputStream out;

    private AetherMesh(Socket socket, InputStream in, OutputStream out) {
        this.socket = socket;
        this.in = new DataInputStream(in);
        this.out = out;
    }

    /** The controller answered with an error, or the connection failed. */
    public static final class MeshException extends IOException {
        private static final long serialVersionUID = 1L;

        MeshException(String message) {
            super(message);
        }
    }

    /** A dataset the controller now holds. Identical bytes yield the same id. */
    public record Published(String dataId, long sizeBytes) {
    }

    /**
     * What a task produced.
     *
     * <p>A task that ran and failed has {@code success} false and an
     * {@code error}; only transport and protocol problems throw.
     */
    public record TaskResult(
            String taskId,
            String nodeId,
            boolean success,
            byte[] output,
            double durationMs,
            String error) {
    }

    /** One node in the mesh. {@code labels} is what it claims to be. */
    public record NodeSummary(
            String nodeId,
            String hostname,
            int cpuCores,
            double cpuUsage,
            double memoryUsage,
            Map<String, String> labels) {
    }

    /** Where the controller is and how to authenticate to it. */
    public static final class Options {
        private String host = "127.0.0.1";
        private int port = 7100;
        private String token;
        private Path tlsCaPath;
        private String tlsServerName;
        private int timeoutMillis = 120_000;

        public Options host(String host) {
            this.host = host;
            return this;
        }

        public Options port(int port) {
            this.port = port;
            return this;
        }

        /** Shared secret, when the controller requires one. */
        public Options token(String token) {
            this.token = token;
            return this;
        }

        /** The controller's certificate. Setting it switches to TLS. */
        public Options tlsCaPath(Path path) {
            this.tlsCaPath = path;
            return this;
        }

        /** Name the certificate was issued for. Defaults to the host. */
        public Options tlsServerName(String name) {
            this.tlsServerName = name;
            return this;
        }

        /** How long to wait for one response. */
        public Options timeoutMillis(int millis) {
            this.timeoutMillis = millis;
            return this;
        }
    }

    /** Opens a connection to 127.0.0.1:7100 and completes the handshake. */
    public static AetherMesh connect() throws IOException {
        return connect(new Options());
    }

    /** Opens a connection and completes the handshake. */
    public static AetherMesh connect(Options options) throws IOException {
        Socket socket = options.tlsCaPath == null
                ? new Socket()
                : tlsSocket(options);

        if (!socket.isConnected()) {
            socket.connect(new InetSocketAddress(options.host, options.port), options.timeoutMillis);
        }
        socket.setSoTimeout(options.timeoutMillis);
        if (socket instanceof SSLSocket tls) {
            tls.startHandshake();
        }

        AetherMesh mesh = new AetherMesh(socket, socket.getInputStream(), socket.getOutputStream());
        Map<String, Object> hello = new LinkedHashMap<>();
        hello.put("type", "hello");
        hello.put("token", options.token);

        Map<String, Object> welcome = mesh.request(hello);
        if (!"welcome".equals(welcome.get("type"))) {
            mesh.close();
            throw new MeshException(text(welcome, "message", "handshake refused"));
        }
        return mesh;
    }

    /** Stores data on the controller. Identical bytes yield the same id. */
    public Published publish(byte[] data) throws IOException {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("type", "publish");
        request.put("data", Base64.getEncoder().encodeToString(data));

        Map<String, Object> frame = request(request);
        expect(frame, "published");
        return new Published(text(frame, "data_id", ""), (long) number(frame, "size_bytes"));
    }

    /** Publishes a file, e.g. a compiled {@code .wasm} module. */
    public Published publishFile(Path path) throws IOException {
        return publish(Files.readAllBytes(path));
    }

    /**
     * Runs a built-in task: {@code echo}, {@code hash}, or {@code cpu}.
     *
     * <p>{@code inputs} are ids from {@link #publish}; the mesh moves them to
     * the chosen node only if that node does not already hold them.
     *
     * <p>{@code constraints} say where the task may run at all:
     * {@code gpu=true}, {@code region!=us-east}, or a bare {@code nvme} for a
     * label that only has to be present. A task no node satisfies is refused
     * rather than placed somewhere it was not allowed.
     */
    public TaskResult run(String kind, byte[] payload, List<String> inputs, List<String> constraints)
            throws IOException {
        return submit(kind, payload, inputs, constraints, null);
    }

    /** Runs a built-in task with no inputs and no constraints. */
    public TaskResult run(String kind, byte[] payload) throws IOException {
        return submit(kind, payload, List.of(), List.of(), null);
    }

    /** Runs a WebAssembly module previously published. */
    public TaskResult runWasm(
            String moduleId, byte[] payload, List<String> inputs, List<String> constraints)
            throws IOException {
        return submit("wasm", payload, inputs, constraints, moduleId);
    }

    /** Runs a WebAssembly module with no extra inputs and no constraints. */
    public TaskResult runWasm(String moduleId, byte[] payload) throws IOException {
        return submit("wasm", payload, List.of(), List.of(), moduleId);
    }

    /** Lists the nodes currently in the mesh. */
    public List<NodeSummary> nodes() throws IOException {
        Map<String, Object> frame = request(Map.of("type", "nodes"));
        expect(frame, "nodes");

        List<NodeSummary> nodes = new ArrayList<>();
        if (!(frame.get("nodes") instanceof List<?> entries)) {
            return nodes;
        }

        for (Object entry : entries) {
            if (!(entry instanceof Map<?, ?> raw)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> node = (Map<String, Object>) raw;

            Map<String, String> labels = new LinkedHashMap<>();
            if (node.get("labels") instanceof Map<?, ?> rawLabels) {
                rawLabels.forEach((key, value) -> labels.put(String.valueOf(key), String.valueOf(value)));
            }

            nodes.add(new NodeSummary(
                    text(node, "node_id", ""),
                    text(node, "hostname", ""),
                    (int) number(node, "cpu_cores"),
                    number(node, "cpu_usage"),
                    number(node, "memory_usage"),
                    labels));
        }
        return nodes;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    private TaskResult submit(
            String kind, byte[] payload, List<String> inputs, List<String> constraints, String module)
            throws IOException {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("type", "submit");
        request.put("kind", kind);
        request.put("payload", Base64.getEncoder().encodeToString(payload));
        request.put("inputs", List.copyOf(inputs));
        request.put("constraints", List.copyOf(constraints));
        request.put("module", module);

        Map<String, Object> frame = request(request);
        expect(frame, "result");
        return new TaskResult(
                text(frame, "task_id", ""),
                text(frame, "node_id", ""),
                Boolean.TRUE.equals(frame.get("success")),
                Base64.getDecoder().decode(text(frame, "output", "")),
                number(frame, "duration_ms"),
                frame.get("error") instanceof String message ? message : null);
    }

    private Map<String, Object> request(Map<String, Object> request) throws IOException {
        byte[] payload = Json.write(request).getBytes(StandardCharsets.UTF_8);
        out.write(new byte[] {
                (byte) (payload.length >>> 24),
                (byte) (payload.length >>> 16),
                (byte) (payload.length >>> 8),
                (byte) payload.length,
        });
        out.write(payload);
        out.flush();

        int length = in.readInt();
        if (length < 0 || length > MAX_FRAME_BYTES) {
            throw new MeshException("controller announced a " + Integer.toUnsignedString(length)
                    + " byte frame");
        }

        byte[] body = new byte[length];
        in.readFully(body);
        try {
            return Json.parseObject(new String(body, StandardCharsets.UTF_8));
        } catch (RuntimeException error) {
            throw new MeshException("controller sent invalid JSON: " + error.getMessage());
        }
    }

    private static void expect(Map<String, Object> frame, String type) throws MeshException {
        if (type.equals(frame.get("type"))) {
            return;
        }
        throw new MeshException(text(frame, "message", "expected " + type + ", got " + frame.get("type")));
    }

    private static String text(Map<String, Object> frame, String name, String fallback) {
        return frame.get(name) instanceof String value ? value : fallback;
    }

    private static double number(Map<String, Object> frame, String name) {
        return frame.get(name) instanceof Number value ? value.doubleValue() : 0.0;
    }

    /** A socket trusting only the certificate the caller named. */
    private static Socket tlsSocket(Options options) throws IOException {
        // A self-signed controller is the normal case, so the CA to trust is
        // given explicitly rather than assumed to be in the JDK's store.
        try (InputStream pem = Files.newInputStream(options.tlsCaPath)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null, null);

            int index = 0;
            for (java.security.cert.Certificate certificate : factory.generateCertificates(pem)) {
                trust.setCertificateEntry("controller-" + index++, (X509Certificate) certificate);
            }

            TrustManagerFactory managers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            managers.init(trust);

            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, managers.getTrustManagers(), null);

            String name = options.tlsServerName == null ? options.host : options.tlsServerName;
            SSLSocket socket = (SSLSocket) context.getSocketFactory()
                    .createSocket(options.host, options.port);
            javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            parameters.setServerNames(List.of(new javax.net.ssl.SNIHostName(name)));
            socket.setSSLParameters(parameters);
            return socket;
        } catch (java.security.GeneralSecurityException error) {
            throw new MeshException("TLS setup failed: " + error);
        }
    }
}
