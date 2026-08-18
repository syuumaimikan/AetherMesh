"""A twenty-line OTLP/HTTP collector, for checking that traces leave the process.

Not a tracing backend. It accepts `POST /v1/traces`, prints one line per span,
and exits when you stop it — enough to see that the controller is exporting
what it says it is, without installing anything.

    python collector.py                 # listens on 127.0.0.1:4318

Then start a controller built with --features otel and point it here:

    aether-controller --otlp-endpoint http://127.0.0.1:4318/v1/traces
"""

import gzip
import json
import sys
from collections import Counter
from http.server import BaseHTTPRequestHandler, HTTPServer

SEEN = Counter()


class Collector(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            print("not JSON: start the controller with the http-json protocol")
            return

        for resource in payload.get("resourceSpans", []):
            service = "?"
            for attribute in resource.get("resource", {}).get("attributes", []):
                if attribute.get("key") == "service.name":
                    service = attribute.get("value", {}).get("stringValue", "?")
            for scope in resource.get("scopeSpans", []):
                for span in scope.get("spans", []):
                    SEEN[span.get("name", "?")] += 1
                    micros = (
                        int(span.get("endTimeUnixNano", 0))
                        - int(span.get("startTimeUnixNano", 0))
                    ) / 1000
                    fields = {
                        attribute["key"]: list(attribute["value"].values())[0]
                        for attribute in span.get("attributes", [])
                    }
                    interesting = {
                        key: value
                        for key, value in fields.items()
                        if key in {"task_id", "kind", "node_id", "inputs", "attempts"}
                    }
                    print(
                        f"{service}  {span.get('name'):<12} {micros:8.0f} us  "
                        f"trace={span.get('traceId', '')[:16]} {interesting}"
                    )

    # Quiet: one line per span is the output, not one per request.
    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 4318
    server = HTTPServer(("127.0.0.1", port), Collector)
    print(f"listening on http://127.0.0.1:{port}/v1/traces — ctrl-c to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nspans received:")
        for name, count in SEEN.most_common():
            print(f"  {name:<12} {count}")
