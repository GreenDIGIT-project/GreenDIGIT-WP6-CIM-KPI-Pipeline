import http.server
import json
import urllib.error
import urllib.request

# Configuration
LISTEN_PORT = 8012
TARGET_URL = "http://kpi-service:8011/transform-and-forward"

# The hardcoded mock payload
MOCK_PAYLOAD = {
    "site": "AEGIS01-IPB-SCL",
    "duration_s": 3590,
    "sites": {"site_type": "cloud"},
    "fact_site_event": {
        "site": "AEGIS01-IPB-SCL",
        "event_start_timestamp": "2025-01-01T12:00:00Z",
        "event_end_timestamp": "2025-01-01T13:00:00Z",
        "execunitfinished": "true",
        "job_finished": "true",
        "startexectime": "2025-01-01T12:00:05Z",
        "stopexectime": "2025-01-01T12:59:55Z",
        "execunitid": "compute-12345",
    },
    "detail_cloud": {"execunitid": "compute-12345"},
}


class DebugHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        print(f"\n--- [DEBUG] Received POST request on {self.path} ---")

        # 1. Check Incoming Headers
        auth_header = self.headers.get("Authorization")
        if auth_header:
            print(f"[DEBUG] Incoming Authorization header found: {auth_header[:15]}...")
        else:
            print(
                "[DEBUG] !!! WARNING: No Authorization header received from Publisher !!!"
            )

        # 2. Drain input (we don't use it, but we must read it)
        length = int(self.headers.get("content-length", 0))
        self.rfile.read(length)

        # 3. Prepare Outgoing Request
        print(f"[DEBUG] Preparing to forward to: {TARGET_URL}")

        # SERIALIZATION CHECK
        try:
            data_bytes = json.dumps(MOCK_PAYLOAD).encode("utf-8")
            print(f"[DEBUG] Payload size: {len(data_bytes)} bytes")
            # print(f"[DEBUG] Payload content: {json.dumps(MOCK_PAYLOAD)}") # Uncomment if you need to see the full string
        except Exception as e:
            print(f"[DEBUG] !!! JSON Serialization Failed: {e}")
            self.send_error(500, "JSON Serialization Failed")
            return

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "cim-service-mock/1.0",
        }
        if auth_header:
            headers["Authorization"] = auth_header

        req = urllib.request.Request(TARGET_URL, data=data_bytes, headers=headers)

        # 4. Execute Forwarding
        try:
            with urllib.request.urlopen(req) as response:
                response_body = response.read().decode("utf-8")
                print(f"[DEBUG] SUCCESS! Upstream responded: {response.status}")
                print(f"[DEBUG] Response body: {response_body}")

                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"Forwarded OK")

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            print(f"[DEBUG] !!! UPSTREAM ERROR: {e.code} {e.reason}")
            print("vvvvvvvvv REMOTE ERROR DETAILS vvvvvvvvv")
            print(error_body)
            print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

            self.send_response(e.code)
            self.end_headers()
            self.wfile.write(error_body.encode("utf-8"))

        except Exception as e:
            print(f"[DEBUG] !!! NETWORK/INTERNAL ERROR: {e}")
            self.send_error(500, str(e))


print(f"cim-service (debug mode) starting on port {LISTEN_PORT}...")
http.server.HTTPServer(("0.0.0.0", LISTEN_PORT), DebugHandler).serve_forever()
