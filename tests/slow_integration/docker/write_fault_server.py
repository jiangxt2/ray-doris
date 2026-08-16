from __future__ import annotations

import os
import socket
import socketserver
from pathlib import Path

PORT = int(os.environ.get("DORIS_STREAM_LOAD_FAULT_PORT", "18445"))
READY_MARKER = Path("/state/write-fault-ready")
BODY_MARKER = Path("/state/write-fault-body-started")


class _FaultHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        self.connection.settimeout(30)
        request_line = self.rfile.readline()
        if not request_line:
            return

        headers: dict[str, str] = {}
        while True:
            line = self.rfile.readline()
            if line in (b"", b"\r\n"):
                break
            name, separator, value = line.decode("latin-1").partition(":")
            if separator:
                headers[name.lower()] = value.strip()

        if headers.get("expect", "").lower() == "100-continue":
            self.wfile.write(b"HTTP/1.1 100 Continue\r\n\r\n")
            self.wfile.flush()

        try:
            content_length = int(headers.get("content-length", "0"))
        except ValueError:
            content_length = 0
        if content_length <= 0 or not self.rfile.read(1):
            return

        BODY_MARKER.write_text("body-started\n", encoding="utf-8")
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


class _FaultServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> None:
    READY_MARKER.unlink(missing_ok=True)
    BODY_MARKER.unlink(missing_ok=True)
    with _FaultServer(("0.0.0.0", PORT), _FaultHandler) as server:
        READY_MARKER.write_text("ready\n", encoding="utf-8")
        server.serve_forever(poll_interval=0.2)


if __name__ == "__main__":
    main()
