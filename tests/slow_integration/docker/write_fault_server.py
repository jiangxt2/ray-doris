from __future__ import annotations

import os
import socket
import socketserver
import ssl
import threading
from pathlib import Path

PORT = int(os.environ.get("DORIS_STREAM_LOAD_FAULT_PORT", "18445"))
REDIRECT_PORT = int(os.environ.get("DORIS_STREAM_LOAD_REDIRECT_PORT", "18446"))
HTTPS_PORT = int(os.environ.get("DORIS_STREAM_LOAD_HTTPS_FAULT_PORT", "18447"))
READY_MARKER = Path("/state/write-fault-ready")
BODY_MARKER = Path("/state/write-fault-body-started")
HTTPS_BODY_MARKER = Path("/state/write-fault-https-body-started")
REDIRECT_MARKER = Path("/state/write-fault-redirect")


def _headers(handler: socketserver.StreamRequestHandler) -> dict[str, str]:
    headers = {}
    while True:
        line = handler.rfile.readline()
        if line in (b"", b"\r\n"):
            break
        name, separator, value = line.decode("latin-1").partition(":")
        if separator:
            headers[name.lower()] = value.strip()
    if headers.get("expect", "").lower() == "100-continue":
        handler.wfile.write(b"HTTP/1.1 100 Continue\r\n\r\n")
        handler.wfile.flush()
    return headers


class _FaultHandler(socketserver.StreamRequestHandler):
    marker = BODY_MARKER

    def handle(self) -> None:
        self.connection.settimeout(30)
        request_line = self.rfile.readline()
        if not request_line:
            return

        headers = _headers(self)

        try:
            content_length = int(headers.get("content-length", "0"))
        except ValueError:
            content_length = 0
        if content_length <= 0 or not self.rfile.read(1):
            return

        with self.marker.open("a", encoding="utf-8") as marker:
            marker.write("body-started\n")
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


class _HttpsFaultHandler(_FaultHandler):
    marker = HTTPS_BODY_MARKER


class _RedirectHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        self.connection.settimeout(30)
        if not self.rfile.readline():
            return
        headers = _headers(self)
        length = int(headers.get("content-length", "0"))
        if length <= 0 or len(self.rfile.read(length)) != length:
            return
        with REDIRECT_MARKER.open("a", encoding="utf-8") as marker:
            marker.write("redirected\n")
        self.wfile.write(
            f"HTTP/1.1 307 Temporary Redirect\r\nLocation: https://write-fault:{HTTPS_PORT}/load\r\n"
            "Content-Length: 0\r\nConnection: close\r\n\r\n".encode()
        )
        self.wfile.flush()


class _FaultServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> None:
    READY_MARKER.unlink(missing_ok=True)
    for marker in (BODY_MARKER, HTTPS_BODY_MARKER, REDIRECT_MARKER):
        marker.unlink(missing_ok=True)
    with (
        _FaultServer(("0.0.0.0", PORT), _FaultHandler) as server,
        _FaultServer(("0.0.0.0", REDIRECT_PORT), _RedirectHandler) as redirect,
        _FaultServer(("0.0.0.0", HTTPS_PORT), _HttpsFaultHandler) as https,
    ):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain("/tls/ingress.pem")
        https.socket = context.wrap_socket(https.socket, server_side=True)
        for listener in (redirect, https):
            threading.Thread(target=listener.serve_forever, daemon=True).start()
        READY_MARKER.write_text("ready\n", encoding="utf-8")
        server.serve_forever(poll_interval=0.2)


if __name__ == "__main__":
    main()
