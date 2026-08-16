"""Secure, non-replaying Apache Doris Stream Load client."""

from __future__ import annotations

import base64
import contextlib
import http.client
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, NoReturn, Optional

from ray_doris._errors import (
    DorisAmbiguousWriteError,
    DorisLabelExistsError,
    DorisWriteError,
)
from ray_doris.write.connection import DorisConnection, DorisTable
from ray_doris.write.options import DorisWriteOptions

_MAX_RESPONSE_BYTES = 16 * 1024 * 1024


class _MalformedStreamLoadResponseError(DorisWriteError):
    """A response that cannot establish the outcome of a transmitted request."""


@dataclass(frozen=True)
class DorisLoadResult:
    """Sanitized result of one physical Stream Load request."""

    status: str
    loaded_rows: int
    filtered_rows: int
    total_rows: int


@dataclass
class _RequestTransmission:
    """Track whether request headers and body reached the socket."""

    header_sent: bool = False
    body_started: bool = False


class _TrackedRequest(urllib.request.Request):
    """A request carrying private transmission state for failure classification."""

    def __init__(
        self,
        url: str,
        *,
        transmission: _RequestTransmission,
        data: bytes,
        method: str,
        headers: dict[str, str],
    ) -> None:
        super().__init__(url, data=data, method=method, headers=headers)
        self.transmission = transmission


class _TrackingHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host: str, *, transmission: _RequestTransmission, **kwargs: Any) -> None:
        self._transmission = transmission
        super().__init__(host, **kwargs)

    def send(self, data: Any) -> None:
        if self._transmission.header_sent:
            self._transmission.body_started = True
        else:
            self._transmission.header_sent = True
        super().send(data)


class _TrackingHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host: str, *, transmission: _RequestTransmission, **kwargs: Any) -> None:
        self._transmission = transmission
        super().__init__(host, **kwargs)

    def send(self, data: Any) -> None:
        if self._transmission.header_sent:
            self._transmission.body_started = True
        else:
            self._transmission.header_sent = True
        super().send(data)


class _TrackingHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, transmission: _RequestTransmission) -> None:
        super().__init__()
        self._transmission = transmission

    def http_open(self, request: urllib.request.Request) -> Any:
        return self.do_open(
            lambda host, **kwargs: _TrackingHTTPConnection(
                host, transmission=self._transmission, **kwargs
            ),
            request,
        )


class _TrackingHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, transmission: _RequestTransmission, context: ssl.SSLContext) -> None:
        super().__init__(context=context)
        self._transmission = transmission
        self._ssl_context = context

    def https_open(self, request: urllib.request.Request) -> Any:
        return self.do_open(
            lambda host, **kwargs: _TrackingHTTPSConnection(
                host, transmission=self._transmission, **kwargs
            ),
            request,
            context=self._ssl_context,
        )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Keep FE-to-BE redirect validation inside this client."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def _int_field(payload: dict[str, Any], name: str) -> int:
    if name not in payload:
        raise _MalformedStreamLoadResponseError(
            f"Doris Stream Load response field {name} is missing"
        )
    value = payload[name]
    if isinstance(value, bool):
        raise _MalformedStreamLoadResponseError(
            f"Doris Stream Load response field {name} is invalid"
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise _MalformedStreamLoadResponseError(
            f"Doris Stream Load response field {name} is invalid"
        )
    if parsed < 0:
        raise _MalformedStreamLoadResponseError(
            f"Doris Stream Load response field {name} is invalid"
        )
    return parsed


def _validate_redirect_target(connection: DorisConnection, newurl: str) -> str:
    """Validate and sanitize one FE-to-BE redirect before resending the body."""
    parsed = urllib.parse.urlsplit(newurl)
    try:
        redirect_port = parsed.port
    except ValueError:
        raise DorisWriteError("Doris Stream Load redirect target has an invalid port") from None
    hostname = parsed.hostname
    allowed_hosts = {host.casefold() for host in connection.allowed_redirect_hosts()}
    if (
        parsed.scheme not in ("http", "https")
        or hostname is None
        or hostname.casefold() not in allowed_hosts
        or redirect_port not in connection.allowed_redirect_ports()
    ):
        raise DorisWriteError("Doris Stream Load redirect target is not allowlisted")
    if connection.http_secure and parsed.scheme != "https":
        raise DorisWriteError("Doris Stream Load HTTPS connection cannot redirect to HTTP")
    rendered_host = f"[{hostname}]" if ":" in hostname else hostname
    return urllib.parse.urlunsplit(
        (parsed.scheme, f"{rendered_host}:{redirect_port}", parsed.path, parsed.query, "")
    )


class StreamLoadClient:
    """One request-scoped client; it never retries an ambiguous request."""

    def __init__(
        self, connection: DorisConnection, table: DorisTable, options: DorisWriteOptions
    ) -> None:
        self._connection = connection
        self._table = table
        self._options = options

    def load(self, payload: bytes, *, rows: int, columns: tuple[str, ...]) -> DorisLoadResult:
        label = self._options.label()
        headers = self._headers(label, columns)
        transmission = _RequestTransmission()
        request = _TrackedRequest(
            self._connection.endpoint(self._table),
            transmission=transmission,
            data=payload,
            method="PUT",
            headers=headers,
        )
        try:
            return self._open_and_parse(self._build_opener(transmission), request, rows)
        except DorisWriteError:
            raise
        except urllib.error.HTTPError as exc:
            if exc.code in (301, 302, 303, 307, 308):
                location = exc.headers.get("Location")
                self._close_http_error(exc)
                if not location:
                    raise DorisWriteError(
                        "Doris Stream Load redirect omitted its location"
                    ) from None
                redirected_transmission = _RequestTransmission()
                redirected = _TrackedRequest(
                    _validate_redirect_target(self._connection, location),
                    transmission=redirected_transmission,
                    data=payload,
                    method="PUT",
                    headers=headers,
                )
                try:
                    return self._open_and_parse(
                        self._build_opener(redirected_transmission), redirected, rows
                    )
                except urllib.error.HTTPError as redirected_error:
                    self._raise_http_error(
                        redirected_error, rows=rows, transmission=redirected_transmission
                    )
                except (TimeoutError, urllib.error.URLError, OSError, http.client.HTTPException):
                    self._raise_transport_error(redirected_transmission)
            self._raise_http_error(exc, rows=rows, transmission=transmission)
        except (TimeoutError, urllib.error.URLError, OSError, http.client.HTTPException):
            self._raise_transport_error(transmission)

    def _build_opener(self, transmission: _RequestTransmission) -> urllib.request.OpenerDirector:
        handlers: list[Any] = [
            urllib.request.ProxyHandler({}),
            _NoRedirectHandler(),
            _TrackingHTTPHandler(transmission),
        ]
        if self._connection.http_secure:
            context = ssl.create_default_context(cafile=self._connection.http_ca_file)
            handlers.append(_TrackingHTTPSHandler(transmission, context))
        return urllib.request.build_opener(*handlers)

    @staticmethod
    def _raise_transport_error(transmission: _RequestTransmission) -> NoReturn:
        if transmission.body_started:
            raise DorisAmbiguousWriteError(
                "Doris Stream Load request status is unknown; automatic replay is disabled"
            ) from None
        raise DorisWriteError(
            "Doris Stream Load request failed before request body transmission"
        ) from None

    def _raise_http_error(
        self,
        error: urllib.error.HTTPError,
        *,
        rows: int,
        transmission: _RequestTransmission,
    ) -> NoReturn:
        try:
            body = self._read_error_body(error)
            result = self._parse_response(body, rows=rows, http_error=error.code)
        except _MalformedStreamLoadResponseError as malformed:
            self._raise_response_error(transmission, malformed, http_status=error.code)
        raise DorisWriteError(f"Doris Stream Load failed with status {result.status}") from None

    def _timeout(self) -> float:
        return float(
            self._options.request_timeout_seconds or self._connection.request_timeout_seconds
        )

    def _open_and_parse(
        self,
        opener: urllib.request.OpenerDirector,
        request: _TrackedRequest,
        rows: int,
    ) -> DorisLoadResult:
        response = opener.open(request, timeout=self._timeout())
        try:
            try:
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
                if len(raw) > _MAX_RESPONSE_BYTES:
                    raise _MalformedStreamLoadResponseError(
                        "Doris Stream Load response is too large"
                    )
                return self._parse_response(raw, rows=rows)
            except _MalformedStreamLoadResponseError as error:
                self._raise_response_error(request.transmission, error)
        finally:
            with contextlib.suppress(Exception):
                response.close()

    @staticmethod
    def _raise_response_error(
        transmission: _RequestTransmission,
        error: _MalformedStreamLoadResponseError,
        *,
        http_status: Any = None,
    ) -> NoReturn:
        if transmission.body_started and http_status is not None and 400 <= http_status < 500:
            raise DorisWriteError(
                f"Doris Stream Load returned malformed HTTP {http_status} response"
            ) from None
        if transmission.body_started:
            raise DorisAmbiguousWriteError(
                "Doris Stream Load response is invalid; request status is unknown"
            ) from None
        raise DorisWriteError(str(error)) from None

    @staticmethod
    def _close_http_error(error: urllib.error.HTTPError) -> None:
        with contextlib.suppress(Exception):
            error.close()

    def _headers(self, label: str, columns: tuple[str, ...]) -> dict[str, str]:
        token = base64.b64encode(
            f"{self._connection.username}:{self._connection.resolve_password()}".encode()
        ).decode("ascii")
        headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/octet-stream",
            "Expect": "100-continue",
            "label": label,
            "format": self._options.format or "parquet",
            "max_filter_ratio": str(self._options.max_filter_ratio),
            "strict_mode": str(self._options.strict_mode).lower(),
        }
        if self._connection.redirect_policy:
            headers["redirect-policy"] = self._connection.redirect_policy
        if self._options.operation == "partial_update":
            headers["partial_columns"] = "true"
            headers["read_json_by_line"] = "true"
            headers["columns"] = ",".join(f"`{column.replace('`', '``')}`" for column in columns)
        headers.update(dict(self._options.load_properties))
        return headers

    @staticmethod
    def _read_error_body(error: urllib.error.HTTPError) -> bytes:
        try:
            body = error.read(_MAX_RESPONSE_BYTES + 1)
            if len(body) > _MAX_RESPONSE_BYTES:
                raise _MalformedStreamLoadResponseError(
                    "Doris Stream Load error response is too large"
                )
            return body
        except OSError:
            return b""
        finally:
            with contextlib.suppress(Exception):
                error.close()

    @staticmethod
    def _parse_response(
        raw: bytes,
        *,
        rows: int,
        http_error: Optional[int] = None,
    ) -> DorisLoadResult:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise _MalformedStreamLoadResponseError(
                "Doris Stream Load returned a non-JSON response"
            ) from None
        if not isinstance(payload, dict):
            raise _MalformedStreamLoadResponseError(
                "Doris Stream Load returned an invalid response"
            )
        status_value = payload.get("Status", payload.get("status", "Fail"))
        if not isinstance(status_value, str):
            raise _MalformedStreamLoadResponseError("Doris Stream Load response status is invalid")
        normalized = " ".join(status_value.casefold().replace("_", " ").split())
        status = {"success": "Success", "publish timeout": "Publish Timeout"}.get(
            normalized, status_value
        )
        if normalized == "label already exists":
            raise DorisLabelExistsError("Doris Stream Load label is already retained")
        if normalized not in {"success", "publish timeout"}:
            suffix = f" (HTTP {http_error})" if http_error is not None else ""
            raise DorisWriteError(f"Doris Stream Load failed{suffix}")
        loaded_rows = _int_field(payload, "NumberLoadedRows")
        filtered_rows = _int_field(payload, "NumberFilteredRows")
        total_rows = _int_field(payload, "NumberTotalRows")
        if total_rows != rows or loaded_rows + filtered_rows > total_rows:
            raise _MalformedStreamLoadResponseError(
                "Doris Stream Load response row counts are invalid"
            )
        return DorisLoadResult(status, loaded_rows, filtered_rows, total_rows)
