"""Inject one transient Iceberg read failure in the isolated Ray cluster."""

import os
import re


def is_storage_authorization_error(error):
    """Recognize a storage denial without treating a file path as an HTTP status."""
    if isinstance(error, PermissionError):
        return True
    message = str(error).lower()
    return any(
        part in message
        for part in (
            "access denied",
            "access_denied",
            "accessdenied",
            "signaturedoesnotmatch",
            "signature_does_not_match",
        )
    ) or bool(re.search(r"\b(?:http(?:\s+status)?|status(?:\s+code)?)\s*[:=]?\s*403\b", message))


def setup():
    from pyiceberg.io.pyarrow import ArrowScan

    if "ICEBERG_FALLBACK_MARKER" in os.environ:
        record_fallback()
    marker = os.environ["ICEBERG_RETRY_MARKER"]
    original = ArrowScan.to_record_batches

    def fail_once(scan, tasks):
        try:
            descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return original(scan, tasks)
        os.close(descriptor)
        raise RuntimeError("injected Iceberg read retry")

    ArrowScan.to_record_batches = fail_once


def record_fallback():
    """Record actual coordinator-side Doris reads in the shared evidence directory."""
    import ray

    import ray_doris._iceberg_stream as stream

    marker = os.environ["ICEBERG_FALLBACK_MARKER"]
    original = stream.read_split

    def record(config, split, schema):
        with open(marker, "a", encoding="utf-8") as output:
            output.write(f"{config.snapshot_id} {ray.get_runtime_context().get_node_id()}\n")
        return original(config, split, schema)

    stream.read_split = record
