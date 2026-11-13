"""Inject one transient Iceberg read failure in the isolated Ray cluster."""

import os


def setup():
    from pyiceberg.io.pyarrow import ArrowScan

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
