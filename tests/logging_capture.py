"""Opt-in Ray worker hook for cross-process diagnostic assertions."""

import json
import logging
import os
from pathlib import Path

KEYS = (
    "event",
    "phase",
    "status",
    "role",
    "operation_id",
    "plan_id",
    "split_id",
    "execution_id",
    "transport",
    "label",
    "send_state",
    "emitted_rows",
    "batches",
    "loaded_rows",
    "uploaded_bytes",
    "outcome",
    "counts_scope",
    "error_type",
    "batch_index",
)


class Capture(logging.Handler):
    def __init__(self, directory):
        super().__init__(logging.DEBUG)
        self.path = Path(directory) / f"diagnostics-{os.getpid()}.jsonl"

    def emit(self, record):
        if not record.name.startswith("ray_doris."):
            return
        values = {key: record.__dict__[key] for key in KEYS if key in record.__dict__}
        values["message"] = record.getMessage()
        with self.path.open("a") as output:
            output.write(json.dumps(values) + "\n")


def install(directory):
    logger = logging.getLogger("ray_doris")
    logger.setLevel(logging.DEBUG)
    if not any(isinstance(handler, Capture) for handler in logger.handlers):
        logger.addHandler(Capture(directory))


def records(directory):
    return [
        json.loads(line)
        for path in Path(directory).glob("diagnostics-*.jsonl")
        for line in path.read_text().splitlines()
    ]


def setup():
    install(os.environ["RAY_DORIS_TEST_LOG_DIR"])
