"""Internal, bounded diagnostics; applications own logging configuration."""

from __future__ import annotations

import logging
import math
import os
import re
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Iterator, Optional, Union
from uuid import uuid4

from ray_doris import _errors

Value = Union[str, int, float, bool, None]
_FIELDS = frozenset(
    "phase status role duration_ms transport from_transport to_transport reason_code "
    "path experimental catalog_kind cache_hit task_count tablet_count parallelism "
    "tablet_size batch_size concurrency override_num_blocks batch_index "
    "emitted_rows emitted_blocks arrow_bytes "
    "cleanup_error resource_kind error_type from_plan to_plan label send_state "
    "attempted_rows loaded_rows filtered_rows uploaded_bytes batches outcome counts_scope "
    "suppressed_progress partition_count file_count origin_count origins fields_truncated".split()
)
_IDENTIFIER = re.compile(r"[A-Za-z0-9_]+\Z")
_CURRENT: ContextVar[Optional[EventContext]] = ContextVar("ray_doris_diagnostics", default=None)


def new_id() -> str:
    return uuid4().hex


@dataclass(frozen=True)
class DiagnosticSnapshot:
    """Only simple, non-business values travel with a datasource or sink."""

    log_batches: bool = False
    operation_id: str = field(default_factory=new_id)
    plan_id: Optional[str] = None
    split_id: Optional[str] = None

    @classmethod
    def from_environment(cls) -> DiagnosticSnapshot:
        value = os.environ.get("RAY_DORIS_LOG_BATCHES", "0")
        if value not in ("0", "1"):
            raise _errors.DorisConfigurationError("RAY_DORIS_LOG_BATCHES must be 0 or 1")
        return cls(log_batches=value == "1")


@dataclass
class EventContext:
    snapshot: DiagnosticSnapshot
    role: str = "driver"
    execution_id: Optional[str] = None
    transport: Optional[str] = None
    cleanup_error: bool = False
    request_index: int = 0
    progress: Optional[ProgressBudget] = None


def current_context() -> Optional[EventContext]:
    return _CURRENT.get()


@contextmanager
def bind(context: EventContext) -> Iterator[None]:
    token = _CURRENT.set(context)
    try:
        yield
    finally:
        _CURRENT.reset(token)


def error_type(error: BaseException) -> str:
    """Never serialize an exception message, repr, chain or caller-defined class name."""
    for cls in (
        _errors.DorisAmbiguousWriteError,
        _errors.DorisLabelExistsError,
        _errors.DorisAuthenticationError,
        _errors.DorisPermissionError,
        _errors.DorisConfigurationError,
        _errors.DorisSchemaError,
        _errors.DorisPlanningError,
        _errors.DorisReadError,
        _errors.DorisWriteError,
        TimeoutError,
        OSError,
        ValueError,
        TypeError,
    ):
        if isinstance(error, cls):
            return cls.__name__
    return "other"


def emit(
    logger: logging.Logger,
    level: int,
    event: str,
    *,
    context: Optional[EventContext] = None,
    **values: Value,
) -> None:
    if not logger.isEnabledFor(level):
        return
    active = context or current_context()
    extra: dict[str, Value] = {"event": event}
    if active is not None:
        extra["role"] = active.role
        for key in ("operation_id", "plan_id", "split_id"):
            value = getattr(active.snapshot, key)
            if isinstance(value, str) and len(value) <= 128 and _IDENTIFIER.fullmatch(value):
                extra[key] = value
        if active.execution_id is not None:
            extra["execution_id"] = active.execution_id
        if active.transport is not None:
            extra["transport"] = active.transport
    for key, value in values.items():
        if key not in _FIELDS or value is None:
            continue
        if isinstance(value, str):
            if len(value) > 128:
                extra["fields_truncated"] = True
                continue
            value = value.replace("\r", "").replace("\n", "")
        elif not isinstance(value, (int, float, bool)):
            continue
        elif isinstance(value, float) and not math.isfinite(value):
            continue
        extra[key] = value
    # Logging is observational, including with a caller-owned failing handler.
    try:
        logger.log(
            level,
            "event=%s phase=%s status=%s",
            event,
            extra.get("phase", ""),
            extra.get("status", ""),
            extra=extra,
        )
    except Exception:
        return


@contextmanager
def stage(logger: logging.Logger, event: str, phase: str) -> Iterator[None]:
    started = time.monotonic()
    emit(logger, logging.DEBUG, event + ".started", phase=phase, status="started")
    try:
        yield
    except BaseException as error:
        emit(
            logger,
            logging.ERROR,
            event + ".finished",
            phase=phase,
            status="failed",
            duration_ms=(time.monotonic() - started) * 1000,
            error_type=error_type(error),
        )
        raise
    else:
        emit(
            logger,
            logging.DEBUG,
            event + ".finished",
            phase=phase,
            status="succeeded",
            duration_ms=(time.monotonic() - started) * 1000,
        )


@dataclass
class ProgressBudget:
    """Constant state, local to one physical execution and one progress kind."""

    enabled: bool
    seen: int = 0
    emitted: int = 0
    last_time: float = 0.0

    def allow(self, logger: logging.Logger) -> bool:
        if not self.enabled or not logger.isEnabledFor(logging.DEBUG):
            return False
        self.seen += 1
        if self.emitted >= 100 or (self.seen != 1 and self.seen % 100 != 0):
            return False
        now = time.monotonic()
        if self.emitted and now - self.last_time < 1:
            return False
        self.last_time = now
        self.emitted += 1
        return True
