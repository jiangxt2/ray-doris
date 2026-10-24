"""Ray public Datasink adapter for Apache Doris Stream Load."""

from __future__ import annotations

import logging
import pickle
import time
from contextlib import nullcontext
from dataclasses import dataclass, replace
from typing import Any, Iterable, Mapping, Optional, cast

import pyarrow as pa
from ray.data.datasource import Datasink, WriteResult

from ray_doris._compat import validate_datasink_signature
from ray_doris._errors import (
    DorisAmbiguousWriteError,
    DorisConfigurationError,
    DorisWriteError,
)
from ray_doris._logging import (
    DiagnosticSnapshot,
    EventContext,
    ProgressBudget,
    bind,
    emit,
    error_type,
    new_id,
    stage,
)
from ray_doris.write.connection import DorisConnection, DorisTable
from ray_doris.write.metadata import (
    DorisTableMetadata,
    discover_table_metadata,
    validate_write_table,
)
from ray_doris.write.options import DorisWriteOptions
from ray_doris.write.serialization import iter_serialized_batches
from ray_doris.write.stream_load import StreamLoadClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DorisWriteResult:
    """Driver-side sanitized summary of one Ray Datasink write."""

    status: str
    batches: int
    attempted_rows: int
    loaded_rows: int
    filtered_rows: int
    uploaded_bytes: int
    ray_num_rows: int
    ray_size_bytes: int


def _block_to_arrow(block: Any) -> pa.Table:
    """Convert public Ray block representations without importing private block accessors."""
    if isinstance(block, pa.Table):
        return block
    if isinstance(block, pa.RecordBatch):
        return pa.Table.from_batches([block])
    if hasattr(block, "to_arrow") and callable(block.to_arrow):
        converted = block.to_arrow()
        if isinstance(converted, pa.Table):
            return converted
        if isinstance(converted, pa.RecordBatch):
            return pa.Table.from_batches([converted])
    if isinstance(block, list):
        try:
            return pa.Table.from_pylist(block)
        except (TypeError, ValueError, pa.ArrowException):
            pass
    try:
        return pa.Table.from_pandas(block, preserve_index=False)
    except (TypeError, ValueError, pa.ArrowException):
        raise DorisConfigurationError(
            f"Ray Datasink received an unsupported block type: {type(block).__name__}"
        ) from None


def _schema_matches(left: pa.Schema, right: pa.Schema) -> bool:
    return left.equals(right, check_metadata=False)


def _summary_status(statuses: Iterable[str]) -> str:
    normalized = set(statuses)
    if not normalized or normalized == {"Success"}:
        return "success"
    if normalized == {"Publish Timeout"}:
        return "publish_timeout"
    if normalized.issubset({"Success", "Publish Timeout"}):
        return "mixed"
    raise DorisWriteError("Doris Stream Load returned an unsupported status")


def _summary_integer(summary: Mapping[str, Any], name: str) -> int:
    value = summary[name]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DorisWriteError("Doris Datasink returned an invalid task summary")
    return cast(int, value)


class DorisDatasink(Datasink[Mapping[str, Any]]):
    """Write Ray Arrow or pandas blocks to Doris through HTTP Stream Load."""

    def __init__(
        self,
        connection: DorisConnection,
        table: DorisTable,
        options: Optional[DorisWriteOptions] = None,
    ) -> None:
        validate_datasink_signature()
        self._diagnostics = DiagnosticSnapshot.from_environment()
        self._driver_finished = False
        self._connection = connection
        self._table = table
        self._options = options or DorisWriteOptions()
        self._metadata: Optional[DorisTableMetadata] = None
        self._input_arrow_schema: Optional[pa.Schema] = None
        self._result: Optional[DorisWriteResult] = None
        try:
            pickle.dumps(
                (self._connection, self._table, self._options), protocol=pickle.HIGHEST_PROTOCOL
            )
        except Exception:
            raise DorisConfigurationError(
                "Doris Datasink configuration must be safely serializable"
            ) from None

    @property
    def connection(self) -> DorisConnection:
        return self._connection

    @property
    def table(self) -> DorisTable:
        return self._table

    @property
    def options(self) -> DorisWriteOptions:
        return self._options

    @property
    def result(self) -> Optional[DorisWriteResult]:
        return self._result

    @property
    def min_rows_per_write(self) -> Optional[int]:
        """Tell Ray to bundle approximately one connector batch per write call."""
        return self._options.batch_rows

    def get_name(self) -> str:
        return "Apache Doris Stream Load"

    def name(self) -> str:
        return self.get_name()

    def _begin_driver_write(self) -> None:
        # A later empty or upstream-failed write may have no on_write_start.
        # Rotate only after the preceding write reached its terminal boundary.
        if self._driver_finished:
            self._diagnostics = replace(self._diagnostics, operation_id=new_id())
            self._driver_finished = False

    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        """Discover metadata and validate the first schema when Ray supplies one."""
        validate_datasink_signature()
        self._begin_driver_write()
        with bind(EventContext(self._diagnostics)):
            with stage(logger, "doris.write.prepare", "metadata"):
                metadata = discover_table_metadata(self._connection, self._table)
            if schema is not None:
                with stage(logger, "doris.write.prepare", "schema"):
                    validate_write_table(
                        metadata, operation=self._options.operation, arrow_schema=schema
                    )
                self._input_arrow_schema = schema
            self._metadata = metadata

    def write(self, blocks: Iterable[Any], ctx: Any) -> Mapping[str, Any]:
        """Observe one physical write without modifying Ray retries or return values."""
        context = EventContext(
            self._diagnostics,
            role="worker",
            execution_id=new_id(),
            progress=ProgressBudget(self._diagnostics.log_batches),
        )
        statistics: dict[str, int] = {}
        started = time.monotonic()
        with bind(context):
            emit(logger, logging.DEBUG, "doris.write.started", phase="write", status="started")
            try:
                result = self._write(blocks, ctx, statistics)
            except BaseException as error:
                emit(
                    logger,
                    logging.ERROR,
                    "doris.write.finished",
                    context=context,
                    phase="write",
                    status="failed",
                    error_type=error_type(error),
                    outcome="ambiguous" if isinstance(error, DorisAmbiguousWriteError) else None,
                    counts_scope="confirmed_batches" if statistics else None,
                    duration_ms=(time.monotonic() - started) * 1000,
                    **statistics,
                )
                raise
            emit(
                logger,
                logging.DEBUG,
                "doris.write.finished",
                context=context,
                phase="write",
                status=result["status"],
                duration_ms=(time.monotonic() - started) * 1000,
                **statistics,
            )
            return result

    def _write(
        self, blocks: Iterable[Any], ctx: Any, statistics: dict[str, int]
    ) -> Mapping[str, Any]:
        """Consume one Ray write bundle and return one serializable task summary."""
        # Task identity is intentionally unused; the write contract does not depend on
        # Ray-private context fields.
        del ctx
        metadata = self._metadata
        if metadata is None:
            raise DorisConfigurationError(
                "Doris Datasink on_write_start() must complete before write()"
            )
        statuses: list[str] = []
        batches = 0
        attempted_rows = 0
        loaded_rows = 0
        filtered_rows = 0
        uploaded_bytes = 0
        prepared_once = False
        for block in blocks:
            with (
                stage(logger, "doris.write.prepare", "schema")
                if not prepared_once
                else nullcontext()
            ):
                arrow_table = _block_to_arrow(block)
                if self._input_arrow_schema is None:
                    self._input_arrow_schema = arrow_table.schema
                elif not _schema_matches(arrow_table.schema, self._input_arrow_schema):
                    raise DorisConfigurationError(
                        "Doris Datasink received inconsistent block schemas"
                    )
                prepared = validate_write_table(
                    metadata,
                    operation=self._options.operation,
                    arrow_table=arrow_table,
                )
                if prepared is not None:
                    arrow_table = prepared
            serialized = iter_serialized_batches(
                arrow_table,
                format=self._options.format or "parquet",
                max_rows=self._options.batch_rows,
                max_bytes=self._options.batch_bytes,
            )
            while True:
                # Time only the first next(), without retaining a separate first
                # payload for the lifetime of this block.
                with (
                    stage(logger, "doris.write.prepare", "serialization")
                    if not prepared_once
                    else nullcontext()
                ):
                    batch = next(serialized, None)
                prepared_once = True
                if batch is None:
                    break
                result = StreamLoadClient(self._connection, self._table, self._options).load(
                    batch.payload, rows=batch.rows, columns=batch.columns
                )
                statuses.append(result.status)
                batches += 1
                attempted_rows += batch.rows
                loaded_rows += result.loaded_rows
                filtered_rows += result.filtered_rows
                uploaded_bytes += len(batch.payload)
                statistics.update(
                    batches=batches,
                    attempted_rows=attempted_rows,
                    loaded_rows=loaded_rows,
                    filtered_rows=filtered_rows,
                    uploaded_bytes=uploaded_bytes,
                )
        statistics.update(
            batches=batches,
            attempted_rows=attempted_rows,
            loaded_rows=loaded_rows,
            filtered_rows=filtered_rows,
            uploaded_bytes=uploaded_bytes,
        )
        return {
            "status": _summary_status(statuses),
            "request_statuses": tuple(statuses),
            "batches": batches,
            "attempted_rows": attempted_rows,
            "loaded_rows": loaded_rows,
            "filtered_rows": filtered_rows,
            "uploaded_bytes": uploaded_bytes,
        }

    def on_write_complete(self, write_result: WriteResult[Mapping[str, Any]]) -> None:
        """Emit success only after validating every sanitized task summary."""
        self._begin_driver_write()
        self._complete(write_result)
        result = self._result
        if result is not None:
            emit(
                logger,
                logging.INFO,
                "doris.write.finished",
                context=EventContext(self._diagnostics),
                phase="write",
                status=result.status,
                batches=result.batches,
                attempted_rows=result.attempted_rows,
                loaded_rows=result.loaded_rows,
                filtered_rows=result.filtered_rows,
                uploaded_bytes=result.uploaded_bytes,
            )
            self._driver_finished = True

    def on_write_failed(self, error: Exception) -> None:
        """Best effort: failed tasks may never return their partial counters."""
        self._begin_driver_write()
        self._driver_finished = True
        emit(
            logger,
            logging.ERROR,
            "doris.write.finished",
            context=EventContext(self._diagnostics),
            phase="write",
            status="failed",
            error_type=error_type(error),
            outcome="ambiguous" if isinstance(error, DorisAmbiguousWriteError) else None,
        )

    def _complete(self, write_result: WriteResult[Mapping[str, Any]]) -> None:
        """Aggregate Doris summaries separately from Ray's standard accounting."""
        summaries = list(write_result.write_returns)
        statuses: list[str] = []
        batches = 0
        attempted_rows = 0
        loaded_rows = 0
        filtered_rows = 0
        uploaded_bytes = 0
        for summary in summaries:
            if not isinstance(summary, Mapping):
                raise DorisWriteError("Doris Datasink returned an invalid task summary")
            try:
                raw_statuses = summary["request_statuses"]
                if isinstance(raw_statuses, (str, bytes)):
                    raise TypeError
                task_statuses = tuple(raw_statuses)
                if any(status not in ("Success", "Publish Timeout") for status in task_statuses):
                    raise DorisWriteError("Doris Datasink returned an invalid task status")
                statuses.extend(task_statuses)
                batches += _summary_integer(summary, "batches")
                attempted_rows += _summary_integer(summary, "attempted_rows")
                loaded_rows += _summary_integer(summary, "loaded_rows")
                filtered_rows += _summary_integer(summary, "filtered_rows")
                uploaded_bytes += _summary_integer(summary, "uploaded_bytes")
            except (KeyError, TypeError, ValueError):
                raise DorisWriteError("Doris Datasink returned an invalid task summary") from None
        self._result = DorisWriteResult(
            _summary_status(statuses),
            batches,
            attempted_rows,
            loaded_rows,
            filtered_rows,
            uploaded_bytes,
            int(write_result.num_rows),
            int(write_result.size_bytes),
        )


__all__ = ["DorisDatasink", "DorisWriteResult"]
