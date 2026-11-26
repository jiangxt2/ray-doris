"""Public exceptions raised by ray-doris."""

import urllib.error

import pyarrow as pa


def _diagnostic_suffix(error: BaseException) -> str:
    """Describe selected failure types without rendering exception text or paths."""
    if type(error) is urllib.error.URLError and isinstance(error.reason, BaseException):
        error = error.reason
    if isinstance(error, TimeoutError):
        category = "timeout"
    elif isinstance(error, PermissionError):
        category = "permission denied"
    elif isinstance(error, FileNotFoundError):
        category = "not found"
    elif isinstance(error, ConnectionError):
        category = "connection failure"
    elif isinstance(error, pa.ArrowInvalid):
        category = "invalid data"
    elif isinstance(error, OSError):
        category = "I/O failure"
    else:
        return ""
    return f" ({category})"


class DorisError(Exception):
    """Base exception for ray-doris."""


class DorisConfigurationError(DorisError, ValueError):
    """Raised when datasource configuration is invalid."""


class DorisSchemaError(DorisError):
    """Raised when a Doris schema cannot be represented safely in Arrow."""


class DorisPlanningError(DorisError):
    """Raised when tablet planning fails."""

    _native_diagnostic: tuple[str, str]


_NATIVE_DIAGNOSTIC_STAGES = frozenset(
    {"HMS connection", "HMS metadata", "storage metadata", "Parquet footer"}
)
_DIAGNOSTIC_SUFFIXES = frozenset(
    {
        "",
        " (timeout)",
        " (permission denied)",
        " (not found)",
        " (connection failure)",
        " (invalid data)",
        " (I/O failure)",
    }
)


def _with_native_diagnostic(
    error: DorisPlanningError, stage: str, suffix: str
) -> DorisPlanningError:
    """Attach fixed diagnostic fields without changing the public error class."""
    error._native_diagnostic = (stage, suffix)
    return error


def _native_diagnostic(error: BaseException) -> str:
    """Return only explicitly allowed fields for the native fallback warning."""
    if type(error) is not DorisPlanningError:
        return ""
    value = getattr(error, "_native_diagnostic", None)
    if type(value) is not tuple or len(value) != 2:
        return ""
    stage, suffix = value
    if (
        type(stage) is not str
        or stage not in _NATIVE_DIAGNOSTIC_STAGES
        or type(suffix) is not str
        or suffix not in _DIAGNOSTIC_SUFFIXES
    ):
        return ""
    return f"; stage={stage}{suffix}"


class _NativeReadNotApplicableError(DorisPlanningError):
    """A valid table or request outside the automatic file-read profile."""


class _NativeCatalogTypeMismatchError(_NativeReadNotApplicableError):
    """The catalog is visible but belongs to another native reader."""


class _PinnedNativeFallbackError(DorisPlanningError):
    """A known incompatibility after selecting an Iceberg snapshot."""

    def __init__(self, snapshot_id: int, schema: pa.Schema) -> None:
        super().__init__("Iceberg native reader is incompatible with the selected snapshot")
        self.snapshot_id = snapshot_id
        self.schema = schema


class _UnsafeIcebergFallbackError(DorisPlanningError):
    """The selected Iceberg identity or schema changed after planning."""


class DorisAuthenticationError(DorisPlanningError):
    """Raised when Doris rejects credentials."""


class DorisPermissionError(DorisPlanningError):
    """Raised when Doris rejects an operation because permission is missing."""


class DorisReadError(DorisError):
    """Raised when a planned split cannot be read."""


class DorisWriteError(DorisError):
    """Raised when a Doris Stream Load request has a known failure."""


class DorisAmbiguousWriteError(DorisWriteError):
    """Raised when a Stream Load request may have reached Doris with unknown outcome."""


class DorisLabelExistsError(DorisWriteError):
    """Raised when Doris reports that a Stream Load label is already retained."""


class DorisMetadataError(DorisWriteError):
    """Raised when target table metadata is absent or malformed."""


class DorisTableCompatibilityError(DorisWriteError):
    """Raised when the requested operation cannot safely target the table."""
