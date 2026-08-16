"""Public exceptions raised by ray-doris."""


class DorisError(Exception):
    """Base exception for ray-doris."""


class DorisConfigurationError(DorisError, ValueError):
    """Raised when datasource configuration is invalid."""


class DorisSchemaError(DorisError):
    """Raised when a Doris schema cannot be represented safely in Arrow."""


class DorisPlanningError(DorisError):
    """Raised when tablet planning fails."""


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
