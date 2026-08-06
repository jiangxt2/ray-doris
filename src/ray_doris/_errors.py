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
