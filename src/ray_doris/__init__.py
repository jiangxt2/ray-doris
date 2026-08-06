"""Apache Doris integration for Ray Data."""

from ray_doris._api import read_doris
from ray_doris._errors import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisError,
    DorisPermissionError,
    DorisPlanningError,
    DorisReadError,
    DorisSchemaError,
)
from ray_doris.datasource import DorisDatasource

__all__ = [
    "DorisAuthenticationError",
    "DorisConfigurationError",
    "DorisDatasource",
    "DorisError",
    "DorisPermissionError",
    "DorisPlanningError",
    "DorisReadError",
    "DorisSchemaError",
    "read_doris",
]

__version__ = "0.1.0a1"
