"""Apache Doris integration for Ray Data."""

from ray_doris._api import read_doris
from ray_doris._errors import (
    DorisAmbiguousWriteError,
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisError,
    DorisLabelExistsError,
    DorisMetadataError,
    DorisPermissionError,
    DorisPlanningError,
    DorisReadError,
    DorisSchemaError,
    DorisTableCompatibilityError,
    DorisWriteError,
)
from ray_doris.datasource import DorisDatasource
from ray_doris.write import (
    DorisConnection,
    DorisDatasink,
    DorisTable,
    DorisWriteOptions,
    DorisWriteResult,
    write_doris,
)

__all__ = [
    "DorisAuthenticationError",
    "DorisAmbiguousWriteError",
    "DorisConfigurationError",
    "DorisConnection",
    "DorisDatasource",
    "DorisDatasink",
    "DorisError",
    "DorisLabelExistsError",
    "DorisMetadataError",
    "DorisPermissionError",
    "DorisPlanningError",
    "DorisReadError",
    "DorisSchemaError",
    "DorisTable",
    "DorisTableCompatibilityError",
    "DorisWriteError",
    "DorisWriteOptions",
    "DorisWriteResult",
    "read_doris",
    "write_doris",
]

__version__ = "1.0"
