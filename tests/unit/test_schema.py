from decimal import Decimal

import pyarrow as pa
import pytest

from ray_doris._errors import DorisSchemaError
from ray_doris._schema import (
    DorisColumn,
    build_arrow_schema,
    coerce_decimal,
    doris_type_to_arrow,
    parse_describe_rows,
)


@pytest.mark.parametrize(
    ("doris_type", "expected"),
    [
        ("BOOLEAN", pa.bool_()),
        ("TINYINT", pa.int8()),
        ("SMALLINT", pa.int16()),
        ("INT", pa.int32()),
        ("BIGINT", pa.int64()),
        ("LARGEINT", pa.decimal256(39, 0)),
        ("FLOAT", pa.float32()),
        ("DOUBLE", pa.float64()),
        ("VARCHAR(255)", pa.string()),
        ("JSON", pa.string()),
        ("DATEV2", pa.date32()),
        ("DATETIMEV2(6)", pa.timestamp("us")),
        ("DECIMALV3(10,2)", pa.decimal128(10, 2)),
        ("DECIMAL256(76,18)", pa.decimal256(76, 18)),
    ],
)
def test_doris_scalar_type_mapping(doris_type: str, expected: pa.DataType) -> None:
    assert doris_type_to_arrow(doris_type, column_name="value") == expected


@pytest.mark.parametrize(
    "doris_type",
    ["ARRAY<INT>", "MAP<STRING,INT>", "STRUCT<a:INT>", "BITMAP", "DECIMALV3(77,0)"],
)
def test_unsupported_or_invalid_types_fail_closed(doris_type: str) -> None:
    with pytest.raises(DorisSchemaError, match="value"):
        doris_type_to_arrow(doris_type, column_name="value")


def test_describe_rows_and_projection_preserve_order_and_nullability() -> None:
    columns = parse_describe_rows(
        [
            ("id", "BIGINT", "NO", "PRI", None, ""),
            ("amount", "DECIMALV3(20,4)", "YES", "", None, ""),
        ]
    )
    schema = build_arrow_schema(columns, ["amount", "id"])
    assert schema.names == ["amount", "id"]
    assert schema.field("amount").nullable
    assert not schema.field("id").nullable


def test_projection_rejects_unknown_and_duplicate_describe_columns() -> None:
    columns = (DorisColumn("id", "INT", False),)
    with pytest.raises(DorisSchemaError, match="unknown"):
        build_arrow_schema(columns, ["missing"])
    with pytest.raises(DorisSchemaError, match="duplicate"):
        build_arrow_schema(columns + columns)


def test_describe_rows_reject_invalid_shape_and_nullability() -> None:
    with pytest.raises(DorisSchemaError, match="DESCRIBE row"):
        parse_describe_rows([("id", "INT")])
    with pytest.raises(DorisSchemaError, match="nullability"):
        parse_describe_rows([("id", "INT", "UNKNOWN")])
    with pytest.raises(DorisSchemaError, match="no columns"):
        parse_describe_rows([])


def test_decimal_coercion_never_round_trips_through_float() -> None:
    value = Decimal("1234567890.123456")
    assert coerce_decimal(value) is value
    assert coerce_decimal("1234567890.123456") == value
    assert coerce_decimal(None) is None
