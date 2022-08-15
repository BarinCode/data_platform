
from datetime import datetime
from decimal import Decimal, getcontext
from sqlite3 import converters
from typing import Any, Dict


getcontext().prec = 8


fields: Dict = {
    "Int8": lambda x: int(x) if x else x,
    "Int16": lambda x: int(x) if x else x,
    "Int32": lambda x: int(x) if x else x,
    "Int64": lambda x: int(x) if x else x,
    "Int128": lambda x: int(x) if x else x,
    "Int256": lambda x: int(x) if x else x,
    "UInt8": lambda x: int(x) if x else x,
    "UInt16": lambda x: int(x) if x else x,
    "UInt32": lambda x: int(x) if x else x,
    "UInt64": lambda x: int(x) if x else x,
    "UInt128": lambda x: int(x) if x else x,
    "UInt256": lambda x: int(x) if x else x,
    "Float32": lambda x: float(x) if x else x,
    "Float64": lambda x: float(x) if x else x,
    "String": lambda x: str(x),
    "Bool": lambda x: bool(x),
    "Nullable": None,
    "UUID": lambda x: str(x),
    "Date": lambda x: datetime.strptime(x, '%Y-%m-%d').date() if x else x,
    "Date32": lambda x: datetime.strptime(x, '%Y-%m-%d').date() if x else x,
    "DateTime": lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') if x else x,
    "DateTime64": lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') if x else x,
    "Decimal": lambda x: Decimal(x) if x else x,
    "Decimal32": lambda x: Decimal(x) if x else x,
    "Decimal64(8)": lambda x: Decimal(x) if x else x,
    "Decimal128(8)": lambda x: Decimal(x) if x else x,
    "Decimal256": lambda x: Decimal(x) if x else x,

    # "Decimal": Decimal,
    # MySQL fields
    # "TINYINT": lambda x: int(x),
    # "SMALLINT": lambda x: int(x),
    # "INT": lambda x: int(x),
    # "BIGINT": lambda x: int(x),
    # "FLOAT": lambda x: float(x),
    # "DOUBLE": lambda x: float(x),
    # "DATE": lambda x: datetime.strptime(x, '%Y-%m-%d').date(),
    # "DATETIME": lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
    # "TIMESTAMP": lambda x: datetime.fromtimestamp(int(x)),
    # "VARCHAR": lambda x: str(x),
    # "CHAR": lambda x: str(x),
}


# def field_converter(table_schema: Dict):
#     return {
#         field_name: fields.get(field_type, lambda x: str(x))
#         for field_name, field_type, in table_schema.items()
#     }


def field_converter(table_schema: Dict):
    return {
        field_name: lambda x: str(x) for field_name, _, in table_schema.items()
    }
