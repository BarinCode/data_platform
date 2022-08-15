from csv import DictReader
from os import path
from typing import Any, Callable, Dict, List, Optional

from clickhouse_driver import Client
from loguru import logger

from apps.core.enums import WriteTypeEnum
from apps.core.fields import field_converter


# TODO: use SQL template file instead.
drop_table_sql = """
DROP TABLE IF EXISTS {database_name}.{table_name}
"""

create_table_sql = """
CREATE TABLE {database_name}.{table_name} (
    {fields_string}
) ENGINE = {engine}
"""

insert_into_sql = """
INSERT INTO {database_name}.{table_name} VALUES
"""


def get_value(
    schema: Dict[str, str],
    converter: Dict[str, Callable],
    field_name: str,
    field_value: str,
):
    # if schema[field_name].find("Decimal") and field_value == "":
    #     return None

    if field_value == "":
        field_value == "NULL"

    return converter.get(field_name, lambda x: str(x))(field_value)


def row_reader(schema: Dict, row: Dict):
    converter = field_converter(schema)
    return {
        name: get_value(schema, converter, name, value) for name, value in row.items()
    }
