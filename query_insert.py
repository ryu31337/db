from dataclasses import dataclass
from datetime import datetime

import re
from typing import Any, List

from db import Database
from query import is_quoted_string, unquote_string


@dataclass
class Insert:
    table: str
    fields: List[str]
    values: List[Any]

    def validate(self, db: Database) -> None:
        if len(self.fields) != len(self.values):
            raise ValueError("Number of fields and values must match")

        if self.table not in [table.name for table in db.tables]:
            raise ValueError(f"Invalid table: {self.table}")

        if "__id" in self.fields:
            raise ValueError("Cannot insert into __id column (autogenerated)")

        table = db.get_table(self.table)
        for index, field in enumerate(self.fields):
            if field not in table.headers:
                raise ValueError(f"Invalid column: {field} in table {self.table}")

            value = self.values[index]
            col = table.get_column(field)
            if col.type == "str":
                if not is_quoted_string(value):
                    raise ValueError(f"Invalid string for column {field}: {value}")
            elif col.type == "int":
                try:
                    int(value)
                except ValueError:
                    raise ValueError(f"Invalid int for column {field}: {value}")
            elif col.type == "float":
                try:
                    float(value)
                except ValueError:
                    raise ValueError(f"Invalid float for column {field}: {value}")
            elif col.type == "datetime":
                if not is_quoted_string(value):
                    raise ValueError(f"Invalid string for column {field}: {value}")

                try:
                    datetime.fromisoformat(unquote_string(value))
                except ValueError:
                    raise ValueError(f"Invalid date for column {field}: {value}")

    def execute(self, db: Database) -> None:
        table = db.get_table(self.table)

        rs = table.read()

        rs.rows = rs.rows + tuple(
            [table.create_row(tuple(self.values), tuple(self.fields))]
        )

        table.write(rs)


def parse_insert(query: str) -> Insert:
    """
    INSERT INTO users (id, name) VALUES (1, "John")
    """
    lower = query.lower().replace(";", "").replace("\n", " ").replace("\t", " ").strip()

    if not lower.startswith("insert into"):
        raise ValueError("Invalid query")

    between_parenthesis: List[str] = re.findall(r"\((.*?)\)", query)

    if len(between_parenthesis) != 2:
        raise ValueError("Query must have exactly two sets of parentheses")

    table = re.search(r"into\s+(\w+)\s*\(", lower)
    if table is None:
        raise ValueError("Missing table name")
    table = table.group(1)

    fields = []
    for fields_part in between_parenthesis[0].split(","):
        sanitized = fields_part.strip("(),").strip()

        if sanitized:
            fields.append(sanitized.lower())

    if re.search(r"\)\s+values\s*\(", lower) is None:
        raise ValueError("Missing VALUES keyword")

    values = []
    for values_part in between_parenthesis[1].split(","):
        sanitized = values_part.strip("(),").strip()

        if sanitized:
            values.append(sanitized)

    return Insert(
        table=table,
        fields=fields,
        values=values,
    )