from dataclasses import dataclass
import re
from typing import List, Optional, cast

from db import Database, Direction, ResultSet, validate_where
from query import Where, parse_where


@dataclass
class OrderBy:
    field: str
    direction: Direction


@dataclass
class Select:
    fields: List[str]
    table: str
    join_table: Optional[str]
    join_on: Optional[Where]
    where: Optional[Where]
    order_by: Optional[OrderBy]
    limit: Optional[int]

    @property
    def is_join(self) -> bool:
        return self.join_table is not None and self.join_on is not None

    def validate(self, db: Database) -> None:
        if self.table not in [table.name for table in db.tables]:
            raise ValueError(f"Invalid table: {self.table}")

        table = db.get_table(self.table)
        headers = table.headers

        if self.join_table:
            if self.join_table not in [table.name for table in db.tables]:
                raise ValueError(f"Invalid table: {self.join_table}")

            join_table = db.get_table(self.join_table)
            headers = table.prefixed_headers + join_table.prefixed_headers

            if not self.join_on:
                raise ValueError("JOIN without ON")

            if self.join_on.left_hand not in headers:
                raise ValueError(f"Invalid column for join: {self.join_on.left_hand}")
            if self.join_on.right_hand not in headers:
                raise ValueError(f"Invalid column for join: {self.join_on.right_hand}")

        if self.where:
            validate_where(self.where, db, headers, self.table)

        if self.order_by:
            if self.order_by.field not in headers:
                raise ValueError(f"Invalid column for limit: {self.order_by.field}")

        if self.limit:
            if self.limit < 0:
                raise ValueError(f"Invalid limit: {self.limit}")

        if self.fields != ["*"]:
            for field in self.fields:
                if "." in field:
                    table_name = field.split(".")[0]
                    if table_name not in [table.name for table in db.tables]:
                        raise ValueError(f"Invalid table in field: {table_name}")
                if field not in headers:
                    raise ValueError(f"Invalid column: {field}")

    def execute(self, db: Database) -> ResultSet:
        table = db.get_table(self.table)

        rs = table.read(prefixed=self.is_join)

        if self.join_table and self.join_on:
            join_table = db.get_table(self.join_table)
            join_rs = join_table.read(prefixed=True)
            rs = rs.inner_join(join_rs, self.join_on)

        if self.where:
            rs = rs.where(self.where)

        if self.fields == ["*"]:
            self.fields = rs.headers
        else:
            col_indexes = [rs.headers.index(field) for field in self.fields]
            rs.rows = tuple(tuple(row[i] for i in col_indexes) for row in rs.rows)
            rs.columns = tuple(rs.columns[i] for i in col_indexes)

        if self.order_by:
            if self.order_by.field not in rs.headers:
                raise ValueError(
                    f"Invalid column: {self.order_by.field} in table {self.table}"
                )
            col_index = rs.headers.index(self.order_by.field)
            rows = sorted(
                rs.rows,
                key=lambda row: row[col_index],
                reverse=self.order_by.direction == "desc",
            )
            rs.rows = tuple(rows)

        if self.limit:
            rs.rows = rs.rows[: self.limit]

        return rs

    def set_default_limit(self, limit: int) -> None:
        if not self.limit:
            self.limit = limit


def parse_select(query: str) -> Select:
    """
    SELECT * FROM users WHERE id = 1 AND age > 18 ORDER BY id DESC
    """
    query = query.replace(";", "").replace("\n", " ").replace("\t", " ").strip()
    lower = query.lower()
    parts = lower.split(" ")

    if parts[0] != "select":
        raise ValueError("Invalid query")

    fields = []
    for part in parts[1:]:
        if part == "from":
            break

        subparts = part.split(",")

        for subpart in subparts:
            sanitized = subpart.strip()

            if sanitized:
                fields.append(sanitized)

    table = parts[parts.index("from") + 1]

    join_table = None
    join_on = None
    if "join" in parts:
        join_table = parts[parts.index("join") + 1]
        try:
            on = lower.index("on")
        except ValueError:
            on = None

        try:
            using = lower.index("using")
        except ValueError:
            using = None

        if on and using:
            raise ValueError("JOIN with both ON and USING")

        if not on and not using:
            raise ValueError("JOIN without ON or USING")

        try:
            next_keyword = (
                lower.index("where") or lower.index("order") or lower.index("limit")
            )
        except ValueError:
            next_keyword = None

        if on:
            if next_keyword:
                text_between_on_and_next = query[on + 2 : next_keyword]
            else:
                text_between_on_and_next = query[on + 2 :]
            join_on = parse_where(text_between_on_and_next)

        elif using:
            if next_keyword:
                text_between_using_and_next = query[using + 5 : next_keyword]
            else:
                text_between_using_and_next = query[using + 5 :]

            using_field = re.search(r"\(\s*(.*)\s*\)", text_between_using_and_next)
            if using_field is None:
                raise ValueError("Invalid USING")
            using_field = using_field.group(1)

            join_on = Where(
                left_hand=table + "." + using_field,
                operator="=",
                right_hand=join_table + "." + using_field,
                and_where=None,
                or_where=None,
            )

    where = None
    if "where" in parts:
        where = lower.index("where")
        try:
            order_by = lower.index("order")
        except ValueError:
            order_by = None

        try:
            limit = lower.index("limit")
        except ValueError:
            limit = None

        if order_by and limit:
            if limit < order_by:
                raise ValueError("LIMIT must be after ORDER BY")

        if order_by:
            text_between_where_and_next_keyword = query[where + 5 : order_by]
        elif limit:
            text_between_where_and_next_keyword = query[where + 5 : limit]
        else:
            text_between_where_and_next_keyword = query[where + 5 :]

        where = parse_where(text_between_where_and_next_keyword)

    order_by = None
    if "order" in parts:
        direction = parts[parts.index("order") + 3]

        if direction not in ["asc", "desc"]:
            raise ValueError(f"Invalid direction in ORDER BY clause ({direction})")

        direction = cast(Direction, direction)

        order_by = OrderBy(
            field=parts[parts.index("order") + 2],
            direction=direction,
        )

    limit = None
    if "limit" in parts:
        limit_str = parts[parts.index("limit") + 1]
        try:
            limit = int(limit_str)
        except ValueError:
            raise ValueError(f"Invalid limit: {limit_str}")

    return Select(
        fields=fields,
        table=table,
        join_table=join_table,
        join_on=join_on,
        where=where,
        order_by=order_by,
        limit=limit,
    )
