from __future__ import annotations

from abc import ABC
from typing import Any, Optional
from sqlalchemy.engine import Result
from sql.model import DatabaseClient


class Explorer(ABC):
    """Abstract base class for database explorers."""
    pass


class DatabaseExplorer(Explorer):
    """
    Simple, safe query executor wrapping a DatabaseClient.

    All queries use SQLAlchemy's parameterized execution to prevent
    SQL injection. Use named parameters with :param_name syntax.

    Args:
        client: DatabaseClient instance for database connection.

    Example:
        from sql import DatabaseClientFactory, DatabaseType

        client = DatabaseClientFactory.create(
            DatabaseType.POSTGRESQL,
            host="localhost",
            database="mydb",
            username="postgres",
            password="secret"
        )

        explorer = DatabaseExplorer(client)

        # Parameterized queries (safe)
        users = explorer.fetch_all(
            "SELECT * FROM users WHERE age > :min_age",
            {"min_age": 18}
        )

        # Convenience methods
        user = explorer.find_by_id("users", "id", 123)
        total = explorer.count("orders")
    """

    def __init__(self, client: DatabaseClient):
        self.client = client

    def execute(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> Result:
        """
        Execute a parameterized SQL query.

        Args:
            query: SQL query with named parameters (:param_name).
            params: Dictionary of parameter values.

        Returns:
            SQLAlchemy Result object.

        Example:
            result = explorer.execute(
                "UPDATE users SET status = :status WHERE id = :id",
                {"status": "active", "id": 123}
            )
        """
        return self.client.execute_query(query, params)

    def fetch_all(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """
        Execute query and return all rows as dictionaries.

        Args:
            query: SQL query with named parameters.
            params: Dictionary of parameter values.

        Returns:
            List of dictionaries, one per row.

        Example:
            users = explorer.fetch_all(
                "SELECT id, name FROM users WHERE active = :active",
                {"active": True}
            )
        """
        result = self.client.execute_query(query, params)
        rows = result.fetchall()
        if not rows:
            return []
        keys = result.keys()
        return [dict(zip(keys, row)) for row in rows]

    def fetch_one(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> Optional[dict[str, Any]]:
        """
        Execute query and return first row as dictionary.

        Args:
            query: SQL query with named parameters.
            params: Dictionary of parameter values.

        Returns:
            Dictionary of the first row, or None if no results.

        Example:
            user = explorer.fetch_one(
                "SELECT * FROM users WHERE email = :email",
                {"email": "john@example.com"}
            )
        """
        result = self.client.execute_query(query, params)
        row = result.fetchone()
        if row is None:
            return None
        return dict(zip(result.keys(), row))

    def find_by_id(
        self, table: str, id_column: str, id_value: Any
    ) -> Optional[dict[str, Any]]:
        """
        Find a single row by ID.

        Args:
            table: Table name.
            id_column: Name of the ID column.
            id_value: Value to match.

        Returns:
            Dictionary of the row, or None if not found.

        Example:
            user = explorer.find_by_id("users", "id", 123)
        """
        query = f"SELECT * FROM {table} WHERE {id_column} = :id_value"
        return self.fetch_one(query, {"id_value": id_value})

    def find_all(
        self, table: str, limit: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """
        Retrieve all rows from a table.

        Args:
            table: Table name.
            limit: Maximum number of rows to return.

        Returns:
            List of dictionaries, one per row.

        Example:
            users = explorer.find_all("users", limit=100)
        """
        query = f"SELECT * FROM {table}"
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        return self.fetch_all(query)

    def count(self, table: str) -> int:
        """
        Count rows in a table.

        Args:
            table: Table name.

        Returns:
            Number of rows.

        Example:
            total = explorer.count("orders")
        """
        query = f"SELECT COUNT(*) as cnt FROM {table}"
        result = self.fetch_one(query)
        return result["cnt"] if result else 0

    def exists(self, table: str, column: str, value: Any) -> bool:
        """
        Check if a row exists with the given column value.

        Args:
            table: Table name.
            column: Column to check.
            value: Value to match.

        Returns:
            True if at least one matching row exists.

        Example:
            has_admin = explorer.exists("users", "role", "admin")
        """
        query = f"SELECT 1 FROM {table} WHERE {column} = :value LIMIT 1"
        result = self.fetch_one(query, {"value": value})
        return result is not None
