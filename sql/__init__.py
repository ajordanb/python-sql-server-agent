"""
SQL database utilities package.

This package provides:
- DatabaseClient: Abstract base class for database operations
- DatabaseClientFactory: Factory for creating database clients
- DatabaseExplorer: Simple, safe query executor

Database Clients:
- SQLServerClient: Microsoft SQL Server
- PostgreSQLClient: PostgreSQL
- MySQLClient: MySQL/MariaDB
- SQLiteClient: SQLite
- GenericClient: Any database via connection URL

Example:
    from sql import DatabaseClientFactory, DatabaseType, DatabaseExplorer

    # Create a PostgreSQL client
    client = DatabaseClientFactory.create(
        DatabaseType.POSTGRESQL,
        host="localhost",
        database="mydb",
        username="postgres",
        password="password"
    )

    # Use explorer for safe parameterized queries
    explorer = DatabaseExplorer(client)
    users = explorer.fetch_all(
        "SELECT * FROM users WHERE age > :min_age",
        {"min_age": 18}
    )

    # Convenience methods
    user = explorer.find_by_id("users", "id", 123)
    total = explorer.count("orders")
"""
from sql.model import (
    DatabaseClient,
    DatabaseClientFactory,
    DatabaseType,
)
from sql.clients import (
    SQLServerClient,
    PostgreSQLClient,
    MySQLClient,
    SQLiteClient,
    GenericClient,
)
from sql.explorer import (
    Explorer,
    DatabaseExplorer,
)

__all__ = [
    "DatabaseClient",
    "DatabaseClientFactory",
    "DatabaseType",
    "SQLServerClient",
    "PostgreSQLClient",
    "MySQLClient",
    "SQLiteClient",
    "GenericClient",
    "Explorer",
    "DatabaseExplorer",
]
