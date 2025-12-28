from __future__ import annotations
from typing import Optional
from urllib.parse import quote_plus
from sql.model import DatabaseClient, DatabaseClientFactory, DatabaseType

@DatabaseClientFactory.register(DatabaseType.SQLSERVER)
class SQLServerClient(DatabaseClient):
    """
    SQL Server database client.

    Supports both Windows Authentication (trusted_connection) and
    SQL Server Authentication (username/password).

    Args:
        server: SQL Server hostname or IP address.
        database: Database name.
        username: SQL Server username (optional if using trusted_connection).
        password: SQL Server password (optional if using trusted_connection).
        driver: ODBC driver name. Defaults to "ODBC Driver 17 for SQL Server".
        trusted_connection: Use Windows Authentication. Defaults to False.
        echo: Log SQL statements. Defaults to False.
        mars_connection: Enable Multiple Active Result Sets. Defaults to True.

    Example:
        # SQL Server Authentication
        client = SQLServerClient(
            server="localhost",
            database="mydb",
            username="sa",
            password="password"
        )

        # Windows Authentication
        client = SQLServerClient(
            server="localhost",
            database="mydb",
            trusted_connection=True
        )
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "ODBC Driver 17 for SQL Server",
        trusted_connection: bool = False,
        echo: bool = False,
        mars_connection: bool = True,
    ):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.trusted_connection = trusted_connection
        self.mars_connection = mars_connection
        super().__init__(echo=echo)

    def _build_connection_url(self) -> str:
        """Build SQL Server ODBC connection URL."""
        params = {
            "driver": self.driver,
            "server": self.server,
            "database": self.database,
        }

        if self.trusted_connection:
            params["trusted_connection"] = "yes"
        else:
            params["uid"] = self.username
            params["pwd"] = self.password

        if self.mars_connection:
            params["Mars_Connection"] = "yes"
            params["MultipleActiveResultSets"] = "True"

        params["TrustServerCertificate"] = "yes"
        params["Connection Timeout"] = "30"
        params["Command Timeout"] = "30"

        connection_parts = [
            f"{key}={quote_plus(str(value))}"
            for key, value in params.items()
            if value is not None
        ]
        connection_string = ";".join(connection_parts)

        return f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}"


@DatabaseClientFactory.register(DatabaseType.POSTGRESQL)
class PostgreSQLClient(DatabaseClient):
    """
    PostgreSQL database client.

    Args:
        host: PostgreSQL hostname or IP address.
        database: Database name.
        username: PostgreSQL username.
        password: PostgreSQL password.
        port: PostgreSQL port. Defaults to 5432.
        echo: Log SQL statements. Defaults to False.
        schema: Default schema to use. Defaults to "public".

    Example:
        client = PostgreSQLClient(
            host="localhost",
            database="mydb",
            username="postgres",
            password="password"
        )
    """

    def __init__(
        self,
        host: str,
        database: str,
        username: str,
        password: str,
        port: int = 5432,
        echo: bool = False,
        schema: str = "public",
    ):
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.schema = schema
        super().__init__(echo=echo)

    def _build_connection_url(self) -> str:
        """Build PostgreSQL connection URL."""
        password_encoded = quote_plus(self.password)
        return (
            f"postgresql://{self.username}:{password_encoded}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@DatabaseClientFactory.register(DatabaseType.MYSQL)
class MySQLClient(DatabaseClient):
    """
    MySQL database client.

    Args:
        host: MySQL hostname or IP address.
        database: Database name.
        username: MySQL username.
        password: MySQL password.
        port: MySQL port. Defaults to 3306.
        echo: Log SQL statements. Defaults to False.
        charset: Character set. Defaults to "utf8mb4".

    Example:
        client = MySQLClient(
            host="localhost",
            database="mydb",
            username="root",
            password="password"
        )
    """

    def __init__(
        self,
        host: str,
        database: str,
        username: str,
        password: str,
        port: int = 3306,
        echo: bool = False,
        charset: str = "utf8mb4",
    ):
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.charset = charset
        super().__init__(echo=echo)

    def _build_connection_url(self) -> str:
        """Build MySQL connection URL."""
        password_encoded = quote_plus(self.password)
        return (
            f"mysql+pymysql://{self.username}:{password_encoded}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?charset={self.charset}"
        )


@DatabaseClientFactory.register(DatabaseType.SQLITE)
class SQLiteClient(DatabaseClient):
    """
    SQLite database client.

    Args:
        database_path: Path to SQLite database file. Use ":memory:" for in-memory.
        echo: Log SQL statements. Defaults to False.

    Example:
        # File-based database
        client = SQLiteClient(database_path="/path/to/db.sqlite")

        # In-memory database
        client = SQLiteClient(database_path=":memory:")
    """

    def __init__(
        self,
        database_path: str = ":memory:",
        echo: bool = False,
    ):
        self.database_path = database_path
        super().__init__(echo=echo)

    def _build_connection_url(self) -> str:
        """Build SQLite connection URL."""
        return f"sqlite:///{self.database_path}"


class GenericClient(DatabaseClient):
    """
    Generic database client that accepts a SQLAlchemy connection URL.

    Use this when you have a connection URL string and don't need
    database-specific configuration options.

    Args:
        url: SQLAlchemy connection URL string.
        echo: Log SQL statements. Defaults to False.

    Example:
        client = GenericClient(
            url="postgresql://user:pass@localhost/mydb"
        )
    """

    def __init__(self, url: str, echo: bool = False):
        self.url = url
        super().__init__(echo=echo)

    def _build_connection_url(self) -> str:
        """Return the provided connection URL."""
        return self.url
