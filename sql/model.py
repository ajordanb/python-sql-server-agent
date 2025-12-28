from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Generator, Optional

from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Connection, Engine, Result
from sqlalchemy.orm import Session, sessionmaker


class DatabaseType(Enum):
    """Supported database types."""
    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


class DatabaseClient(ABC):
    """
    Abstract base class for database clients.

    Provides common interface for database operations across different
    database engines using SQLAlchemy.

    Attributes:
        engine: SQLAlchemy engine instance.
        metadata: SQLAlchemy MetaData for schema reflection.
        Session: Session factory bound to the engine.
    """

    engine: Engine
    metadata: MetaData
    Session: sessionmaker

    def __init__(self, echo: bool = False):
        """
        Initialize the database client.

        Args:
            echo: If True, SQLAlchemy will log all SQL statements.
        """
        self.echo = echo
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)

    @abstractmethod
    def _build_connection_url(self) -> str:
        """Build the database-specific connection URL."""
        pass

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine from connection URL."""
        connection_url = self._build_connection_url()
        return create_engine(connection_url, echo=self.echo)

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """
        Get a raw database connection as a context manager.

        Yields:
            SQLAlchemy Connection object.
        """
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get a database session as a context manager.

        Automatically commits on success, rolls back on exception.

        Yields:
            SQLAlchemy Session object.
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> Result:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query string.
            params: Optional dictionary of query parameters.

        Returns:
            SQLAlchemy Result object.
        """
        with self.get_session() as session:
            result = session.execute(text(query), params or {})
            return result

    def execute_many(
        self, query: str, params_list: list[Dict[str, Any]]
    ) -> None:
        """
        Execute a SQL query multiple times with different parameters.

        Args:
            query: SQL query string.
            params_list: List of parameter dictionaries.
        """
        with self.get_session() as session:
            for params in params_list:
                session.execute(text(query), params)

    def close(self) -> None:
        """Close the database engine and release connections."""
        self.engine.dispose()


class DatabaseClientFactory:
    """
    Factory for creating database client instances.

    Example:
        # Create SQL Server client
        client = DatabaseClientFactory.create(
            DatabaseType.SQLSERVER,
            server="localhost",
            database="mydb",
            username="sa",
            password="password"
        )

        # Create PostgreSQL client
        client = DatabaseClientFactory.create(
            DatabaseType.POSTGRESQL,
            host="localhost",
            database="mydb",
            username="postgres",
            password="password"
        )

        # Create from connection URL
        client = DatabaseClientFactory.from_url(
            "postgresql://user:pass@localhost/mydb"
        )
    """

    _registry: Dict[DatabaseType, type[DatabaseClient]] = {}

    @classmethod
    def register(cls, db_type: DatabaseType):
        """
        Decorator to register a client class with the factory.

        Args:
            db_type: The database type this client handles.
        """
        def decorator(client_class: type[DatabaseClient]):
            cls._registry[db_type] = client_class
            return client_class
        return decorator

    @classmethod
    def create(cls, db_type: DatabaseType, **kwargs) -> DatabaseClient:
        """
        Create a database client of the specified type.

        Args:
            db_type: The type of database to connect to.
            **kwargs: Database-specific connection parameters.

        Returns:
            A configured DatabaseClient instance.

        Raises:
            ValueError: If the database type is not registered.
        """
        if db_type not in cls._registry:
            available = ", ".join(t.value for t in cls._registry.keys())
            raise ValueError(
                f"Unknown database type: {db_type}. Available: {available}"
            )

        client_class = cls._registry[db_type]
        return client_class(**kwargs)

    @classmethod
    def from_url(cls, url: str, echo: bool = False) -> DatabaseClient:
        """
        Create a database client from a SQLAlchemy connection URL.

        Args:
            url: SQLAlchemy connection URL string.
            echo: If True, log all SQL statements.

        Returns:
            A configured DatabaseClient instance.

        Example:
            client = DatabaseClientFactory.from_url(
                "postgresql://user:pass@localhost/mydb"
            )
        """
        # Import here to avoid circular dependency
        from sql.clients import GenericClient
        return GenericClient(url=url, echo=echo)
