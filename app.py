from contextlib import contextmanager
from typing import Generator

from sql import DatabaseClientFactory, DatabaseType, DatabaseExplorer


@contextmanager
def db_explorer(db_type: DatabaseType, **kwargs) -> Generator[DatabaseExplorer, None, None]:
    client = DatabaseClientFactory.create(db_type, **kwargs)
    try:
        yield DatabaseExplorer(client)
    finally:
        client.close()


with db_explorer(DatabaseType.SQLITE, database_path=":memory:") as explorer:
    # Setup: Create a sample table
    explorer.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        )
    """)

    # Insert some data
    explorer.execute(
        "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
        {"name": "Alice", "email": "alice@example.com", "age": 30}
    )
    explorer.execute(
        "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
        {"name": "Bob", "email": "bob@example.com", "age": 25}
    )
    explorer.execute(
        "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
        {"name": "Charlie", "email": "charlie@example.com", "age": 35}
    )

    # Demo: fetch_all with parameterized query
    print("Users older than 26:")
    users = explorer.fetch_all(
        "SELECT * FROM users WHERE age > :min_age",
        {"min_age": 26}
    )
    for user in users:
        print(f"  {user['name']} ({user['age']})")

    # Demo: fetch_one
    print("\nFind user by email:")
    user = explorer.fetch_one(
        "SELECT * FROM users WHERE email = :email",
        {"email": "bob@example.com"}
    )
    print(f"  Found: {user['name']}")

    # Demo: convenience methods
    print("\nConvenience methods:")
    print(f"  Total users: {explorer.count('users')}")
    print(f"  User #1: {explorer.find_by_id('users', 'id', 1)['name']}")
    print(f"  Has Alice? {explorer.exists('users', 'name', 'Alice')}")
    print(f"  Has Dave? {explorer.exists('users', 'name', 'Dave')}")
