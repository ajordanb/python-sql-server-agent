## Python SQL Database Client

A simple, safe SQL database client supporting multiple databases.

### Supported Databases

- SQL Server
- PostgreSQL
- MySQL
- SQLite

### Installation

```bash
uv sync
```

### Usage

```python
from sql import DatabaseClientFactory, DatabaseType, DatabaseExplorer

# Create client (SQLite example)
client = DatabaseClientFactory.create(
    DatabaseType.SQLITE,
    database_path=":memory:"
)

# Use explorer for safe parameterized queries
explorer = DatabaseExplorer(client)

users = explorer.fetch_all(
    "SELECT * FROM users WHERE age > :min_age",
    {"min_age": 18}
)

# Convenience methods
user = explorer.find_by_id("users", "id", 123)
total = explorer.count("users")
exists = explorer.exists("users", "email", "test@example.com")

client.close()
```

### Run

```bash
uv run python app.py
```

### Docker

```bash
docker build -t sql-client .
docker run sql-client
```
