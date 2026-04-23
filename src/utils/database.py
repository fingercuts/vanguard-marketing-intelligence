"""
DuckDB connection manager with context manager pattern.
Provides safe, reusable database connections across the pipeline.
"""

import duckdb
import logging
from contextlib import contextmanager
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DUCKDB_PATH

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages DuckDB connections with safe lifecycle handling."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DUCKDB_PATH
        # Ensure parent directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"DatabaseManager initialized → {self.db_path}")

    @contextmanager
    def connection(self):
        """Context manager for a DuckDB connection."""
        conn = None
        try:
            conn = duckdb.connect(self.db_path)
            logger.debug("Database connection opened.")
            yield conn
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Database connection closed.")

    def execute(self, sql: str, params: list = None):
        """Execute a single SQL statement and return results."""
        with self.connection() as conn:
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            try:
                return result.fetchdf()
            except Exception:
                return None

    def execute_script(self, sql: str):
        """Execute multiple SQL statements (separated by ;)."""
        with self.connection() as conn:
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for stmt in statements:
                logger.debug(f"Executing: {stmt[:80]}...")
                conn.execute(stmt)
            logger.info(f"Executed {len(statements)} SQL statements.")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result[0] > 0

    def row_count(self, table_name: str) -> int:
        """Get the row count of a table."""
        with self.connection() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return result[0]

    def get_tables(self) -> list:
        """List all tables in the database."""
        with self.connection() as conn:
            df = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchdf()
            return df["table_name"].tolist()


# Module-level convenience instance
db = DatabaseManager()
