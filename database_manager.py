from __future__ import annotations

"""
database_manager.py

Shared SQLite database management utilities for repository ingestion pipelines.

This module intentionally contains only repository-agnostic database concerns:
- connection setup
- schema initialization
- transaction control
- small helper queries used by multiple pipelines

Repository-specific SQL INSERT logic should remain inside each repository script.
"""

from pathlib import Path
import sqlite3
from typing import Any, Iterable, Optional, Sequence


class DatabaseManagement:
    """
    Thin SQLite manager for ingestion pipelines.

    This class keeps database setup and common row-management logic in one place
    while leaving repository-specific persistence logic to the pipeline scripts.
    """

    def __init__(
        self,
        db_path: str,
        schema_path: str,
        *,
        timeout: float = 30.0,
        enable_foreign_keys: bool = True,
    ) -> None:
        """
        Initialize the SQLite connection and apply the schema.

        Parameters
        ----------
        db_path:
            Path to the SQLite database file.
        schema_path:
            Path to the schema.sql file.
        timeout:
            SQLite connection timeout in seconds.
        enable_foreign_keys:
            Whether to enable SQLite foreign key checks.
        """
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)
        self.timeout = timeout
        self.enable_foreign_keys = enable_foreign_keys

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(str(self.db_path), timeout=self.timeout)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()

        if self.enable_foreign_keys:
            self.cur.execute("PRAGMA foreign_keys = ON")

        self._initialize_schema()

    def _initialize_schema(self) -> None:
        """
        Load and execute the schema file.
        """
        with self.schema_path.open("r", encoding="utf-8") as f:
            self.cur.executescript(f.read())
        self.conn.commit()

    def begin(self) -> None:
        """
        Begin an explicit transaction.
        """
        self.conn.execute("BEGIN")

    def commit(self) -> None:
        """
        Commit the current transaction.
        """
        self.conn.commit()

    def rollback(self) -> None:
        """
        Roll back the current transaction.
        """
        self.conn.rollback()

    def close(self) -> None:
        """
        Close the database cursor and connection safely.
        """
        try:
            self.cur.close()
        finally:
            self.conn.close()

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        """
        Execute a single SQL statement.

        Parameters
        ----------
        sql:
            SQL statement.
        params:
            Positional parameters for the SQL statement.
        """
        return self.cur.execute(sql, params)

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> sqlite3.Cursor:
        """
        Execute a SQL statement against multiple parameter rows.

        Parameters
        ----------
        sql:
            SQL statement.
        rows:
            Iterable of parameter sequences.
        """
        return self.cur.executemany(sql, rows)

    def get_existing_project_folder(self, project_key: Any) -> Optional[str]:
        """
        Return the existing download_project_folder for a project, if present.

        Parameters
        ----------
        project_key:
            Repository-specific project key.
        """
        self.cur.execute(
            "SELECT download_project_folder FROM projects WHERE project_key = ?",
            (project_key,),
        )
        row = self.cur.fetchone()
        if not row:
            return None
        folder = row["download_project_folder"]
        return str(folder) if folder else None

    def delete_project_rows(
        self,
        project_key: Any,
        score_table: str,
        extra_tables: Optional[Iterable[str]] = None,
    ) -> None:
        """
        Delete all common project-linked rows for one project.

        Parameters
        ----------
        project_key:
            Repository-specific project key.
        score_table:
            Repository-specific qualitative score table name.
        extra_tables:
            Optional extra project-linked tables to delete from.
        """
        tables = [
            "keywords",
            "licenses",
            "person_role",
            "files",
            score_table,
            "projects",
        ]

        if extra_tables:
            tables.extend(extra_tables)

        for table in tables:
            self.cur.execute(f"DELETE FROM {table} WHERE project_key = ?", (project_key,))

    def __enter__(self) -> "DatabaseManagement":
        """
        Return the database manager for context-manager usage.
        """
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """
        Roll back on exception and always close the connection.
        """
        if exc_type is not None:
            self.rollback()
        self.close()