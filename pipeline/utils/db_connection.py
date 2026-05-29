"""
Database connection utility.

Provides a context-managed PostgreSQL connection using psycopg2.
Ensures connections are properly closed after use, even on errors.
"""

import psycopg2
from contextlib import contextmanager

from pipeline.config import settings


class DBConnectionManager:
    """
    Context manager for PostgreSQL database connections.

    Reads connection parameters from the application settings
    and guarantees cleanup via ``contextmanager``.
    """

    @contextmanager
    def connect(self):
        """
        Opens a database connection and yields it to the caller.

        The connection is automatically closed when the ``with``
        block exits, whether normally or via an exception.

        :yields: Active psycopg2 connection object.
        :rtype: psycopg2.extensions.connection
        """
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            user=settings.db_user,
            password=settings.db_pass,
            dbname=settings.db_name
        )
        try:
            yield conn
        finally:
            conn.close()
