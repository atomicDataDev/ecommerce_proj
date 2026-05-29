"""
Database extractor for enrichment data (customers, products).

Reads catalogue tables from a PostgreSQL database and returns
them as Pandas DataFrames for downstream joins in the pipeline.
"""

import pandas as pd

from pipeline.extractors.interfaces import IEnrichmentExtractor
from pipeline.utils.db_connection import DBConnectionManager


class DbExtractor(IEnrichmentExtractor):
    """
    Extracts customer and product catalogues from PostgreSQL.

    Uses :class:`DBConnectionManager` to obtain database connections
    via a context manager, ensuring proper cleanup.

    :param db_manager: Connection manager providing database access.
    :type db_manager: DBConnectionManager
    """

    def __init__(self, db_manager: DBConnectionManager):
        self.db_manager = db_manager

    def extract_customers(self) -> pd.DataFrame:
        """
        Loads the ``customers`` table from the database.

        :return: DataFrame with columns ``customer_id``, ``join_date``, ``segment``.
        :rtype: pd.DataFrame
        """
        query = "SELECT customer_id, join_date, segment FROM customers;"
        with self.db_manager.connect() as conn:
            df = pd.read_sql(query, conn)

        return df

    def extract_products(self) -> pd.DataFrame:
        """
        Loads the ``products`` table from the database.

        :return: DataFrame with columns ``product_id``, ``product_name``,
            ``category``, ``price``.
        :rtype: pd.DataFrame
        """
        query = "SELECT product_id, product_name, category, price FROM products;"

        with self.db_manager.connect() as conn:
            df = pd.read_sql(query, conn)

        return df