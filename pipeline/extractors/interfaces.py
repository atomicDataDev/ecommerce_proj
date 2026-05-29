"""
Abstract interfaces for data extractors.

Defines contracts for event log extraction (file-based) and
enrichment data extraction (database-based). All concrete
extractors must implement these interfaces.
"""

from abc import ABC, abstractmethod

import pandas as pd


class IEventsExtractor(ABC):
    """
    Interface for extracting raw event logs from file-based sources.

    Concrete implementations handle the specifics of file format,
    compression, and batching strategy.
    """

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Reads all event logs and returns them as a single DataFrame.

        :return: DataFrame containing all raw event records.
        :rtype: pd.DataFrame
        """
        pass


class IEnrichmentExtractor(ABC):
    """
    Interface for extracting enrichment data from a database.

    Provides separate methods for each entity type to allow
    independent loading and caching strategies.
    """

    @abstractmethod
    def extract_customers(self) -> pd.DataFrame:
        """
        Loads the full customer catalogue.

        :return: DataFrame with columns ``customer_id``, ``join_date``, ``segment``.
        :rtype: pd.DataFrame
        """
        pass

    @abstractmethod
    def extract_products(self) -> pd.DataFrame:
        """
        Loads the full product catalogue.

        :return: DataFrame with columns ``product_id``, ``product_name``,
            ``category``, ``price``.
        :rtype: pd.DataFrame
        """
        pass