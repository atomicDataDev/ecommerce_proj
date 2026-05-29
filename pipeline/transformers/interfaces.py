"""
Abstract interface for data transformation.

Defines the contract for transforming raw extracted data
into aggregated business-level reports.
"""

from abc import ABC, abstractmethod

import pandas as pd


class ITransformer(ABC):
    """
    Interface for data transformation and aggregation.

    Implementations receive raw DataFrames from the extract stage
    and apply filtering, joining, feature engineering, and
    aggregation to produce the final report.
    """

    @abstractmethod
    def transform(self, events_df: pd.DataFrame, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms raw data into an aggregated sales report.

        :param events_df: Raw event logs (all event types).
        :type events_df: pd.DataFrame
        :param customers_df: Customer catalogue from the database.
        :type customers_df: pd.DataFrame
        :param products_df: Product catalogue from the database.
        :type products_df: pd.DataFrame
        :return: Aggregated report DataFrame.
        :rtype: pd.DataFrame
        """
        pass