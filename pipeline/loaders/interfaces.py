"""
Abstract interface for data loaders.

Defines the contract for persisting the final report
to a target destination (file system, cloud storage, etc.).
"""

from abc import ABC, abstractmethod

import pandas as pd


class ILoader(ABC):
    """
    Interface for saving the final aggregated report.

    Implementations decide the output format and destination
    (e.g., local CSV, S3, database table).
    """

    @abstractmethod
    def load(self, df: pd.DataFrame) -> None:
        """
        Persists the final DataFrame to the target destination.

        :param df: Aggregated report to save.
        :type df: pd.DataFrame
        """
        pass