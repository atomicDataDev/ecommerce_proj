"""
CSV file loader for pipeline reports.

Saves the aggregated sales report to the file system as CSV.
Also generates per-category breakdown files.
"""

from pathlib import Path

import pandas as pd

from pipeline.loaders.interfaces import ILoader
from pipeline.config import settings


class FileLoader(ILoader):
    """
    Writes the final report to CSV files in the reports directory.

    Produces two types of output:

    - ``sales_report.csv`` - the full aggregated report.
    - ``{category}.csv`` - a separate file per product category.
    """

    def load(self, df: pd.DataFrame) -> None:
        """
        Saves the report DataFrame to CSV files.

        Creates the output directory if it does not exist.
        Writes the consolidated report first, then iterates over
        unique categories to produce individual breakdowns.

        :param df: Final aggregated report with a ``category`` column.
        :type df: pd.DataFrame
        """
        output_dir = Path(settings.reports_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save the full consolidated report.
        file_path = output_dir / "sales_report.csv"
        df.to_csv(file_path, index=False)

        # Save a separate report for each product category.
        for category in df['category'].unique():
            category_df = df[df['category'] == category]
            category_path = output_dir / f"{category}.csv"
            category_df.to_csv(category_path, index=False)