"""Airflow task wrappers for the e-commerce ETL pipeline.

This module provides a file-based interface for Airflow tasks. Task
functions execute existing pipeline classes and persist intermediate
datasets to local files so that state is passed through the
filesystem rather than in-memory objects.

The functions intentionally obtain the Airflow execution context at
runtime and create per-run storage directories, avoiding race
conditions between concurrent DAG runs.
"""

from pathlib import Path
from typing import Dict

from pipeline.config import settings

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TMP_DIR_BASE = PROJECT_ROOT / settings.tmp_dir


def _sanitize_run_id(run_key: str) -> str:
    """Sanitize a run identifier so it is safe for filenames.

    :param run_key: Original run identifier (logical date or run id).
    :type run_key: str
    :return: Sanitized run identifier suitable for a filesystem path.
    :rtype: str
    """
    return run_key.replace(":", "_").replace("/", "_").replace(" ", "_")


def _run_dir_for(run_key: str) -> Path:
    """Return the path to the per-run temporary directory.

    :param run_key: Run key (logical date or run id).
    :type run_key: str
    :return: Path to the run-specific temporary directory.
    :rtype: pathlib.Path
    """
    safe = _sanitize_run_id(run_key)
    run_dir = TMP_DIR_BASE / safe
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def extract_files(run_date: str) -> str:
    """Extract events from nested archives and persist them to a CSV file.

    :param run_date: The logical Airflow execution date (``ds``) in
        ``YYYY-MM-DD`` format used to isolate artifacts.
    :type run_date: str
    :return: Path to the persisted events CSV file.
    :rtype: str
    """
    # Local imports keep DAG parsing lightweight.
    from pipeline.extractors import FileExtractor

    run_dir = _run_dir_for(run_date)
    events_path = run_dir / "events.csv"

    extractor = FileExtractor()
    events_df = extractor.extract()
    events_df.to_csv(events_path, index=False)
    return str(events_path)


def extract_db(run_date: str) -> Dict[str, str]:
    """Extract customer and product catalogues from the database.

    :param run_date: The logical Airflow execution date (``ds``) in
        ``YYYY-MM-DD`` format used to isolate artifacts.
    :type run_date: str
    :return: Dictionary with keys ``customers_path`` and ``products_path``.
    :rtype: dict
    """
    # Local imports keep DAG parsing lightweight.
    from pipeline.extractors import DbExtractor
    from pipeline.utils.db_connection import DBConnectionManager

    run_dir = _run_dir_for(run_date)
    customers_path = run_dir / "customers.csv"
    products_path = run_dir / "products.csv"

    db_manager = DBConnectionManager()
    extractor = DbExtractor(db_manager=db_manager)
    customers_df = extractor.extract_customers()
    products_df = extractor.extract_products()
    customers_df.to_csv(customers_path, index=False)
    products_df.to_csv(products_path, index=False)

    return {"customers_path": str(customers_path), "products_path": str(products_path)}


def transform(
    run_date: str, events_path: str, customers_path: str, products_path: str
) -> str:
    """Load extracted datasets, run transformation, and persist the report.

    :param run_date: The logical Airflow run date (``ds``) in ``YYYY-MM-DD``.
    :type run_date: str
    :param events_path: Path to the CSV file with extracted events.
    :type events_path: str
    :param customers_path: Path to the CSV file with customers.
    :type customers_path: str
    :param products_path: Path to the CSV file with products.
    :type products_path: str
    :return: Path to the persisted report CSV file.
    :rtype: str
    """
    # Local imports keep DAG parsing lightweight.
    import pandas as pd

    from pipeline.transformers import SalesTransformer

    run_dir = _run_dir_for(run_date)
    report_path = run_dir / "sales_report.csv"

    events_df = pd.read_csv(events_path)
    customers_df = pd.read_csv(customers_path)
    products_df = pd.read_csv(products_path)
    transformer = SalesTransformer()
    report_df = transformer.transform(events_df, customers_df, products_df)
    report_df.to_csv(report_path, index=False)
    return str(report_path)


def load(report_path: str) -> None:
    """Load the transformed report and persist final CSV files.

    :param report_path: Path to the transformed report CSV file.
    :type report_path: str
    :return: None
    :rtype: None
    """
    # Local imports keep module import lightweight for DAG parsing.
    import pandas as pd

    from pipeline.loaders import FileLoader

    report_df = pd.read_csv(report_path)
    loader = FileLoader()
    loader.load(report_df)
