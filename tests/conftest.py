"""
Pytest fixtures for the sales transformer unit tests.

Provides small, deterministic sample DataFrames that mimic
the schema of real extracted data (events, customers, products).
"""

import pytest
import pandas as pd


@pytest.fixture
def sample_events():
    """
    Sample event logs with mixed event types.

    Includes ``purchase`` and ``view_product`` events to verify
    that the transformer correctly filters non-purchase records.

    :return: DataFrame matching the file extractor output schema.
    :rtype: pd.DataFrame
    """
    return pd.DataFrame([
        {'timestamp': '2023-10-01', 'customer_id': 'c1', 'event_type': 'purchase', 'product_id': 'p1', 'quantity': 2},
        {'timestamp': '2023-10-01', 'customer_id': 'c1', 'event_type': 'view_product', 'product_id': 'p2', 'quantity': None},
        {'timestamp': '2023-10-02', 'customer_id': 'c2', 'event_type': 'purchase', 'product_id': 'p2', 'quantity': 1},
        {'timestamp': '2023-10-03', 'customer_id': 'c3', 'event_type': 'purchase', 'product_id': 'p1', 'quantity': 3},
    ])


@pytest.fixture
def sample_customers():
    """
    Sample customer catalogue matching the DB schema.

    Uses ``segment`` column (not ``customer_segment``) to match
    the real database output - the transformer handles renaming.

    :return: DataFrame matching the DB extractor customers schema.
    :rtype: pd.DataFrame
    """
    return pd.DataFrame([
        {'customer_id': 'c1', 'join_date': '2022-01-01', 'segment': 'VIP'},
        {'customer_id': 'c2', 'join_date': '2022-02-01', 'segment': 'New'},
        {'customer_id': 'c3', 'join_date': '2022-03-01', 'segment': 'VIP'},
    ])


@pytest.fixture
def sample_products():
    """
    Sample product catalogue matching the DB schema.

    Covers two categories (Books, Electronics) with different
    price points for verifying revenue calculations.

    :return: DataFrame matching the DB extractor products schema.
    :rtype: pd.DataFrame
    """
    return pd.DataFrame([
        {'product_id': 'p1', 'product_name': 'Book A', 'category': 'Books', 'price': 10.50},
        {'product_id': 'p2', 'product_name': 'Laptop', 'category': 'Electronics', 'price': 1000.00},
    ])