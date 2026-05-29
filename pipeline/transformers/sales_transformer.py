"""
Sales data transformer.

Implements the core business logic: filters purchase events,
enriches them with product and customer data, engineers the
``total_revenue`` feature, and aggregates by category and segment.
"""

import pandas as pd

from pipeline.transformers.interfaces import ITransformer


class SalesTransformer(ITransformer):
    """
    Transforms raw event data into an aggregated sales report.

    Processing steps:

    1. Filter events to keep only ``purchase`` records.
    2. Join with products on ``product_id`` to get price and category.
    3. Join with customers on ``customer_id`` to get segment.
    4. Compute ``total_revenue`` = ``quantity`` * ``price``.
    5. Group by ``category`` and ``customer_segment``.
    6. Aggregate revenue, units sold, and unique customer count.
    """

    def transform(self, events_df: pd.DataFrame, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the full transformation pipeline on extracted data.

        :param events_df: Raw event logs containing all event types.
        :type events_df: pd.DataFrame
        :param customers_df: Customer data with ``segment`` column.
        :type customers_df: pd.DataFrame
        :param products_df: Product data with ``price`` and ``category`` columns.
        :type products_df: pd.DataFrame
        :return: Aggregated report with columns ``category``,
            ``customer_segment``, ``total_revenue``, ``units_sold``,
            ``unique_customers``.
        :rtype: pd.DataFrame
        """
        # Step 1 - Keep only purchase events (drop views and cart adds).
        purchases = events_df[events_df['event_type'] == 'purchase'].copy()

        # Step 2 - Enrich with product info (price, category).
        merged_df = purchases.merge(products_df, on='product_id', how='inner')

        # Step 3 - Enrich with customer info (segment).
        merged_df = merged_df.merge(customers_df, on='customer_id', how='inner')

        # Step 4 - Feature engineering: compute revenue per line item.
        merged_df['total_revenue'] = merged_df['quantity'] * merged_df['price']

        # Rename DB column to match the report schema.
        merged_df = merged_df.rename(columns={'segment': 'customer_segment'})

        # Step 5-6 - Aggregate by category and customer segment.
        report_df = merged_df.groupby(['category', 'customer_segment']).agg(
            total_revenue=('total_revenue', 'sum'),
            units_sold=('quantity', 'sum'),
            unique_customers=('customer_id', 'nunique')
        ).reset_index()

        # Round revenue to 2 decimal places and ensure integer units.
        report_df['total_revenue'] = report_df['total_revenue'].round(2)
        report_df['units_sold'] = report_df['units_sold'].astype(int)

        return report_df