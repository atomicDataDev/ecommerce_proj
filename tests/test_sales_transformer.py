"""
Unit tests for :class:`SalesTransformer`.

Verifies the core transformation logic: purchase filtering,
revenue calculations, and correct aggregation by category
and customer segment.
"""

from pipeline.transformers.sales_transformer import SalesTransformer


def test_sales_transformer_calculations(sample_events, sample_customers, sample_products):
    """
    Verifies revenue and quantity aggregation for known inputs.

    Expected results (Books/VIP):
    - c1 buys 2x Book A @ 10.50 = 21.00
    - c3 buys 3x Book A @ 10.50 = 31.50
    - total_revenue = 52.50, units_sold = 5, unique_customers = 2
    """
    transformer = SalesTransformer()

    result_df = transformer.transform(sample_events, sample_customers, sample_products)

    # Two groups expected: (Books, VIP) and (Electronics, New).
    assert len(result_df) == 2

    books_vip = result_df[(result_df['category'] == 'Books') & (result_df['customer_segment'] == 'VIP')].iloc[0]

    assert books_vip['total_revenue'] == 52.50
    assert books_vip['units_sold'] == 5
    assert books_vip['unique_customers'] == 2


def test_sales_transformer_views(sample_events, sample_customers, sample_products):
    """
    Verifies that non-purchase events (view_product, add_to_cart) are excluded.

    Customer c1 has a ``view_product`` event for p2 (Electronics) that
    must not appear in the final report. Only c2's purchase should
    count for Electronics/New.
    """
    transformer = SalesTransformer()
    result_df = transformer.transform(sample_events, sample_customers, sample_products)

    electronics_new = result_df[(result_df['category'] == 'Electronics') & (result_df['customer_segment'] == 'New')].iloc[0]

    assert electronics_new['unique_customers'] == 1
    assert electronics_new['units_sold'] == 1
    assert electronics_new['total_revenue'] == 1000.00