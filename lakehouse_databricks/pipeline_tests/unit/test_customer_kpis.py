import pytest

from decimal import Decimal

from pyspark.sql import functions as F

from utilities.helpers import calculate_customer_kpis


pytestmark = pytest.mark.unit


@pytest.fixture
def customer_kpis(valid_customers, valid_orders):

    return calculate_customer_kpis(
        valid_customers,
        valid_orders
    )


def test_gold_customer_count(customer_kpis):

    assert customer_kpis.count() == 7


def test_alice_total_revenue(customer_kpis):

    customer = (
        customer_kpis
        .filter(F.col("customer_id") == 1)
        .first()
    )

    assert customer.total_revenue == Decimal("250.00")


def test_alice_average_order_value(customer_kpis):

    customer = (
        customer_kpis
        .filter(F.col("customer_id") == 1)
        .first()
    )

    assert customer.average_order_value == Decimal("125.00")


def test_bob_order_statistics(customer_kpis):

    customer = (
        customer_kpis
        .filter(F.col("customer_id") == 2)
        .first()
    )

    assert customer.total_orders == 2
    assert customer.completed_orders == 1
    assert customer.pending_orders == 1
    assert customer.total_revenue == Decimal("200.00")


def test_cancelled_orders_excluded_from_revenue(customer_kpis):

    customer = (
        customer_kpis
        .filter(F.col("customer_id") == 3)
        .first()
    )

    assert customer.cancelled_orders == 1
    assert customer.total_revenue == Decimal("300.00")


def test_customer_without_orders(customer_kpis):

    customer = (
        customer_kpis
        .filter(F.col("customer_id") == 6)
        .first()
    )

    assert customer.total_orders == 0
    assert customer.total_revenue == Decimal("0.00")
    assert customer.average_order_value == Decimal("0.00")
    assert customer.last_order_date is None


@pytest.mark.parametrize(
    "customer_id, expected_segment",
    [
        (1, "HIGH"),
        (2, "MEDIUM"),
        (3, "HIGH"),
        (4, "LOW"),
        (5, "MEDIUM"),
        (6, "LOW"),
        (7, "LOW")
    ]
)
def test_customer_segmentation(
    customer_kpis,
    customer_id,
    expected_segment
):

    customer = (
        customer_kpis
        .filter(F.col("customer_id") == customer_id)
        .first()
    )

    assert customer.customer_segment == expected_segment


def test_total_revenue(customer_kpis):

    result = customer_kpis.agg(
        F.sum("total_revenue").alias("revenue")
    ).first()

    assert result.revenue == Decimal("960.00")


def test_gold_revenue_data_type(customer_kpis):

    schema = dict(customer_kpis.dtypes)

    assert schema["total_revenue"] == "decimal(18,2)"
    assert schema["average_order_value"] == "decimal(18,2)"