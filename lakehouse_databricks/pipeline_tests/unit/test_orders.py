import pytest

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from utilities.helpers import clean_orders
from utilities.data_quality import ORDER_RULES


pytestmark = pytest.mark.unit


def test_raw_order_count(raw_orders):

    assert raw_orders.count() == 12


def test_order_duplicate_removal(raw_orders, valid_customers):

    result = clean_orders(raw_orders, valid_customers)

    assert (
        result
        .filter(F.col("order_id") == 101)
        .count()
    ) == 1


def test_orphan_customer_removed(raw_orders, valid_customers):

    result = clean_orders(raw_orders, valid_customers)

    assert (
        result
        .filter(F.col("customer_id") == 999)
        .count()
    ) == 0


def test_negative_amount_removed(valid_orders):

    assert (
        valid_orders
        .filter(F.col("amount") < 0)
        .count()
    ) == 0


def test_null_amount_removed(valid_orders):

    assert (
        valid_orders
        .filter(F.col("amount").isNull())
        .count()
    ) == 0


def test_order_id_uniqueness(valid_orders):

    assert (
        valid_orders.count()
        == valid_orders.select("order_id").distinct().count()
    )


def test_order_status_normalization(valid_orders):

    order = (
        valid_orders
        .filter(F.col("order_id") == 111)
        .first()
    )

    assert order.status == "PENDING"


def test_order_date_conversion(valid_orders):

    order = (
        valid_orders
        .filter(F.col("order_id") == 101)
        .first()
    )

    assert order.order_date == date(2026, 11, 1)


def test_order_amount_precision(valid_orders):

    order = (
        valid_orders
        .filter(F.col("order_id") == 101)
        .first()
    )

    assert order.amount == Decimal("100.00")


def test_order_schema(valid_orders):

    expected_schema = {
        "order_id": "bigint",
        "customer_id": "bigint",
        "order_date": "date",
        "amount": "decimal(12,2)",
        "status": "string"
    }

    assert dict(valid_orders.dtypes) == expected_schema


def test_invalid_order_date(
    spark,
    valid_customers,
    apply_expectations
):

    df = spark.createDataFrame(
        [(999, 1, "invalid-date", 100.0, "COMPLETED")],
        [
            "order_id",
            "customer_id",
            "order_date",
            "amount",
            "status"
        ]
    )

    result = clean_orders(df, valid_customers)

    result = apply_expectations(result, ORDER_RULES)

    assert result.count() == 0


def test_final_order_count(valid_orders):

    assert valid_orders.count() == 8