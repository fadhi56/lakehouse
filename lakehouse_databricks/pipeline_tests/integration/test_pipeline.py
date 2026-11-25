import pytest

from decimal import Decimal

from pyspark.sql import functions as F

from utilities.dummy_data import (
    generate_customers,
    generate_orders
)

from utilities.helpers import (
    clean_customers,
    clean_orders,
    calculate_customer_kpis
)

from utilities.data_quality import (
    CUSTOMER_RULES,
    ORDER_RULES,
    GOLD_RULES
)


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def pipeline_results(spark):
    """Execute the transformation chain from Bronze to Gold."""

    # Bronze
    bronze_customers = (
        generate_customers(spark)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("dummy_customers"))
    )

    bronze_orders = (
        generate_orders(spark)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("dummy_orders"))
    )

    # Silver customers
    silver_customers = clean_customers(bronze_customers)

    for rule in CUSTOMER_RULES.values():
        silver_customers = silver_customers.filter(F.expr(rule))

    # Silver orders
    silver_orders = clean_orders(
        bronze_orders,
        silver_customers
    )

    for rule in ORDER_RULES.values():
        silver_orders = silver_orders.filter(F.expr(rule))

    # Gold
    gold_customer_kpis = calculate_customer_kpis(
        silver_customers,
        silver_orders
    )

    results = {
        "bronze_customers": bronze_customers,
        "bronze_orders": bronze_orders,
        "silver_customers": silver_customers,
        "silver_orders": silver_orders,
        "gold_customer_kpis": gold_customer_kpis
    }

    yield results

@pytest.mark.parametrize(
    "table_name, expected_count",
    [
        ("bronze_customers", 9),
        ("bronze_orders", 12),
        ("silver_customers", 7),
        ("silver_orders", 8),
        ("gold_customer_kpis", 7)
    ]
)
def test_pipeline_record_counts(
    pipeline_results,
    table_name,
    expected_count
):

    assert pipeline_results[table_name].count() == expected_count

def test_pipeline_referential_integrity(pipeline_results):

    customers = pipeline_results["silver_customers"]

    orders = pipeline_results["silver_orders"]

    orphan_orders = orders.join(
        customers.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )

    assert orphan_orders.count() == 0

def test_pipeline_business_kpis(pipeline_results):

    gold = pipeline_results["gold_customer_kpis"]

    metrics = gold.agg(
        F.sum("total_orders").alias("orders"),
        F.sum("completed_orders").alias("completed"),
        F.sum("cancelled_orders").alias("cancelled"),
        F.sum("pending_orders").alias("pending"),
        F.sum("total_revenue").alias("revenue")
    ).first()

    assert metrics.orders == 8
    assert metrics.completed == 6
    assert metrics.cancelled == 1
    assert metrics.pending == 1
    assert metrics.revenue == Decimal("960.00")

def test_gold_data_quality_rules(pipeline_results):

    gold = pipeline_results["gold_customer_kpis"]

    for rule_name, rule_expression in GOLD_RULES.items():

        valid_expression = F.coalesce(
            F.expr(rule_expression),
            F.lit(False)
        )

        invalid_records = gold.filter(
            ~valid_expression
        ).count()

        assert invalid_records == 0, (
            f"Data quality rule failed: {rule_name}"
        )

def test_revenue_reconciliation(pipeline_results):

    silver = pipeline_results["silver_orders"]

    gold = pipeline_results["gold_customer_kpis"]

    silver_revenue = (
        silver
        .filter(F.col("status") == "COMPLETED")
        .agg(F.sum("amount").alias("revenue"))
        .first()
        .revenue
    )

    gold_revenue = (
        gold
        .agg(F.sum("total_revenue").alias("revenue"))
        .first()
        .revenue
    )

    assert silver_revenue == gold_revenue
    assert gold_revenue == Decimal("960.00")

def test_customer_segment_distribution(pipeline_results):

    gold = pipeline_results["gold_customer_kpis"]

    segments = {
        row["customer_segment"]: row["count"]
        for row in (
            gold
            .groupBy("customer_segment")
            .count()
            .collect()
        )
    }

    assert segments == {
        "HIGH": 2,
        "MEDIUM": 2,
        "LOW": 3
    }

def test_bronze_data_preservation(pipeline_results):

    customers = pipeline_results["bronze_customers"]

    orders = pipeline_results["bronze_orders"]

    assert customers.count() == 9
    assert orders.count() == 12

    assert customers.filter(
        F.col("customer_id").isNull()
    ).count() == 1

    assert orders.filter(
        F.col("amount") < 0
    ).count() == 1

    assert "_ingestion_timestamp" in customers.columns
    assert "_source_system" in customers.columns

def test_order_status_reconciliation(pipeline_results):

    gold = pipeline_results["gold_customer_kpis"]

    invalid_rows = gold.filter(
        F.col("total_orders") != (
            F.col("completed_orders")
            + F.col("cancelled_orders")
            + F.col("pending_orders")
        )
    )

    assert invalid_rows.count() == 0

