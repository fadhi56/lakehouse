from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_customers(df: DataFrame) -> DataFrame:
    """Normalize and deduplicate customer data."""

    return (
        df
        .select(
            F.col("customer_id").cast("long"),
            F.trim(F.col("customer_name")).alias("customer_name"),
            F.upper(F.trim(F.col("country"))).alias("country"),
            F.lower(F.trim(F.col("email"))).alias("email")
        )
        .dropDuplicates()
    )


def clean_orders(
    orders_df: DataFrame,
    customers_df: DataFrame
) -> DataFrame:
    """Normalize orders and enforce customer referential integrity."""

    orders_cleaned = (
        orders_df
        .select(
            F.col("order_id").cast("long"),
            F.col("customer_id").cast("long"),
            F.expr("try_cast(order_date AS DATE)").alias("order_date"),
            F.col("amount").cast("decimal(12,2)"),
            F.upper(F.trim(F.col("status"))).alias("status")
        )
        .dropDuplicates()
    )

    # Remove orders belonging to non-existent customers
    return (
        orders_cleaned
        .join(
            customers_df.select("customer_id").distinct(),
            on="customer_id",
            how="left_semi"
        )
    )


def calculate_customer_kpis(
    customers_df: DataFrame,
    orders_df: DataFrame
) -> DataFrame:
    """Calculate aggregated customer-level business KPIs."""

    order_kpis = (
        orders_df
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),

            F.count(
                F.when(F.col("status") == "COMPLETED", 1)
            ).alias("completed_orders"),

            F.count(
                F.when(F.col("status") == "CANCELLED", 1)
            ).alias("cancelled_orders"),

            F.count(
                F.when(F.col("status") == "PENDING", 1)
            ).alias("pending_orders"),

            F.sum(
                F.when(
                    F.col("status") == "COMPLETED",
                    F.col("amount")
                ).otherwise(F.lit(0))
            ).alias("total_revenue"),

            F.avg(
                F.when(
                    F.col("status") == "COMPLETED",
                    F.col("amount")
                )
            ).alias("average_order_value"),

            F.max("order_date").alias("last_order_date")
        )
    )

    return (
        customers_df
        .join(
            order_kpis,
            on="customer_id",
            how="left"
        )
        .fillna({
            "total_orders": 0,
            "completed_orders": 0,
            "cancelled_orders": 0,
            "pending_orders": 0,
            "total_revenue": 0,
            "average_order_value": 0
        })
        .withColumn(
            "total_revenue",
            F.col("total_revenue").cast("decimal(18,2)")
        )
        .withColumn(
            "average_order_value",
            F.col("average_order_value").cast("decimal(18,2)")
        )
        .withColumn(
            "customer_segment",
            F.when(F.col("total_revenue") >= 250, "HIGH")
            .when(F.col("total_revenue") >= 100, "MEDIUM")
            .otherwise("LOW")
        )
    )