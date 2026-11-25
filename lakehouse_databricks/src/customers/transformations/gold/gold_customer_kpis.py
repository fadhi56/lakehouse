from pyspark import pipelines as dp

from utilities.helpers import calculate_customer_kpis
from utilities.data_quality import GOLD_RULES


@dp.materialized_view(
    name="gold_customer_kpis",
    comment="Customer-level revenue, order statistics and segmentation"
)
@dp.expect_all_or_fail(GOLD_RULES)
def gold_customer_kpis():

    customers_df = spark.read.table("silver_customers")

    orders_df = spark.read.table("silver_orders")

    return calculate_customer_kpis(
        customers_df=customers_df,
        orders_df=orders_df
    )