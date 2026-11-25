from pyspark import pipelines as dp

from utilities.helpers import clean_orders
from utilities.data_quality import ORDER_RULES


@dp.materialized_view(
    name="silver_orders",
    comment="Cleaned and validated customer orders"
)
@dp.expect_all_or_drop(ORDER_RULES)
def silver_orders():

    orders_df = spark.read.table("bronze_orders")

    customers_df = spark.read.table("silver_customers")

    return clean_orders(
        orders_df=orders_df,
        customers_df=customers_df
    )