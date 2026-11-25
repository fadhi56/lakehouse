from pyspark import pipelines as dp

from utilities.helpers import clean_customers
from utilities.data_quality import CUSTOMER_RULES


@dp.materialized_view(
    name="silver_customers",
    comment="Cleaned and validated customer data"
)
@dp.expect_all_or_drop(CUSTOMER_RULES)
def silver_customers():

    bronze_df = spark.read.table("bronze_customers")

    return clean_customers(bronze_df)