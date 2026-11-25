from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.dummy_data import generate_customers


@dp.materialized_view(
    name="bronze_customers",
    comment="Raw customer data with ingestion metadata"
)
def bronze_customers():

    return (
        generate_customers(spark)
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_source_system",
            F.lit("dummy_customers")
        )
    )