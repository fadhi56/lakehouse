from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.dummy_data import generate_orders


@dp.materialized_view(
    name="bronze_orders",
    comment="Raw order data with ingestion metadata"
)
def bronze_orders():

    return (
        generate_orders(spark)
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_source_system",
            F.lit("dummy_orders")
        )
    )