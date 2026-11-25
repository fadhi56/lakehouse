import os
import sys
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utilities.dummy_data import generate_customers, generate_orders
from utilities.helpers import clean_customers, clean_orders
from utilities.data_quality import CUSTOMER_RULES, ORDER_RULES

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Create a reusable local SparkSession for all tests."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("LakehousePipelineTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def raw_customers(spark):
    """Return dummy customer source data."""

    return generate_customers(spark)


@pytest.fixture
def raw_orders(spark):
    """Return dummy order source data."""

    return generate_orders(spark)


@pytest.fixture
def apply_expectations():
    """Locally simulate Lakeflow drop expectations."""

    def apply_rules(df, rules):

        for rule in rules.values():
            df = df.filter(F.expr(rule))

        return df

    return apply_rules


@pytest.fixture
def valid_customers(raw_customers, apply_expectations):
    """Return normalized and validated customers."""

    df = clean_customers(raw_customers)

    return apply_expectations(df, CUSTOMER_RULES)


@pytest.fixture
def valid_orders(raw_orders, valid_customers, apply_expectations):
    """Return normalized and validated orders."""

    df = clean_orders(raw_orders, valid_customers)

    return apply_expectations(df, ORDER_RULES)