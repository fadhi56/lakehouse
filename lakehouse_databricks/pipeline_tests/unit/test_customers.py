import pytest

from pyspark.sql import functions as F

from utilities.helpers import clean_customers
from utilities.data_quality import CUSTOMER_RULES


pytestmark = pytest.mark.unit


def test_raw_customer_count(raw_customers):
    assert raw_customers.count() == 9


def test_customer_duplicate_removal(raw_customers):

    result = clean_customers(raw_customers)

    assert result.count() == 8


def test_customer_null_id_removed(valid_customers):

    invalid_count = (
        valid_customers
        .filter(F.col("customer_id").isNull())
        .count()
    )

    assert invalid_count == 0


def test_customer_id_uniqueness(valid_customers):

    assert (
        valid_customers.count()
        == valid_customers.select("customer_id").distinct().count()
    )


def test_customer_name_normalization(valid_customers):

    customer = (
        valid_customers
        .filter(F.col("customer_id") == 7)
        .first()
    )

    assert customer.customer_name == "Grace"


def test_country_normalization(valid_customers):

    customer = (
        valid_customers
        .filter(F.col("customer_id") == 7)
        .first()
    )

    assert customer.country == "DE"


def test_email_normalization(valid_customers):

    customer = (
        valid_customers
        .filter(F.col("customer_id") == 7)
        .first()
    )

    assert customer.email == "grace@example.com"


def test_optional_email(valid_customers):

    customer = (
        valid_customers
        .filter(F.col("customer_id") == 6)
        .first()
    )

    assert customer is not None
    assert customer.email is None


def test_customer_schema(valid_customers):

    expected_schema = {
        "customer_id": "bigint",
        "customer_name": "string",
        "country": "string",
        "email": "string"
    }

    assert dict(valid_customers.dtypes) == expected_schema


def test_invalid_country(spark, apply_expectations):

    df = spark.createDataFrame(
        [(100, "Test Customer", "INVALID", "test@example.com")],
        ["customer_id", "customer_name", "country", "email"]
    )

    result = apply_expectations(
        clean_customers(df),
        CUSTOMER_RULES
    )

    assert result.count() == 0


def test_final_customer_count(valid_customers):

    assert valid_customers.count() == 7