from pyspark.sql import DataFrame, SparkSession


def generate_customers(spark: SparkSession) -> DataFrame:
    """Generate synthetic customer source data."""

    return spark.sql("""
        SELECT *
        FROM VALUES
            (1, 'Alice',   'DE', 'alice@example.com'),
            (2, 'Bob',     'UK', 'bob@example.com'),
            (3, 'Charlie', 'DE', 'charlie@example.com'),
            (4, 'David',   'FR', 'david@example.com'),
            (5, 'Emma',    'DE', 'emma@example.com'),
            (6, 'Frank',   'UK', NULL),
            (2, 'Bob',     'UK', 'bob@example.com'),
            (NULL, 'Unknown', 'DE', 'unknown@example.com'),
            (7, '  Grace  ', 'de', 'GRACE@EXAMPLE.COM')

        AS customers(
            customer_id,
            customer_name,
            country,
            email
        )
    """)


def generate_orders(spark: SparkSession) -> DataFrame:
    """Generate synthetic order source data."""

    return spark.sql("""
        SELECT *
        FROM VALUES
            (101, 1, '2026-11-01', 100.00, 'COMPLETED'),
            (102, 1, '2026-11-02', 150.00, 'COMPLETED'),
            (103, 2, '2026-11-03', 200.00, 'COMPLETED'),
            (104, 3, '2026-11-04', 50.00,  'CANCELLED'),
            (105, 3, '2026-11-05', 300.00, 'COMPLETED'),
            (106, 4, '2026-11-06', -25.00, 'COMPLETED'),
            (107, 5, '2026-11-07', 120.00, 'COMPLETED'),
            (108, 999, '2026-11-08', 80.00, 'COMPLETED'),
            (109, 7, '2026-11-09', 90.00,  'COMPLETED'),
            (101, 1, '2026-11-01', 100.00, 'COMPLETED'),
            (110, 2, '2026-11-10', NULL,   'COMPLETED'),
            (111, 2, '2026-11-11', 75.00,  'pending')

        AS orders(
            order_id,
            customer_id,
            order_date,
            amount,
            status
        )
    """)
