CUSTOMER_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_customer_name": """
        customer_name IS NOT NULL
        AND LENGTH(TRIM(customer_name)) > 0
    """,
    "valid_country": "country IN ('DE', 'UK', 'FR')"
}


ORDER_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_order_date": "order_date IS NOT NULL",
    "valid_amount": "amount IS NOT NULL AND amount >= 0",
    "valid_status": """
        status IN ('COMPLETED', 'CANCELLED', 'PENDING')
    """
}


GOLD_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_total_revenue": "total_revenue >= 0",
    "valid_total_orders": "total_orders >= 0",
    "valid_completed_orders": """
        completed_orders >= 0
        AND completed_orders <= total_orders
    """
}