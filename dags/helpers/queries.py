query_schema_facts = "CREATE SCHEMA IF NOT EXISTS facts;"

query_fact_table = """
    CREATE TABLE IF NOT EXISTS facts.sales(
        customer_rfc VARCHAR(13) NOT NULL,
        item_name VARCHAR(50) NOT NULL,
        item_quantity_bought INTEGER,
        item_id INTEGER,
        store_name VARCHAR(50) NOT NULL,
        total_bought NUMERIC (5, 2),
        purchase_date DATE NOT NULL,
        PRIMARY KEY (item_id,customer_rfc,purchase_date)
    );
"""

query_customers = """
    SELECT * FROM staging.customers;
"""

query_items = """
    SELECT * FROM staging.items;
"""

query_stores = """
    SELECT * FROM staging.stores;
"""
