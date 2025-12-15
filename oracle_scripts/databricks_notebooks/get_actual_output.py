# Databricks notebook source
# Get actual tier distribution results

# COMMAND ----------

spark.sql("USE oracle_migration")

# COMMAND ----------

# Get tier counts
tier_counts = spark.sql("""
    SELECT segment, COUNT(*) as count
    FROM customers
    GROUP BY segment
    ORDER BY segment
""")

print("=== ACTUAL OUTPUT ===\n")
tier_data = {row.segment: row['count'] for row in tier_counts.collect()}

gold_count = tier_data.get('GOLD', 0)
silver_count = tier_data.get('SILVER', 0)
bronze_count = tier_data.get('BRONZE', 0)

print(f"Gold Customers: {gold_count}")
print(f"Silver Customers: {silver_count}")
print(f"Bronze Customers: {bronze_count}")
print(f"Total: {gold_count + silver_count + bronze_count}")

# COMMAND ----------

# Get spending statistics by tier
print("\n=== SPENDING BY TIER ===\n")
spending_stats = spark.sql("""
    SELECT
        c.segment,
        COUNT(DISTINCT c.customer_id) as customer_count,
        ROUND(MIN(spend.total_spend), 2) as min_spend,
        ROUND(AVG(spend.total_spend), 2) as avg_spend,
        ROUND(MAX(spend.total_spend), 2) as max_spend,
        ROUND(SUM(spend.total_spend), 2) as total_revenue
    FROM customers c
    LEFT JOIN (
        SELECT
            customer_id,
            SUM(p.unit_price * t.quantity) as total_spend
        FROM sales_transactions t
        JOIN products p ON t.product_id = p.product_id
        GROUP BY customer_id
    ) spend ON c.customer_id = spend.customer_id
    GROUP BY c.segment
    ORDER BY
        CASE c.segment
            WHEN 'GOLD' THEN 1
            WHEN 'SILVER' THEN 2
            WHEN 'BRONZE' THEN 3
        END
""")

display(spending_stats)

# COMMAND ----------

# Show sample customers from each tier
print("\n=== SAMPLE CUSTOMERS ===\n")
samples = spark.sql("""
    SELECT
        c.customer_id,
        c.cust_name,
        c.segment,
        ROUND(SUM(p.unit_price * t.quantity), 2) as total_spend
    FROM customers c
    JOIN sales_transactions t ON c.customer_id = t.customer_id
    JOIN products p ON t.product_id = p.product_id
    GROUP BY c.customer_id, c.cust_name, c.segment
    ORDER BY total_spend DESC
    LIMIT 20
""")

display(samples)
