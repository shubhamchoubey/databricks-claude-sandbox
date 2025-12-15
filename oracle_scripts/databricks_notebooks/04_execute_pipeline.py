# Databricks notebook source
# MAGIC %md
# MAGIC # Execute Customer Analytics Pipeline
# MAGIC
# MAGIC This notebook executes the customer tier processing pipeline.
# MAGIC Equivalent to Oracle execution_proc.sql script.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Display tier counts before processing
# MAGIC 2. Execute process_customer_tiers()
# MAGIC 3. Display tier counts after processing
# MAGIC 4. Show sample results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Run Functions from Analytics Package

# COMMAND ----------

# MAGIC %run ./03_customer_analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check Tier Counts Before Processing

# COMMAND ----------

print("=" * 60)
print("CUSTOMER TIER ANALYSIS - BEFORE PROCESSING")
print("=" * 60)

gold_before = get_tier_count('GOLD')
silver_before = get_tier_count('SILVER')
bronze_before = get_tier_count('BRONZE')

print(f"\n📊 Tier Distribution BEFORE:")
print(f"   Gold Customers:   {gold_before:>5}")
print(f"   Silver Customers: {silver_before:>5}")
print(f"   Bronze Customers: {bronze_before:>5}")
print(f"   {'─' * 30}")
print(f"   Total:            {gold_before + silver_before + bronze_before:>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Execute Customer Tier Processing

# COMMAND ----------

print("\n" + "=" * 60)
print("EXECUTING TIER PROCESSING...")
print("=" * 60 + "\n")

# Execute the main procedure
update_count = process_customer_tiers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check Tier Counts After Processing

# COMMAND ----------

print("\n" + "=" * 60)
print("CUSTOMER TIER ANALYSIS - AFTER PROCESSING")
print("=" * 60)

gold_after = get_tier_count('GOLD')
silver_after = get_tier_count('SILVER')
bronze_after = get_tier_count('BRONZE')

print(f"\n📊 Tier Distribution AFTER:")
print(f"   Gold Customers:   {gold_after:>5}")
print(f"   Silver Customers: {silver_after:>5}")
print(f"   Bronze Customers: {bronze_after:>5}")
print(f"   {'─' * 30}")
print(f"   Total:            {gold_after + silver_after + bronze_after:>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Compare Before vs After

# COMMAND ----------

print("\n" + "=" * 60)
print("TIER CHANGES SUMMARY")
print("=" * 60)

print(f"\n📈 Tier Changes:")
print(f"   Gold:   {gold_before:>5} → {gold_after:>5} ({'+' if gold_after > gold_before else ''}{gold_after - gold_before})")
print(f"   Silver: {silver_before:>5} → {silver_after:>5} ({'+' if silver_after > silver_before else ''}{silver_after - silver_before})")
print(f"   Bronze: {bronze_before:>5} → {bronze_after:>5} ({'+' if bronze_after > bronze_before else ''}{bronze_after - bronze_before})")

print(f"\n✅ Total Customers Updated: {update_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Show Top Customers by Tier

# COMMAND ----------

print("\n" + "=" * 60)
print("TOP CUSTOMERS BY TIER")
print("=" * 60)

# Query to show top customers with their spending
top_customers_query = """
SELECT
    c.customer_id,
    c.cust_name,
    c.segment,
    ROUND(SUM(p.unit_price * t.quantity), 2) as total_spend,
    COUNT(DISTINCT t.txn_id) as transaction_count
FROM customers c
JOIN sales_transactions t ON c.customer_id = t.customer_id
JOIN products p ON t.product_id = p.product_id
GROUP BY c.customer_id, c.cust_name, c.segment
ORDER BY total_spend DESC
LIMIT 20
"""

print("\n🏆 Top 20 Customers by Total Spend:")
display(spark.sql(top_customers_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Tier Distribution with Spending Stats

# COMMAND ----------

print("\n" + "=" * 60)
print("DETAILED TIER STATISTICS")
print("=" * 60)

tier_stats_query = """
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
        t.customer_id,
        SUM(p.unit_price * t.quantity) as total_spend
    FROM sales_transactions t
    JOIN products p ON t.product_id = p.product_id
    GROUP BY t.customer_id
) spend ON c.customer_id = spend.customer_id
GROUP BY c.segment
ORDER BY
    CASE c.segment
        WHEN 'GOLD' THEN 1
        WHEN 'SILVER' THEN 2
        WHEN 'BRONZE' THEN 3
    END
"""

print("\n📊 Spending Statistics by Tier:")
display(spark.sql(tier_stats_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("EXECUTION COMPLETE!")
print("=" * 60)

print(f"""
✅ Pipeline executed successfully!

📊 Results Summary:
   • Customers Processed: {gold_after + silver_after + bronze_after}
   • Customers Updated:   {update_count}
   • Gold Tier:          {gold_after} customers
   • Silver Tier:        {silver_after} customers
   • Bronze Tier:        {bronze_after} customers

🎯 Business Rules Applied:
   • GOLD:   >= $3,000 total spend
   • SILVER: >= $1,000 total spend
   • BRONZE: <  $1,000 total spend

📈 Proceed to notebook 05 for detailed validation
""")
