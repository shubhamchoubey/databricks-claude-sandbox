# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Sample Data
# MAGIC
# MAGIC This notebook generates sample data equivalent to the Oracle data_generator.sql script.
# MAGIC
# MAGIC **Data Generation:**
# MAGIC - 50 products (3 categories: Electronics, Home, Clothing)
# MAGIC - 500 customers (random signup dates)
# MAGIC - 10,000 sales transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, current_date, date_sub, concat, round as spark_round
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, DateType, DecimalType
from datetime import datetime, timedelta
from decimal import Decimal
import random

# Set database name
database_name = "oracle_migration"
spark.sql(f"USE {database_name}")

print(f"Using database: {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Products Data

# COMMAND ----------

print("Generating 50 products...")

# Generate 50 products
products_data = []
for i in range(1, 51):
    product_name = f"Product_{i}"
    # Category based on modulo: Electronics, Home, Clothing
    if i % 3 == 0:
        category = "Electronics"
    elif i % 3 == 1:
        category = "Home"
    else:
        category = "Clothing"

    # Random price between 10 and 500 (convert to Decimal)
    unit_price = Decimal(str(round(random.uniform(10, 500), 2)))

    products_data.append((product_name, category, unit_price))

# Create DataFrame
products_schema = StructType([
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DecimalType(10, 2), False)
])

products_df = spark.createDataFrame(products_data, schema=products_schema)

# Write to Delta table
products_df.write.format("delta").mode("append").saveAsTable("products")

print(f"✓ Generated and inserted {products_df.count()} products")
display(products_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customers Data

# COMMAND ----------

print("Generating 500 customers...")

# Generate 500 customers
customers_data = []
for i in range(1, 501):
    cust_name = f"Customer_{i}"
    # Random signup date within last 1000 days
    days_ago = random.randint(1, 1000)

    customers_data.append((cust_name, days_ago))

# Create DataFrame with date calculation
customers_schema = StructType([
    StructField("cust_name", StringType(), False),
    StructField("days_ago", IntegerType(), False)
])

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# Add signup_date column by subtracting days from current_date
customers_df = customers_df.withColumn("signup_date", date_sub(current_date(), col("days_ago")))
# Add segment column with default value 'GOLD' (all customers start as GOLD)
customers_df = customers_df.withColumn("segment", lit("GOLD"))
customers_df = customers_df.select("cust_name", "segment", "signup_date")

# Write to Delta table
customers_df.write.format("delta").mode("append").saveAsTable("customers")

print(f"✓ Generated and inserted {customers_df.count()} customers")
display(customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sales Transactions Data

# COMMAND ----------

print("Generating 10,000 sales transactions...")

# Generate 10,000 transactions
transactions_data = []
for i in range(1, 10001):
    # Random customer ID (1-500)
    customer_id = random.randint(1, 500)
    # Random product ID (1-50)
    product_id = random.randint(1, 50)
    # Random quantity (1-5)
    quantity = random.randint(1, 5)
    # Random transaction date within last 365 days
    days_ago = random.randint(0, 365)

    transactions_data.append((customer_id, product_id, quantity, days_ago))

# Create DataFrame
transactions_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("product_id", LongType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("days_ago", IntegerType(), False)
])

transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

# Add txn_date column
transactions_df = transactions_df.withColumn("txn_date", date_sub(current_date(), col("days_ago")))
transactions_df = transactions_df.select("customer_id", "product_id", "quantity", "txn_date")

# Write to Delta table
transactions_df.write.format("delta").mode("append").saveAsTable("sales_transactions")

print(f"✓ Generated and inserted {transactions_df.count()} transactions")
display(transactions_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Generation

# COMMAND ----------

print("=== Data Generation Summary ===\n")

# Count records in each table
products_count = spark.table("products").count()
customers_count = spark.table("customers").count()
transactions_count = spark.table("sales_transactions").count()

print(f"Products: {products_count} rows")
print(f"Customers: {customers_count} rows")
print(f"Sales Transactions: {transactions_count} rows")

print("\n=== Product Categories Distribution ===")
display(spark.sql("SELECT category, COUNT(*) as count FROM products GROUP BY category ORDER BY category"))

print("\n=== Customer Segment Distribution (Before Processing) ===")
display(spark.sql("SELECT segment, COUNT(*) as count FROM customers GROUP BY segment"))

print("\n=== Sample Transactions with Details ===")
display(spark.sql("""
    SELECT
        t.txn_id,
        c.cust_name,
        p.product_name,
        p.category,
        t.quantity,
        p.unit_price,
        ROUND(p.unit_price * t.quantity, 2) as line_total,
        t.txn_date
    FROM sales_transactions t
    JOIN customers c ON t.customer_id = c.customer_id
    JOIN products p ON t.product_id = p.product_id
    ORDER BY t.txn_id
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Data Generation Complete!")
print(f"✅ Total Records Generated: {products_count + customers_count + transactions_count}")
print("✅ Ready for analytics processing in notebook 03 and execution in notebook 04")
