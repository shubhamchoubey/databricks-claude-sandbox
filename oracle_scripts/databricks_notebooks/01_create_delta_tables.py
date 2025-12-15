# Databricks notebook source
# MAGIC %md
# MAGIC # Create Delta Tables
# MAGIC
# MAGIC This notebook creates the Delta tables equivalent to Oracle table definitions.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - customers
# MAGIC - products
# MAGIC - sales_transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
from delta.tables import DeltaTable

# Set database name (you can change this)
database_name = "oracle_migration"

print(f"Creating tables in database: {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database

# COMMAND ----------

# Create database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

print(f"✓ Database '{database_name}' created/selected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Existing Tables (if any)

# COMMAND ----------

# Drop tables if they exist to ensure clean state
spark.sql("DROP TABLE IF EXISTS sales_transactions")
spark.sql("DROP TABLE IF EXISTS products")
spark.sql("DROP TABLE IF EXISTS customers")

print("✓ Existing tables dropped (if any)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Customers Table

# COMMAND ----------

# Create customers table
spark.sql("""
CREATE TABLE customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY,
    cust_name STRING,
    segment STRING,
    signup_date DATE
)
USING DELTA
COMMENT 'Customer master table with tier segments - segment defaults to GOLD in data generation'
""")

print("✓ Created table: customers")

# Describe the table
display(spark.sql("DESCRIBE EXTENDED customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Products Table

# COMMAND ----------

# Create products table
spark.sql("""
CREATE TABLE products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY,
    product_name STRING,
    category STRING,
    unit_price DECIMAL(10, 2)
)
USING DELTA
COMMENT 'Product catalog table'
""")

print("✓ Created table: products")

# Describe the table
display(spark.sql("DESCRIBE EXTENDED products"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sales Transactions Table

# COMMAND ----------

# Create sales_transactions table
spark.sql("""
CREATE TABLE sales_transactions (
    txn_id BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id BIGINT,
    product_id BIGINT,
    quantity INT,
    txn_date DATE
)
USING DELTA
COMMENT 'Sales transactions fact table'
""")

print("✓ Created table: sales_transactions")

# Describe the table
display(spark.sql("DESCRIBE EXTENDED sales_transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table Creation

# COMMAND ----------

# List all tables in the database
print("=== Tables Created ===")
display(spark.sql("SHOW TABLES"))

# Verify each table structure
print("\n=== Table Structures ===")
for table_name in ['customers', 'products', 'sales_transactions']:
    print(f"\n{table_name.upper()}:")
    spark.sql(f"DESCRIBE {table_name}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Delta Table Creation Complete!")
print(f"✅ Database: {database_name}")
print("✅ Tables Created:")
print("   - customers (customer_id, cust_name, segment, signup_date)")
print("   - products (product_id, product_name, category, unit_price)")
print("   - sales_transactions (txn_id, customer_id, product_id, quantity, txn_date)")
print("\n📊 Ready for data generation in notebook 02")
