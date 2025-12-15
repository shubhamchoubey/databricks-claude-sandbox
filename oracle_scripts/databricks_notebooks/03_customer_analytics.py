# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Analytics Package
# MAGIC
# MAGIC This notebook contains the PySpark equivalent of the Oracle PL/SQL package `cust_analytics_pkg`.
# MAGIC
# MAGIC **Functions:**
# MAGIC - `get_tier_count(tier_name)` - Returns count of customers in a specific tier
# MAGIC - `process_customer_tiers()` - Calculates customer spending and updates tier segments

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, count, lit
from delta.tables import DeltaTable

# Set database name
database_name = "oracle_migration"
spark.sql(f"USE {database_name}")

print(f"Using database: {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function: get_tier_count

# COMMAND ----------

def get_tier_count(tier_name):
    """
    Returns the count of customers in a specific tier segment.

    Args:
        tier_name (str): The tier name ('GOLD', 'SILVER', or 'BRONZE')

    Returns:
        int: Count of customers in the specified tier
    """
    try:
        # Read customers table and filter by segment
        customers_df = spark.table("customers")
        tier_count = customers_df.filter(col("segment") == tier_name).count()

        return tier_count

    except Exception as e:
        print(f"Error in get_tier_count: {str(e)}")
        return 0

# Test the function
print("Testing get_tier_count function...")
bronze_count = get_tier_count('BRONZE')
print(f"BRONZE customers: {bronze_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function: process_customer_tiers

# COMMAND ----------

def process_customer_tiers():
    """
    Processes customer tiers based on total spending.

    This function:
    1. Calculates total spend per customer (sum of unit_price * quantity)
    2. Assigns tier based on spend: GOLD (>=3000), SILVER (>=1000), BRONZE (<1000)
    3. Updates customers table only for customers whose tier changed
    4. Returns the count of updated customers

    Returns:
        int: Number of customers updated
    """
    try:
        print("Starting customer tier processing...")

        # Read tables
        customers_df = spark.table("customers")
        products_df = spark.table("products")
        transactions_df = spark.table("sales_transactions")

        # Calculate total spend per customer using 3-way join
        # Equivalent to Oracle cursor:
        # SELECT c.customer_id, SUM(p.unit_price * t.quantity) AS total_spend
        # FROM customers c JOIN sales_transactions t ... JOIN products p ...
        # GROUP BY c.customer_id

        customer_spend_df = (
            transactions_df
            .join(products_df, transactions_df.product_id == products_df.product_id, "inner")
            .join(customers_df, transactions_df.customer_id == customers_df.customer_id, "inner")
            .groupBy(customers_df.customer_id)
            .agg(spark_sum(products_df.unit_price * transactions_df.quantity).alias("total_spend"))
        )

        # Apply business logic to assign new tier
        # Equivalent to Oracle IF-ELSIF-ELSE logic
        customer_tier_df = customer_spend_df.withColumn(
            "new_tier",
            when(col("total_spend") >= 3000, "GOLD")
            .when(col("total_spend") >= 1000, "SILVER")
            .otherwise("BRONZE")
        )

        # Join with current customers to identify changes
        # Only update where segment != new_tier (equivalent to Oracle WHERE clause)
        customers_current = customers_df.select("customer_id", col("segment").alias("current_segment"))

        changes_df = (
            customer_tier_df
            .join(customers_current, "customer_id", "inner")
            .filter(col("current_segment") != col("new_tier"))
            .select("customer_id", "new_tier", "total_spend")
        )

        update_count = changes_df.count()

        if update_count > 0:
            # Perform Delta MERGE operation to update customers table
            # Equivalent to Oracle UPDATE customers SET segment = v_new_tier WHERE ...

            customers_delta = DeltaTable.forName(spark, "customers")

            customers_delta.alias("target").merge(
                changes_df.alias("source"),
                "target.customer_id = source.customer_id"
            ).whenMatchedUpdate(
                set={"segment": col("source.new_tier")}
            ).execute()

            print(f"✓ Tier processing complete. Updated {update_count} customers.")

            # Show sample of updated customers
            print("\nSample of updated customers:")
            changes_df.orderBy(col("total_spend").desc()).show(10, truncate=False)
        else:
            print("✓ Tier processing complete. No customers required tier updates.")

        return update_count

    except Exception as e:
        print(f"❌ Error in process_customer_tiers: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0

# Test the function (optional - will be called from execution notebook)
# Uncomment the line below to test
# update_count = process_customer_tiers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Customer Analytics Package Functions Defined:")
print("   - get_tier_count(tier_name)")
print("   - process_customer_tiers()")
print("\n📊 Ready to execute in notebook 04")
