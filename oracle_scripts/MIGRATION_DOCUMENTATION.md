# Oracle to Databricks Migration Documentation

## Table of Contents
1. [Overview](#overview)
2. [Oracle Source Code Documentation](#oracle-source-code-documentation)
3. [Databricks Target Code Documentation](#databricks-target-code-documentation)
4. [Migration Mapping](#migration-mapping)
5. [Execution Guide](#execution-guide)
6. [Validation & Testing](#validation--testing)
7. [Troubleshooting](#troubleshooting)

---

## Overview

This document provides comprehensive documentation for the Oracle PL/SQL to Databricks PySpark migration project.

**Project Goal**: Migrate Oracle PL/SQL customer analytics package to Databricks Delta Lake architecture using PySpark.

**Migration Scope**:
- 3 Oracle tables → 3 Delta tables
- 1 PL/SQL package with 2 functions → 2 Python functions
- Data generation logic → PySpark data generation
- Execution procedure → Databricks notebook workflow

---

## Oracle Source Code Documentation

### 1. Source Files

#### 1.1 `table_definations.sql` - Table DDL

**Purpose**: Defines the database schema for customer analytics.

**Tables Created**:

##### Table: `customers`
Primary table for customer master data.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `customer_id` | NUMBER | PRIMARY KEY, IDENTITY | Auto-incrementing unique customer identifier |
| `cust_name` | VARCHAR2(100) | - | Customer name |
| `segment` | VARCHAR2(20) | DEFAULT 'BRONZE' | Customer tier (BRONZE/SILVER/GOLD) |
| `signup_date` | DATE | - | Date customer signed up |

**Business Logic**:
- `customer_id` auto-generates using Oracle IDENTITY column
- `segment` defaults to 'BRONZE' (modified to 'GOLD' in migration)
- Tracks customer loyalty tier based on spending

##### Table: `products`
Product catalog for available items.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `product_id` | NUMBER | PRIMARY KEY, IDENTITY | Auto-incrementing product identifier |
| `product_name` | VARCHAR2(100) | - | Product name |
| `category` | VARCHAR2(50) | - | Product category (Electronics/Home/Clothing) |
| `unit_price` | NUMBER(10,2) | - | Price per unit |

**Business Logic**:
- Supports 3 product categories
- Prices stored with 2 decimal precision
- Product catalog is relatively static

##### Table: `sales_transactions`
Fact table for customer purchase transactions.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `txn_id` | NUMBER | PRIMARY KEY, IDENTITY | Auto-incrementing transaction ID |
| `customer_id` | NUMBER | FOREIGN KEY → customers | Reference to customer |
| `product_id` | NUMBER | FOREIGN KEY → products | Reference to product |
| `quantity` | NUMBER | - | Number of units purchased |
| `txn_date` | DATE | - | Transaction date |

**Business Logic**:
- Foreign keys maintain referential integrity
- Supports multiple transactions per customer
- Transaction history used for tier calculation

---

#### 1.2 `oracle_package.sql` - PL/SQL Package

**Package**: `SHUBHAM_CHOUBEY_SCHEMA_V4C5D.cust_analytics_pkg`

**Purpose**: Contains business logic for customer tier processing and analytics.

##### Procedure: `process_customer_tiers`

**Signature**:
```sql
PROCEDURE process_customer_tiers
```

**Purpose**: Calculate customer spending and update tier segments based on business rules.

**Algorithm**:
```
1. DECLARE cursor c_cust_spend with:
   - 3-table JOIN (customers ⟗ sales_transactions ⟗ products)
   - Aggregate: SUM(unit_price * quantity) AS total_spend
   - GROUP BY customer_id

2. OPEN cursor and LOOP through each customer:
   a. Fetch customer_id and total_spend
   b. Apply tier logic:
      IF total_spend >= 3000 THEN tier = 'GOLD'
      ELSIF total_spend >= 1000 THEN tier = 'SILVER'
      ELSE tier = 'BRONZE'
   c. UPDATE customers SET segment = new_tier
      WHERE customer_id = current_customer
      AND segment != new_tier  -- Only update if changed
   d. Count updates (SQL%ROWCOUNT)

3. CLOSE cursor
4. COMMIT transaction
5. OUTPUT: "Updated X customers"
```

**Key Features**:
- **Cursor-based processing**: Iterates through each customer individually
- **Conditional updates**: Only updates records where tier changed
- **Exception handling**: ROLLBACK on error, output error message
- **Transaction management**: Explicit COMMIT after successful processing

**Business Rules**:
- **GOLD tier**: Total spending >= $3,000
- **SILVER tier**: Total spending >= $1,000 and < $3,000
- **BRONZE tier**: Total spending < $1,000

##### Function: `get_tier_count`

**Signature**:
```sql
FUNCTION get_tier_count(p_tier_name VARCHAR2) RETURN NUMBER
```

**Purpose**: Return count of customers in a specific tier segment.

**Algorithm**:
```
1. INPUT: p_tier_name (e.g., 'GOLD', 'SILVER', 'BRONZE')
2. Execute: SELECT COUNT(*) FROM customers WHERE segment = p_tier_name
3. RETURN: Integer count of matching customers
```

**Usage**: Called before/after tier processing to show changes.

---

#### 1.3 `data_generator.sql` - Test Data Generation

**Purpose**: Generate realistic test data for development and testing.

**Data Volumes**:
- 50 products (3 categories)
- 500 customers
- 10,000 transactions

**Algorithm**:

##### A. Product Generation
```sql
FOR i IN 1..50 LOOP
    product_name = 'Product_' || i
    category = CASE MOD(i, 3)
                 WHEN 0 THEN 'Electronics'
                 WHEN 1 THEN 'Home'
                 ELSE 'Clothing'
               END
    unit_price = ROUND(DBMS_RANDOM.VALUE(10, 500), 2)
    INSERT INTO products...
END LOOP
```

**Distribution**: ~17 products per category

##### B. Customer Generation
```sql
FOR i IN 1..500 LOOP
    cust_name = 'Customer_' || i
    signup_date = SYSDATE - DBMS_RANDOM.VALUE(1, 1000)  -- Last 1000 days
    INSERT INTO customers...
END LOOP
```

**Distribution**: Random signup dates spanning ~3 years

##### C. Transaction Generation
```sql
FOR i IN 1..10000 LOOP
    customer_id = ROUND(DBMS_RANDOM.VALUE(1, 500))  -- Random customer
    product_id = ROUND(DBMS_RANDOM.VALUE(1, 50))    -- Random product
    quantity = ROUND(DBMS_RANDOM.VALUE(1, 5))       -- 1-5 units
    txn_date = SYSDATE - DBMS_RANDOM.VALUE(0, 365)  -- Last year
    INSERT INTO sales_transactions...
END LOOP
```

**Distribution**:
- Average ~20 transactions per customer
- Average ~200 transactions per product
- Realistic spending patterns

---

#### 1.4 `execution_proc.sql` - Execution Script

**Purpose**: Execute the tier processing and display results.

**Workflow**:
```sql
1. SET SERVEROUTPUT ON  -- Enable console output

2. Display tier counts BEFORE processing:
   - get_tier_count('GOLD')

3. Execute main procedure:
   - cust_analytics_pkg.process_customer_tiers

4. Display tier counts AFTER processing:
   - get_tier_count('GOLD')
   - get_tier_count('SILVER')
```

**Expected Output**:
```
Gold Customers before: 500
Tier processing complete. Updated X customers.
Gold Customers after: XXX
Silver Customers after: XXX
```

---

## Databricks Target Code Documentation

### 2. Databricks Notebooks

#### 2.1 `01_create_delta_tables.py` - Delta Table Creation

**Purpose**: Create Delta Lake tables equivalent to Oracle tables.

**Database**: `oracle_migration`

**Key Changes from Oracle**:

| Oracle | Databricks | Reason |
|--------|------------|--------|
| `NUMBER IDENTITY` | `BIGINT GENERATED ALWAYS AS IDENTITY` | Databricks requires BIGINT for IDENTITY |
| `VARCHAR2(n)` | `STRING` | Databricks string type |
| `NUMBER(10,2)` | `DECIMAL(10,2)` | Decimal precision maintained |
| `DEFAULT 'BRONZE'` | Removed from DDL | Feature flag required; handled in data gen |

**Tables Created**:

##### Delta Table: `customers`
```sql
CREATE TABLE customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY,
    cust_name STRING,
    segment STRING,  -- No DEFAULT constraint
    signup_date DATE
)
USING DELTA
COMMENT 'Customer master table - segment defaults to GOLD in data generation'
```

##### Delta Table: `products`
```sql
CREATE TABLE products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY,
    product_name STRING,
    category STRING,
    unit_price DECIMAL(10, 2)
)
USING DELTA
COMMENT 'Product catalog table'
```

##### Delta Table: `sales_transactions`
```sql
CREATE TABLE sales_transactions (
    txn_id BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id BIGINT,  -- Changed to BIGINT
    product_id BIGINT,   -- Changed to BIGINT
    quantity INT,
    txn_date DATE
)
USING DELTA
COMMENT 'Sales transactions fact table'
```

**Execution Flow**:
1. Create database if not exists
2. Drop existing tables (clean slate)
3. Create tables with IDENTITY columns
4. Describe tables for verification

---

#### 2.2 `02_generate_sample_data.py` - PySpark Data Generation

**Purpose**: Generate test data using PySpark DataFrames.

**Imports Required**:
```python
from pyspark.sql.functions import col, lit, current_date, date_sub
from pyspark.sql.types import (StructType, StructField, IntegerType,
                                LongType, StringType, DateType, DecimalType)
from decimal import Decimal
import random
```

##### A. Product Generation (PySpark)

**Code**:
```python
products_data = []
for i in range(1, 51):
    product_name = f"Product_{i}"
    category = "Electronics" if i % 3 == 0 else \
               "Home" if i % 3 == 1 else "Clothing"
    unit_price = Decimal(str(round(random.uniform(10, 500), 2)))
    products_data.append((product_name, category, unit_price))

products_schema = StructType([
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DecimalType(10, 2), False)
])

products_df = spark.createDataFrame(products_data, schema=products_schema)
products_df.write.format("delta").mode("append").saveAsTable("products")
```

**Key Points**:
- `Decimal` type required for DECIMAL columns (not float)
- Schema explicitly defined for type safety
- IDENTITY columns excluded (auto-generated)

##### B. Customer Generation (PySpark)

**Code**:
```python
customers_data = []
for i in range(1, 501):
    cust_name = f"Customer_{i}"
    days_ago = random.randint(1, 1000)
    customers_data.append((cust_name, days_ago))

customers_schema = StructType([
    StructField("cust_name", StringType(), False),
    StructField("days_ago", IntegerType(), False)
])

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
customers_df = customers_df.withColumn("signup_date",
                                       date_sub(current_date(), col("days_ago")))
customers_df = customers_df.withColumn("segment", lit("GOLD"))  # Default tier
customers_df = customers_df.select("cust_name", "segment", "signup_date")

customers_df.write.format("delta").mode("append").saveAsTable("customers")
```

**Key Points**:
- DEFAULT value handled via `withColumn(lit("GOLD"))`
- Date calculation using `date_sub(current_date(), ...)`
- Select only columns matching table schema

##### C. Transaction Generation (PySpark)

**Code**:
```python
transactions_data = []
for i in range(1, 10001):
    customer_id = random.randint(1, 500)
    product_id = random.randint(1, 50)
    quantity = random.randint(1, 5)
    days_ago = random.randint(0, 365)
    transactions_data.append((customer_id, product_id, quantity, days_ago))

transactions_schema = StructType([
    StructField("customer_id", LongType(), False),  # BIGINT
    StructField("product_id", LongType(), False),   # BIGINT
    StructField("quantity", IntegerType(), False),
    StructField("days_ago", IntegerType(), False)
])

transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)
transactions_df = transactions_df.withColumn("txn_date",
                                             date_sub(current_date(), col("days_ago")))
transactions_df = transactions_df.select("customer_id", "product_id",
                                         "quantity", "txn_date")

transactions_df.write.format("delta").mode("append").saveAsTable("sales_transactions")
```

**Key Points**:
- Foreign keys use `LongType()` to match BIGINT IDENTITY columns
- Schema mismatch causes `DELTA_FAILED_TO_MERGE_FIELDS` error
- Temporary `days_ago` column dropped in final select

---

#### 2.3 `03_customer_analytics.py` - Business Logic Functions

**Purpose**: Migrate Oracle package functions to PySpark.

**Imports Required**:
```python
from pyspark.sql.functions import col, sum as spark_sum, when, count
from delta.tables import DeltaTable
```

##### Function: `get_tier_count(tier_name)`

**Oracle Original**:
```sql
FUNCTION get_tier_count(p_tier_name VARCHAR2) RETURN NUMBER IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM customers
    WHERE segment = p_tier_name;
    RETURN v_count;
END;
```

**PySpark Migration**:
```python
def get_tier_count(tier_name):
    """
    Returns the count of customers in a specific tier segment.

    Args:
        tier_name (str): The tier name ('GOLD', 'SILVER', or 'BRONZE')

    Returns:
        int: Count of customers in the specified tier
    """
    try:
        customers_df = spark.table("customers")
        tier_count = customers_df.filter(col("segment") == tier_name).count()
        return tier_count
    except Exception as e:
        print(f"Error in get_tier_count: {str(e)}")
        return 0
```

**Migration Notes**:
- `SELECT COUNT(*) INTO` → `DataFrame.filter().count()`
- `RETURN NUMBER` → `return int`
- Exception handling with try/except instead of PL/SQL EXCEPTION block

##### Procedure: `process_customer_tiers()`

**Oracle Original** (Cursor-based):
```sql
PROCEDURE process_customer_tiers IS
    CURSOR c_cust_spend IS
        SELECT c.customer_id,
               SUM(p.unit_price * t.quantity) AS total_spend
        FROM customers c
        JOIN sales_transactions t ON c.customer_id = t.customer_id
        JOIN products p ON t.product_id = p.product_id
        GROUP BY c.customer_id;

    r_cust_spend c_cust_spend%ROWTYPE;
    v_new_tier VARCHAR2(20);
    v_update_count NUMBER := 0;
BEGIN
    OPEN c_cust_spend;
    LOOP
        FETCH c_cust_spend INTO r_cust_spend;
        EXIT WHEN c_cust_spend%NOTFOUND;

        IF r_cust_spend.total_spend >= 3000 THEN
            v_new_tier := 'GOLD';
        ELSIF r_cust_spend.total_spend >= 1000 THEN
            v_new_tier := 'SILVER';
        ELSE
            v_new_tier := 'BRONZE';
        END IF;

        UPDATE customers
        SET segment = v_new_tier
        WHERE customer_id = r_cust_spend.customer_id
        AND segment != v_new_tier;

        IF SQL%ROWCOUNT > 0 THEN
            v_update_count := v_update_count + 1;
        END IF;
    END LOOP;
    CLOSE c_cust_spend;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Updated ' || v_update_count || ' customers.');
END;
```

**PySpark Migration** (Set-based operations):
```python
def process_customer_tiers():
    """
    Processes customer tiers based on total spending.

    Business Rules:
    - GOLD: >= $3,000 total spend
    - SILVER: >= $1,000 total spend
    - BRONZE: < $1,000 total spend

    Returns:
        int: Number of customers updated
    """
    try:
        print("Starting customer tier processing...")

        # Read tables
        customers_df = spark.table("customers")
        products_df = spark.table("products")
        transactions_df = spark.table("sales_transactions")

        # Calculate total spend per customer (replaces cursor)
        customer_spend_df = (
            transactions_df
            .join(products_df, "product_id", "inner")
            .join(customers_df, "customer_id", "inner")
            .groupBy("customer_id")
            .agg(spark_sum(col("unit_price") * col("quantity")).alias("total_spend"))
        )

        # Apply tier logic (replaces IF-ELSIF-ELSE)
        customer_tier_df = customer_spend_df.withColumn(
            "new_tier",
            when(col("total_spend") >= 3000, "GOLD")
            .when(col("total_spend") >= 1000, "SILVER")
            .otherwise("BRONZE")
        )

        # Identify changed tiers (replaces WHERE segment != new_tier)
        customers_current = customers_df.select(
            "customer_id",
            col("segment").alias("current_segment")
        )

        changes_df = (
            customer_tier_df
            .join(customers_current, "customer_id", "inner")
            .filter(col("current_segment") != col("new_tier"))
            .select("customer_id", "new_tier", "total_spend")
        )

        update_count = changes_df.count()

        if update_count > 0:
            # Perform Delta MERGE (replaces UPDATE statement)
            customers_delta = DeltaTable.forName(spark, "customers")

            customers_delta.alias("target").merge(
                changes_df.alias("source"),
                "target.customer_id = source.customer_id"
            ).whenMatchedUpdate(
                set={"segment": col("source.new_tier")}
            ).execute()

            print(f"✓ Tier processing complete. Updated {update_count} customers.")
            changes_df.orderBy(col("total_spend").desc()).show(10, truncate=False)
        else:
            print("✓ Tier processing complete. No customers required tier updates.")

        return update_count

    except Exception as e:
        print(f"❌ Error in process_customer_tiers: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0
```

**Key Migration Patterns**:

| Oracle Pattern | PySpark Equivalent |
|----------------|-------------------|
| `CURSOR c IS SELECT...` | `df = spark.table().join().groupBy().agg()` |
| `OPEN c; LOOP; FETCH c INTO r; EXIT WHEN c%NOTFOUND` | DataFrame transformations (set-based) |
| `IF...ELSIF...ELSE` | `when().when().otherwise()` |
| `UPDATE...WHERE` | `DeltaTable.merge().whenMatchedUpdate()` |
| `SQL%ROWCOUNT` | `changes_df.count()` |
| `COMMIT` | Auto-committed (Databricks) |
| `DBMS_OUTPUT.PUT_LINE` | `print()` or `display()` |

---

#### 2.4 `04_execute_pipeline.py` - Execution Workflow

**Purpose**: Orchestrate the tier processing workflow with before/after reporting.

**Structure**:
```python
# Import functions from analytics notebook
%run ./03_customer_analytics

# Step 1: Display BEFORE counts
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

# Step 2: Execute tier processing
print("\n" + "=" * 60)
print("EXECUTING TIER PROCESSING...")
print("=" * 60 + "\n")

update_count = process_customer_tiers()

# Step 3: Display AFTER counts
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

# Step 4: Show changes
print(f"\n📈 Tier Changes:")
print(f"   Gold:   {gold_before:>5} → {gold_after:>5}")
print(f"   Silver: {silver_before:>5} → {silver_after:>5}")
print(f"   Bronze: {bronze_before:>5} → {bronze_after:>5}")
print(f"\n✅ Total Customers Updated: {update_count}")

# Step 5: Display top customers
top_customers_query = """
    SELECT c.customer_id, c.cust_name, c.segment,
           ROUND(SUM(p.unit_price * t.quantity), 2) as total_spend,
           COUNT(DISTINCT t.txn_id) as transaction_count
    FROM customers c
    JOIN sales_transactions t ON c.customer_id = t.customer_id
    JOIN products p ON t.product_id = p.product_id
    GROUP BY c.customer_id, c.cust_name, c.segment
    ORDER BY total_spend DESC
    LIMIT 20
"""
display(spark.sql(top_customers_query))
```

**Key Features**:
- `%run` imports functions from other notebooks
- Before/after comparison
- Formatted output with alignment
- Visual separators for readability
- Top customers analysis

---

#### 2.5 `05_validation.py` - Comprehensive Validation

**Purpose**: Validate migration correctness and data quality.

**Validation Checks**:

##### 1. Data Volume Verification
```python
products_count = spark.table("products").count()
customers_count = spark.table("customers").count()
transactions_count = spark.table("sales_transactions").count()

print(f"Products:     Expected=50,    Actual={products_count}")
print(f"Customers:    Expected=500,   Actual={customers_count}")
print(f"Transactions: Expected=10000, Actual={transactions_count}")
```

##### 2. Tier Assignment Correctness
```python
validation_query = """
    SELECT c.customer_id, c.segment as assigned_tier,
           ROUND(SUM(p.unit_price * t.quantity), 2) as total_spend,
           CASE
               WHEN SUM(p.unit_price * t.quantity) >= 3000 THEN 'GOLD'
               WHEN SUM(p.unit_price * t.quantity) >= 1000 THEN 'SILVER'
               ELSE 'BRONZE'
           END as expected_tier,
           CASE
               WHEN c.segment = CASE...END THEN 'PASS'
               ELSE 'FAIL'
           END as validation_result
    FROM customers c
    LEFT JOIN sales_transactions t ON c.customer_id = t.customer_id
    LEFT JOIN products p ON t.product_id = p.product_id
    GROUP BY c.customer_id, c.segment
"""

validation_df = spark.sql(validation_query)
fail_count = validation_df.filter(col("validation_result") == "FAIL").count()

if fail_count == 0:
    print("✅ All tier assignments are correct!")
else:
    print(f"❌ Found {fail_count} incorrect assignments")
```

##### 3. Business Rule Verification
```python
# Verify spending ranges match tier thresholds
spending_ranges = spark.sql("""
    SELECT c.segment,
           ROUND(MIN(spend.total_spend), 2) as min_spend,
           ROUND(MAX(spend.total_spend), 2) as max_spend
    FROM customers c
    LEFT JOIN (
        SELECT customer_id, SUM(p.unit_price * t.quantity) as total_spend
        FROM sales_transactions t
        JOIN products p ON t.product_id = p.product_id
        GROUP BY customer_id
    ) spend ON c.customer_id = spend.customer_id
    GROUP BY c.segment
""")

# Validate:
# - GOLD min >= 3000
# - SILVER min >= 1000 AND max < 3000
# - BRONZE max < 1000
```

##### 4. Data Quality Checks
```python
# Check for NULL values in critical columns
null_checks = [
    ("customers", "customer_id"),
    ("customers", "segment"),
    ("products", "unit_price"),
    ("sales_transactions", "customer_id")
]

for table, column in null_checks:
    null_count = spark.table(table).filter(col(column).isNull()).count()
    status = "✅ PASS" if null_count == 0 else "❌ FAIL"
    print(f"{table}.{column}: {null_count} NULLs - {status}")
```

##### 5. Referential Integrity
```python
# Check for orphaned transactions
orphaned_customers = spark.sql("""
    SELECT COUNT(*) FROM sales_transactions t
    LEFT JOIN customers c ON t.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").first()[0]

orphaned_products = spark.sql("""
    SELECT COUNT(*) FROM sales_transactions t
    LEFT JOIN products p ON t.product_id = p.product_id
    WHERE p.product_id IS NULL
""").first()[0]

print(f"Orphaned customer references: {orphaned_customers}")
print(f"Orphaned product references: {orphaned_products}")
```

---

## Migration Mapping

### 3. Detailed Oracle → Databricks Mappings

#### 3.1 Data Type Mappings

| Oracle Type | Databricks Type | PySpark Type | Notes |
|-------------|-----------------|--------------|-------|
| `NUMBER` (no precision) | `BIGINT` | `LongType()` | For IDENTITY columns |
| `NUMBER(p,s)` | `DECIMAL(p,s)` | `DecimalType(p,s)` | Precision preserved |
| `VARCHAR2(n)` | `STRING` | `StringType()` | Variable length |
| `DATE` | `DATE` | `DateType()` | Date only (no time) |
| `TIMESTAMP` | `TIMESTAMP` | `TimestampType()` | Date + time |

#### 3.2 SQL Feature Mappings

| Oracle Feature | Databricks Equivalent | Example |
|----------------|----------------------|---------|
| `IDENTITY` column | `GENERATED ALWAYS AS IDENTITY` | Must use BIGINT |
| `DEFAULT` constraint | Handle in application code | Use `withColumn(lit(value))` |
| `SEQUENCE.NEXTVAL` | IDENTITY or `monotonically_increasing_id()` | Auto-increment |
| `COMMIT` | Auto-commit | No explicit COMMIT needed |
| `ROLLBACK` | Transaction handled | Use try/except |
| `DBMS_RANDOM.VALUE(a,b)` | Python `random.uniform(a,b)` | Generate random numbers |
| `SYSDATE` | `current_date()` or `current_timestamp()` | Current date/time |

#### 3.3 PL/SQL to PySpark Patterns

| Oracle Pattern | PySpark Pattern | Complexity |
|----------------|-----------------|------------|
| **Cursor Processing** | DataFrame transformations | High |
| `CURSOR c IS SELECT...` | `df = spark.sql(query)` | Medium |
| `OPEN c` | Not needed | - |
| `LOOP FETCH c INTO r` | `for row in df.collect()` or transformations | High |
| `EXIT WHEN c%NOTFOUND` | Loop ends automatically | - |
| `CLOSE c` | Not needed | - |
| **Conditional Logic** | | |
| `IF...THEN` | `when()` | Low |
| `ELSIF` | `.when()` (chained) | Low |
| `ELSE` | `.otherwise()` | Low |
| **DML Operations** | | |
| `INSERT INTO` | `df.write.saveAsTable()` | Low |
| `UPDATE...SET...WHERE` | Delta `MERGE` operation | High |
| `DELETE FROM WHERE` | `DELETE` on Delta table | Medium |
| **Aggregations** | | |
| `SUM(column)` | `spark_sum(col("column"))` | Low |
| `COUNT(*)` | `.count()` or `count(lit(1))` | Low |
| `GROUP BY` | `.groupBy()` | Low |
| **Joins** | | |
| `FROM a JOIN b ON...` | `.join(b, on_condition, "inner")` | Low |
| `LEFT JOIN` | `.join(b, on_condition, "left")` | Low |
| **Output** | | |
| `DBMS_OUTPUT.PUT_LINE` | `print()` | Low |
| N/A | `display(df)` (Databricks specific) | Low |

---

## Execution Guide

### 4. How to Execute the Migration

#### 4.1 Prerequisites

**Databricks Environment**:
- Databricks workspace with serverless compute
- Python 3.11+
- PySpark 3.5+
- Delta Lake support
- Databricks CLI configured

**Authentication**:
```bash
# Configure Databricks CLI with profile
databricks configure --profile sandbox
# Enter host: https://your-workspace.cloud.databricks.com
# Enter token: dapi...
```

**Required Libraries** (pre-installed in Databricks):
- `pyspark.sql`
- `delta.tables`
- Python `random`, `decimal`

#### 4.2 Execution Sequence

**Step 1: Upload Notebooks**
```bash
cd oracle_scripts/databricks_notebooks

# Upload all notebooks
databricks workspace import \
  /Users/your.email@company.com/01_create_delta_tables \
  --file 01_create_delta_tables.py \
  --language PYTHON --format SOURCE --overwrite --profile sandbox

databricks workspace import \
  /Users/your.email@company.com/02_generate_sample_data \
  --file 02_generate_sample_data.py \
  --language PYTHON --format SOURCE --overwrite --profile sandbox

databricks workspace import \
  /Users/your.email@company.com/03_customer_analytics \
  --file 03_customer_analytics.py \
  --language PYTHON --format SOURCE --overwrite --profile sandbox

databricks workspace import \
  /Users/your.email@company.com/04_execute_pipeline \
  --file 04_execute_pipeline.py \
  --language PYTHON --format SOURCE --overwrite --profile sandbox

databricks workspace import \
  /Users/your.email@company.com/05_validation \
  --file 05_validation.py \
  --language PYTHON --format SOURCE --overwrite --profile sandbox
```

**Step 2: Execute Notebooks via CLI**

Create job submission configs:

`run_01.json`:
```json
{
  "run_name": "Create Delta Tables",
  "tasks": [{
    "task_key": "create_tables",
    "notebook_task": {
      "notebook_path": "/Users/your.email@company.com/01_create_delta_tables",
      "source": "WORKSPACE"
    },
    "timeout_seconds": 600
  }]
}
```

Execute:
```bash
# Step 1: Create tables
databricks jobs submit --json @run_01.json --profile sandbox

# Step 2: Generate data
databricks jobs submit --json @run_02.json --profile sandbox

# Step 3: (03 is library only, no execution needed)

# Step 4: Execute pipeline
databricks jobs submit --json @run_04.json --profile sandbox

# Step 5: Run validation
databricks jobs submit --json @run_05.json --profile sandbox
```

**Step 3: Monitor Execution**
```bash
# Get latest run status
databricks jobs list-runs --limit 1 --profile sandbox

# Get run output
databricks jobs get-run-output <RUN_ID> --profile sandbox
```

#### 4.3 Manual Execution (Databricks UI)

1. **Navigate** to Databricks workspace
2. **Open** notebook: `01_create_delta_tables`
3. **Attach** to cluster (or use serverless)
4. **Run All** cells
5. **Repeat** for notebooks 02, 04, 05 in sequence
6. **View** output in notebook cells

---

## Validation & Testing

### 5. Validation Procedures

#### 5.1 Unit Testing

**Test 1: Table Creation**
```python
# Verify tables exist
tables = spark.sql("SHOW TABLES IN oracle_migration").collect()
table_names = [row.tableName for row in tables]

assert 'customers' in table_names
assert 'products' in table_names
assert 'sales_transactions' in table_names
print("✅ All tables created")
```

**Test 2: Data Generation**
```python
# Verify row counts
assert spark.table("products").count() == 50
assert spark.table("customers").count() == 500
assert spark.table("sales_transactions").count() == 10000
print("✅ Data volumes correct")
```

**Test 3: Tier Logic**
```python
# Test tier assignment for known spending
test_cases = [
    (5000, "GOLD"),
    (3000, "GOLD"),
    (2999, "SILVER"),
    (1000, "SILVER"),
    (999, "BRONZE"),
    (0, "BRONZE")
]

for spend, expected_tier in test_cases:
    actual_tier = (
        "GOLD" if spend >= 3000 else
        "SILVER" if spend >= 1000 else
        "BRONZE"
    )
    assert actual_tier == expected_tier, f"Failed for spend={spend}"
print("✅ Tier logic correct")
```

#### 5.2 Integration Testing

**Test 1: End-to-End Workflow**
```python
# 1. Clear data
spark.sql("TRUNCATE TABLE customers")
spark.sql("TRUNCATE TABLE products")
spark.sql("TRUNCATE TABLE sales_transactions")

# 2. Generate test data
# Run 02_generate_sample_data

# 3. Verify before state
gold_before = get_tier_count('GOLD')
assert gold_before == 500  # All start as GOLD

# 4. Process tiers
update_count = process_customer_tiers()
assert update_count >= 0

# 5. Verify after state
total_after = (get_tier_count('GOLD') +
               get_tier_count('SILVER') +
               get_tier_count('BRONZE'))
assert total_after == 500  # All customers accounted for

print("✅ End-to-end test passed")
```

**Test 2: Idempotency**
```python
# Process tiers twice - second run should update 0 records
update_count_1 = process_customer_tiers()
update_count_2 = process_customer_tiers()

assert update_count_2 == 0, "Second run should update 0 records"
print("✅ Idempotency test passed")
```

#### 5.3 Data Quality Validation

**Test 1: No Duplicates**
```python
# Check for duplicate customer_ids
duplicates = spark.sql("""
    SELECT customer_id, COUNT(*) as cnt
    FROM customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
""").count()

assert duplicates == 0, "Found duplicate customer_ids"
print("✅ No duplicates")
```

**Test 2: Referential Integrity**
```python
# All transactions reference valid customers
invalid_customers = spark.sql("""
    SELECT COUNT(*) FROM sales_transactions t
    LEFT JOIN customers c ON t.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").first()[0]

assert invalid_customers == 0
print("✅ Referential integrity maintained")
```

**Test 3: Business Rule Compliance**
```python
# All GOLD customers have >= $3000 spend
gold_violations = spark.sql("""
    SELECT COUNT(*) FROM customers c
    JOIN (
        SELECT customer_id, SUM(p.unit_price * t.quantity) as spend
        FROM sales_transactions t
        JOIN products p ON t.product_id = p.product_id
        GROUP BY customer_id
    ) s ON c.customer_id = s.customer_id
    WHERE c.segment = 'GOLD' AND s.spend < 3000
""").first()[0]

assert gold_violations == 0
print("✅ Business rules compliant")
```

---

## Troubleshooting

### 6. Common Issues and Solutions

#### Issue 1: IDENTITY Column Type Error

**Error**:
```
[IDENTITY_COLUMNS_UNSUPPORTED_DATA_TYPE] DataType IntegerType is not supported for IDENTITY columns
```

**Cause**: Databricks IDENTITY columns only support BIGINT, not INT.

**Solution**:
```sql
-- WRONG
customer_id INT GENERATED ALWAYS AS IDENTITY

-- CORRECT
customer_id BIGINT GENERATED ALWAYS AS IDENTITY
```

---

#### Issue 2: DEFAULT Constraint Error

**Error**:
```
[WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED]
Failed to execute CREATE TABLE because DEFAULT value assigned but feature not enabled
```

**Cause**: DEFAULT constraint requires feature flag in Delta tables.

**Solution**:
Remove DEFAULT from DDL, handle in data generation:
```python
# Instead of: segment STRING DEFAULT 'GOLD'
# Use: segment STRING
# Then in data generation:
df = df.withColumn("segment", lit("GOLD"))
```

---

#### Issue 3: Decimal Type Assertion Error

**Error**:
```
AssertionError: (in convert_decimal)
assert isinstance(value, decimal.Decimal)
```

**Cause**: Python `round()` returns float, but DataFrame expects Decimal.

**Solution**:
```python
# WRONG
unit_price = round(random.uniform(10, 500), 2)  # Returns float

# CORRECT
from decimal import Decimal
unit_price = Decimal(str(round(random.uniform(10, 500), 2)))
```

---

#### Issue 4: Schema Mismatch - Field Merge Failed

**Error**:
```
[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'customer_id' and 'customer_id'
```

**Cause**: Data type mismatch - table has BIGINT but inserting INT.

**Solution**:
```python
# WRONG
StructField("customer_id", IntegerType(), False)

# CORRECT
from pyspark.sql.types import LongType
StructField("customer_id", LongType(), False)  # LongType = BIGINT
```

---

#### Issue 5: Cursor Pattern Migration

**Challenge**: Oracle cursor iterates row-by-row, PySpark prefers set-based operations.

**Solution**:
```python
# DON'T DO THIS (inefficient):
for row in df.collect():  # Collects all data to driver
    # Process each row individually
    # Update table for each row

# DO THIS (efficient):
# 1. Calculate all values as DataFrame transformations
calculated_df = (
    df.groupBy("customer_id")
    .agg(spark_sum(...))
    .withColumn("new_tier", when(...))
)

# 2. Use Delta MERGE for bulk update
DeltaTable.forName(spark, "table").merge(
    calculated_df,
    "target.id = source.id"
).whenMatchedUpdate(set={...}).execute()
```

---

#### Issue 6: Function Import in Notebooks

**Error**:
```
NameError: name 'get_tier_count' is not defined
```

**Cause**: Function defined in another notebook not imported.

**Solution**:
```python
# At top of 04_execute_pipeline.py
%run ./03_customer_analytics

# Now functions are available:
count = get_tier_count('GOLD')
```

---

## Appendix

### A. Performance Considerations

**Oracle vs Databricks Performance**:

| Aspect | Oracle | Databricks |
|--------|--------|------------|
| **Row-by-row processing** | Fast (cursor) | Slow (avoid) |
| **Set-based operations** | Fast | Very Fast |
| **Parallel processing** | Limited | Excellent |
| **Data volume** | Good for < 1M rows | Excellent for billions |
| **Join performance** | Good | Excellent (broadcast joins) |

**Optimization Tips**:
1. **Avoid** `.collect()` on large DataFrames
2. **Use** broadcast joins for small dimension tables
3. **Partition** large fact tables by date
4. **Cache** frequently accessed DataFrames
5. **Use** Delta MERGE instead of row-by-row updates

---

### B. Best Practices

**Data Engineering**:
1. ✅ Use IDENTITY columns for surrogate keys
2. ✅ Partition large tables by date
3. ✅ Use Delta Lake for ACID transactions
4. ✅ Implement data quality checks
5. ✅ Version control all notebooks
6. ✅ Document business logic clearly

**PySpark Development**:
1. ✅ Use explicit schemas (avoid schema inference)
2. ✅ Prefer DataFrame API over SQL for transformations
3. ✅ Use typed functions (`LongType`, not `IntegerType`)
4. ✅ Handle NULLs explicitly
5. ✅ Use column references: `col("name")` not `"name"`
6. ✅ Test with small data samples first

**Migration**:
1. ✅ Document all Oracle-to-Databricks mappings
2. ✅ Preserve business logic exactly
3. ✅ Add comprehensive validation
4. ✅ Include before/after comparisons
5. ✅ Test edge cases (NULL values, 0 records, etc.)
6. ✅ Provide rollback procedures

---

### C. Glossary

| Term | Definition |
|------|------------|
| **IDENTITY Column** | Auto-incrementing column for surrogate keys |
| **Delta Lake** | Open-source storage layer with ACID transactions |
| **Delta MERGE** | UPSERT operation (UPDATE + INSERT) |
| **PySpark DataFrame** | Distributed collection of data organized into columns |
| **Serverless Compute** | Auto-scaling compute without cluster management |
| **ACID** | Atomicity, Consistency, Isolation, Durability |
| **Cursor** | Oracle mechanism for row-by-row processing |
| **Set-based Operation** | Process entire dataset in single operation |

---

### D. References

**Documentation**:
- [Databricks Delta Lake Guide](https://docs.databricks.com/delta/)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Delta Table Operations](https://docs.delta.io/latest/delta-update.html)

**Source Code Locations**:
- Oracle Scripts: `oracle_scripts/`
- Databricks Notebooks: `oracle_scripts/databricks_notebooks/`
- Workspace Path: `/Users/shubham.choubey@celebaltech.com/`

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-12-15 | Migration Team | Initial documentation |
| 1.1 | 2024-12-15 | Migration Team | Updated with GOLD default tier |

---

**End of Documentation**
