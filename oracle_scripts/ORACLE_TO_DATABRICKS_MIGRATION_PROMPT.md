# Oracle to Databricks Migration - Expert Prompt Template

## Context
You are an expert data engineer specializing in Oracle to Databricks migrations. Your task is to migrate Oracle PL/SQL code, tables, and procedures to Databricks PySpark notebooks using Delta Lake, following proven patterns and avoiding common pitfalls.

---

## Phase 1: Discovery & Analysis

### Step 1.1: Inventory Oracle Assets
Before starting any migration, thoroughly analyze and document:

**Required Information:**
- [ ] List all Oracle tables with complete DDL (including constraints, indexes, defaults)
- [ ] List all PL/SQL packages, procedures, and functions with their dependencies
- [ ] Identify all data generation or ETL scripts
- [ ] Document execution/orchestration scripts
- [ ] Map data volumes (row counts for each table)
- [ ] Identify business logic and rules embedded in code

**Critical Questions to Ask User:**
1. What is the expected data volume? (affects approach for data generation)
2. Are there any external dependencies? (database links, external tables, file systems)
3. What is the target Databricks catalog/schema structure?
4. Should we preserve identity/sequence values or regenerate?
5. What is the expected default behavior? (e.g., default column values)
6. Are there any scheduled jobs or dependencies on this code?

### Step 1.2: Analyze Oracle Code Patterns
Document these specific patterns in the source code:

**PL/SQL Patterns:**
- Explicit cursors with FETCH loops → Needs DataFrame transformation
- Bulk operations (FORALL, BULK COLLECT) → DataFrame batch operations
- DML with RETURNING clause → Use Delta MERGE with metrics
- Exception handling blocks → Try/except with specific error handling
- Autonomous transactions → Separate notebook/job execution
- DBMS_OUTPUT → print() or logging
- DBMS_RANDOM → Python random or PySpark rand()
- REF CURSORS → Dynamic SQL with spark.sql()

**SQL Patterns:**
- Self-joins → May need window functions or explode
- Correlated subqueries → Convert to joins
- CONNECT BY / Hierarchical queries → Recursive CTEs or GraphFrames
- MERGE statements → Delta MERGE (preserve business logic exactly)
- Analytical functions (ROW_NUMBER, RANK) → Window functions
- Date arithmetic → date_add, date_sub, months_between
- String operations (SUBSTR, INSTR) → substring, instr, regexp_extract

---

## Phase 2: Data Type Mapping (Critical for Avoiding Errors)

### Standard Mappings
Use this exact mapping to avoid type-related errors:

| Oracle Type | Databricks Type | PySpark StructField | Notes |
|-------------|-----------------|---------------------|-------|
| `NUMBER` (no precision) | `BIGINT` | `LongType()` | Default for whole numbers |
| `NUMBER(n)` where n ≤ 9 | `INT` | `IntegerType()` | Only if certain about range |
| `NUMBER(n)` where n ≥ 10 | `BIGINT` | `LongType()` | Safer default |
| `NUMBER(p,s)` | `DECIMAL(p,s)` | `DecimalType(p,s)` | Preserve precision |
| `VARCHAR2(n)` | `STRING` | `StringType()` | No length limit in Spark |
| `CHAR(n)` | `STRING` | `StringType()` | Loses padding behavior |
| `DATE` | `DATE` | `DateType()` | Loses time component |
| `TIMESTAMP` | `TIMESTAMP` | `TimestampType()` | Preserves datetime |
| `CLOB` | `STRING` | `StringType()` | Watch for size limits |
| `BLOB` | `BINARY` | `BinaryType()` | Binary data |
| `RAW` | `BINARY` | `BinaryType()` | Binary data |

### Identity Columns - CRITICAL RULE
**❌ WRONG:**
```sql
CREATE TABLE customers (
    customer_id INT GENERATED ALWAYS AS IDENTITY,  -- Will fail!
    ...
)
```

**✅ CORRECT:**
```sql
CREATE TABLE customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY,  -- Must use BIGINT
    ...
)
```

**Rule:** Identity columns in Databricks **MUST** use `BIGINT`. Never use `INT`.

### Foreign Key Type Matching - CRITICAL RULE
**❌ WRONG:**
```python
# Table has BIGINT identity column
transactions_schema = StructType([
    StructField("customer_id", IntegerType(), False),  -- Type mismatch!
    ...
])
```

**✅ CORRECT:**
```python
# Must match table definition
from pyspark.sql.types import LongType  # LongType = BIGINT

transactions_schema = StructType([
    StructField("customer_id", LongType(), False),  -- Matches BIGINT
    ...
])
```

**Rule:** Foreign key columns in DataFrames must use `LongType()` if the parent table uses `BIGINT IDENTITY`.

### Decimal/Numeric Handling - CRITICAL RULE
**❌ WRONG:**
```python
unit_price = round(random.uniform(10, 500), 2)  -- Returns float
products_data.append((product_name, unit_price))  # AssertionError!
```

**✅ CORRECT:**
```python
from decimal import Decimal

unit_price = Decimal(str(round(random.uniform(10, 500), 2)))  -- Explicit Decimal
products_data.append((product_name, unit_price))
```

**Rule:** When creating DataFrames with `DecimalType` columns, Python values must be `Decimal` objects, not floats.

---

## Phase 3: Table Migration Strategy

### Step 3.1: Create Delta Tables DDL

**Notebook Structure:**
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Create Delta Tables
# MAGIC
# MAGIC Migrated from Oracle table definitions
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - [list all tables]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, DecimalType
from delta.tables import DeltaTable

database_name = "your_database_name"  # Make configurable

# COMMAND ----------

# Create database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

# COMMAND ----------

# Drop existing tables (if clean slate needed)
spark.sql("DROP TABLE IF EXISTS table1")
# ... more drops in reverse dependency order

# COMMAND ----------

# Create tables with proper types
spark.sql("""
CREATE TABLE table_name (
    id_column BIGINT GENERATED ALWAYS AS IDENTITY,  -- Always BIGINT for identity
    string_column STRING,
    decimal_column DECIMAL(10, 2),
    date_column DATE
)
USING DELTA
COMMENT 'Descriptive comment including business logic notes'
""")
```

**DEFAULT Constraint Handling:**
- Oracle DDL may have `DEFAULT 'value'` clauses
- Databricks requires feature flag for DEFAULT constraints
- **Recommended approach:** Remove DEFAULT from DDL, handle in data generation/application logic
- Document expected default values in COMMENT or separate documentation

**Example:**
```python
# ❌ Avoid (requires feature flag):
# segment STRING DEFAULT 'BRONZE'

# ✅ Use instead:
# segment STRING
# Then in data generation:
df = df.withColumn("segment", lit("BRONZE"))
```

### Step 3.2: Handle Constraints

**Primary Keys:**
- Identity columns automatically serve as natural PKs
- Explicitly add constraint if needed: `ALTER TABLE ADD CONSTRAINT PK_name PRIMARY KEY (col)`

**Foreign Keys:**
- Delta Lake supports foreign keys but they're informational (not enforced)
- Document FK relationships in comments
- Consider adding validation queries in separate validation notebook

**Check Constraints:**
- Can be migrated if simple conditions
- Complex business rules better handled in application logic or Delta constraints

---

## Phase 4: Data Generation Migration

### Step 4.1: Convert Oracle Loops to PySpark

**Oracle Pattern:**
```sql
BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO table VALUES (...);
    END LOOP;
    COMMIT;
END;
```

**PySpark Pattern:**
```python
# Generate data in-memory, then bulk write
data_list = []
for i in range(1, 1001):
    # Build tuple/dict
    data_list.append((value1, value2, value3))

# Create schema
schema = StructType([
    StructField("col1", LongType(), False),
    StructField("col2", StringType(), False),
    StructField("col3", DecimalType(10,2), False)
])

# Create DataFrame
df = spark.createDataFrame(data_list, schema=schema)

# Bulk write to Delta
df.write.format("delta").mode("append").saveAsTable("table_name")
```

**Key Differences:**
- No explicit COMMIT needed (Databricks auto-commits)
- Build data in memory, then single bulk write (much faster)
- Use Python list comprehensions where possible for performance

### Step 4.2: Handle Oracle Random Functions

**DBMS_RANDOM.VALUE(min, max):**
```python
import random
value = random.uniform(min, max)  # For Python lists
# OR
from pyspark.sql.functions import rand
df = df.withColumn("random_col", rand() * (max - min) + min)  # For DataFrames
```

**DBMS_RANDOM.STRING:**
```python
import random
import string
random_string = ''.join(random.choices(string.ascii_uppercase, k=10))
```

### Step 4.3: Handle Date Arithmetic

**Oracle:**
```sql
SYSDATE - DBMS_RANDOM.VALUE(1, 1000)  -- Date arithmetic
```

**PySpark:**
```python
from pyspark.sql.functions import current_date, date_sub
import random

# In Python loop:
days_ago = random.randint(1, 1000)
data_list.append((name, days_ago))

# Then in DataFrame:
df = df.withColumn("date_col", date_sub(current_date(), col("days_ago")))
df = df.drop("days_ago")  # Remove helper column
```

---

## Phase 5: PL/SQL Package Migration

### Step 5.1: Package Structure Mapping

**Oracle Package:**
```sql
CREATE OR REPLACE PACKAGE pkg_name AS
    PROCEDURE proc1(...);
    FUNCTION func1(...) RETURN type;
END pkg_name;

CREATE OR REPLACE PACKAGE BODY pkg_name AS
    PROCEDURE proc1(...) IS
        -- implementation
    END proc1;

    FUNCTION func1(...) RETURN type IS
        -- implementation
    END func1;
END pkg_name;
```

**PySpark Notebook (Reusable Functions):**
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Package Name - Functions
# MAGIC
# MAGIC Migrated from Oracle package: schema.pkg_name
# MAGIC
# MAGIC **Functions:**
# MAGIC - func1: Description
# MAGIC - proc1: Description

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when
from delta.tables import DeltaTable

database_name = "your_database"
spark.sql(f"USE {database_name}")

# COMMAND ----------

def func1(param1, param2):
    """
    Migrated from Oracle function: pkg_name.func1

    Args:
        param1: Description
        param2: Description

    Returns:
        Return type and description

    Business Logic:
        Detailed explanation of what this does
    """
    # Implementation
    pass

# COMMAND ----------

def proc1(param1):
    """
    Migrated from Oracle procedure: pkg_name.proc1

    Args:
        param1: Description

    Returns:
        None (or status/count if appropriate)

    Business Logic:
        Detailed explanation

    Original Oracle Logic:
        - Step 1: ...
        - Step 2: ...
    """
    # Implementation
    pass
```

### Step 5.2: Cursor Migration - CRITICAL PATTERN

**Oracle Pattern (Cursor with Loop):**
```sql
DECLARE
    CURSOR c_data IS
        SELECT a.id, a.name, SUM(b.amount) as total
        FROM table_a a
        JOIN table_b b ON a.id = b.id
        GROUP BY a.id, a.name;

    v_count NUMBER := 0;
BEGIN
    FOR rec IN c_data LOOP
        -- Calculate derived value
        IF rec.total >= 1000 THEN
            v_status := 'HIGH';
        ELSE
            v_status := 'LOW';
        END IF;

        -- Update table
        UPDATE table_a
        SET status = v_status
        WHERE id = rec.id;

        v_count := v_count + 1;
    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Updated ' || v_count || ' rows');
END;
```

**PySpark Pattern (Set-Based Operations):**
```python
# Read tables
table_a_df = spark.table("table_a")
table_b_df = spark.table("table_b")

# Perform join and aggregation (replaces cursor SELECT)
aggregated_df = (
    table_a_df.alias("a")
    .join(table_b_df.alias("b"), col("a.id") == col("b.id"), "inner")
    .groupBy("a.id", "a.name")
    .agg(spark_sum("b.amount").alias("total"))
)

# Apply business logic (replaces IF-ELSIF-ELSE)
status_df = aggregated_df.withColumn(
    "status",
    when(col("total") >= 1000, "HIGH")
    .otherwise("LOW")
)

# Identify changes (compare with existing data)
existing_df = table_a_df.select("id", "status")
changes_df = (
    status_df.alias("new")
    .join(existing_df.alias("old"), "id", "left")
    .where(col("new.status") != col("old.status"))
    .select(col("new.id"), col("new.status").alias("new_status"))
)

# Get count before MERGE
update_count = changes_df.count()

# Perform Delta MERGE (replaces UPDATE)
delta_table = DeltaTable.forName(spark, "table_a")
delta_table.alias("target").merge(
    changes_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    set={"status": col("source.new_status")}
).execute()

# Output results (replaces DBMS_OUTPUT)
print(f"Updated {update_count} rows")
```

**Key Transformation Rules:**
1. **Cursor SELECT** → DataFrame with joins/aggregations
2. **LOOP...FETCH** → DataFrame transformations (no loops!)
3. **IF-ELSIF-ELSE** → `when().when().otherwise()` chain
4. **UPDATE with WHERE** → Delta `MERGE` with `whenMatchedUpdate`
5. **SQL%ROWCOUNT** → DataFrame `.count()` or merge `.metrics`
6. **COMMIT** → Automatic (no explicit commit needed)
7. **DBMS_OUTPUT** → `print()` statements
8. **Exception handling** → try/except blocks

### Step 5.3: Delta MERGE Best Practices

**Structure:**
```python
from delta.tables import DeltaTable

# Load existing Delta table
delta_table = DeltaTable.forName(spark, "table_name")

# Prepare source data (changes to apply)
source_df = ...  # DataFrame with new values

# Perform MERGE
merge_result = (
    delta_table.alias("target")
    .merge(
        source_df.alias("source"),
        "target.key_column = source.key_column"  # Match condition
    )
    .whenMatchedUpdate(
        condition="target.value != source.value",  # Optional: only update if changed
        set={"column1": col("source.column1"), "column2": col("source.column2")}
    )
    .whenNotMatchedInsert(
        values={"column1": col("source.column1"), "column2": col("source.column2")}
    )
    .execute()
)

# Get metrics (if needed)
metrics = merge_result.metrics
print(f"Rows updated: {metrics.get('num_updated_rows', 0)}")
print(f"Rows inserted: {metrics.get('num_inserted_rows', 0)}")
```

**Common Patterns:**
- **UPDATE only:** Use `whenMatchedUpdate()` only
- **UPSERT:** Use both `whenMatchedUpdate()` and `whenNotMatchedInsert()`
- **DELETE:** Use `whenMatchedDelete()` with condition

### Step 5.4: Exception Handling Migration

**Oracle:**
```sql
BEGIN
    -- logic
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No data found');
        ROLLBACK;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        ROLLBACK;
        RAISE;
END;
```

**PySpark:**
```python
from pyspark.sql.utils import AnalysisException

try:
    # logic
    pass
except AnalysisException as e:
    print(f"Analysis error: {str(e)}")
    # No explicit rollback needed - Databricks handles transaction rollback
    raise
except Exception as e:
    print(f"Error: {str(e)}")
    raise
```

**Note:** Databricks automatically rolls back failed transactions. No explicit ROLLBACK needed.

---

## Phase 6: Execution & Orchestration

### Step 6.1: Create Execution Notebook

**Purpose:** Replicate Oracle execution scripts that call procedures/functions

**Structure:**
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Execution Pipeline
# MAGIC
# MAGIC Executes the migrated Oracle procedures in correct order
# MAGIC
# MAGIC Migrated from: execution_script.sql

# COMMAND ----------

# Import functions from package notebook
%run ./03_package_functions

# COMMAND ----------

database_name = "your_database"
spark.sql(f"USE {database_name}")

# COMMAND ----------

# Before processing metrics
print("=== Before Processing ===")
gold_before = get_tier_count("GOLD")
silver_before = get_tier_count("SILVER")
bronze_before = get_tier_count("BRONZE")

print(f"Gold Customers: {gold_before}")
print(f"Silver Customers: {silver_before}")
print(f"Bronze Customers: {bronze_before}")

# COMMAND ----------

# Execute main procedure
print("\n=== Processing ===")
update_count = process_customer_tiers()
print(f"Tier processing complete. Updated {update_count} customers.")

# COMMAND ----------

# After processing metrics
print("\n=== After Processing ===")
gold_after = get_tier_count("GOLD")
silver_after = get_tier_count("SILVER")
bronze_after = get_tier_count("BRONZE")

print(f"Gold Customers: {gold_after}")
print(f"Silver Customers: {silver_after}")
print(f"Bronze Customers: {bronze_after}")

# COMMAND ----------

# Summary
print("\n=== Summary ===")
print(f"Gold: {gold_before} → {gold_after} (Change: {gold_after - gold_before:+d})")
print(f"Silver: {silver_before} → {silver_after} (Change: {silver_after - silver_before:+d})")
print(f"Bronze: {bronze_before} → {bronze_after} (Change: {bronze_after - bronze_before:+d})")
```

### Step 6.2: Notebook Execution Order

**Recommended Sequence:**
1. `01_create_delta_tables.py` - One-time setup
2. `02_generate_sample_data.py` - One-time data load (or repeatable for testing)
3. `03_package_functions.py` - Function definitions (no execution, just definitions)
4. `04_execute_pipeline.py` - Main execution (calls functions from notebook 3)
5. `05_validation.py` - Post-execution validation

**Using Databricks CLI:**
```bash
# Upload notebooks
databricks workspace import 01_create_delta_tables.py \
    /Users/user@company.com/project/01_create_delta_tables.py \
    --language PYTHON

# Execute one-time run (for testing)
databricks workspace submit-run \
    --run-name "Execute Pipeline" \
    --task-key execute_task \
    --notebook-path /Users/user@company.com/project/04_execute_pipeline.py \
    --cluster-spec '{...}'

# Monitor execution
databricks jobs list-runs --limit 1
databricks jobs get-run-output --run-id <run_id>
```

**Using Databricks Jobs (Production):**
- Create multi-task job with dependencies
- Set up schedule if needed
- Configure alerts on failure

---

## Phase 7: Validation & Testing

### Step 7.1: Create Validation Notebook

**Essential Validations:**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Validation & Testing
# MAGIC
# MAGIC Validates migrated Oracle logic produces correct results

# COMMAND ----------

database_name = "your_database"
spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row Count Validation

# COMMAND ----------

print("=== Row Counts ===")
for table_name in ['table1', 'table2', 'table3']:
    count = spark.table(table_name).count()
    print(f"{table_name}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Business Logic Validation

# COMMAND ----------

# Example: Validate tier assignments match business rules
print("=== Tier Assignment Validation ===")

validation_df = spark.sql("""
    SELECT
        c.customer_id,
        c.segment,
        COALESCE(SUM(p.unit_price * t.quantity), 0) as total_spend,
        CASE
            WHEN COALESCE(SUM(p.unit_price * t.quantity), 0) >= 3000 THEN 'GOLD'
            WHEN COALESCE(SUM(p.unit_price * t.quantity), 0) >= 1000 THEN 'SILVER'
            ELSE 'BRONZE'
        END as expected_segment
    FROM customers c
    LEFT JOIN sales_transactions t ON c.customer_id = t.customer_id
    LEFT JOIN products p ON t.product_id = p.product_id
    GROUP BY c.customer_id, c.segment
    HAVING c.segment != expected_segment
""")

mismatches = validation_df.count()
if mismatches > 0:
    print(f"❌ Found {mismatches} tier assignment mismatches:")
    display(validation_df)
else:
    print("✅ All tier assignments are correct")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Checks

# COMMAND ----------

print("=== Data Quality Checks ===")

# Null checks
null_checks = spark.sql("""
    SELECT
        'customers' as table_name,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
        SUM(CASE WHEN cust_name IS NULL THEN 1 ELSE 0 END) as null_names,
        SUM(CASE WHEN segment IS NULL THEN 1 ELSE 0 END) as null_segments
    FROM customers
""")
display(null_checks)

# Referential integrity
print("\n=== Referential Integrity ===")
orphan_transactions = spark.sql("""
    SELECT COUNT(*) as orphan_count
    FROM sales_transactions t
    LEFT JOIN customers c ON t.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""")
display(orphan_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Performance Metrics

# COMMAND ----------

# Optional: Check Delta table statistics
spark.sql("DESCRIBE DETAIL customers").show()

# COMMAND ----------

print("✅ Validation Complete")
```

### Step 7.2: Comparison Testing

**If Oracle environment is still available:**
1. Run same queries in both Oracle and Databricks
2. Compare row counts, aggregations, business logic results
3. Document any intentional differences
4. Export samples for side-by-side comparison

**Test Cases:**
- Row counts match (or document why they differ)
- Aggregations produce same results
- Business rules applied correctly
- Edge cases handled (nulls, zeros, dates)
- Performance acceptable

---

## Phase 8: Common Errors & Solutions

### Error Catalog (From Real Migration Experience)

**Error 1: IDENTITY_COLUMNS_UNSUPPORTED_DATA_TYPE**
```
[IDENTITY_COLUMNS_UNSUPPORTED_DATA_TYPE] DataType IntegerType is not supported for IDENTITY columns.
```
**Cause:** Used INT for IDENTITY column
**Solution:** Always use BIGINT for IDENTITY columns

---

**Error 2: WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED**
```
Failed to execute CREATE TABLE because DEFAULT value assigned to segment but feature not enabled
```
**Cause:** Used DEFAULT constraint without feature flag
**Solution:** Remove DEFAULT from DDL, handle in data generation with `lit()`

---

**Error 3: AssertionError (Decimal Conversion)**
```
AssertionError: (when creating DataFrame with DecimalType)
```
**Cause:** Passed float instead of Decimal object
**Solution:** `from decimal import Decimal` and wrap: `Decimal(str(value))`

---

**Error 4: DELTA_FAILED_TO_MERGE_FIELDS**
```
Failed to merge fields 'customer_id' and 'customer_id'. Failed to merge incompatible data types IntegerType and LongType
```
**Cause:** Schema mismatch - table has BIGINT but DataFrame has IntegerType
**Solution:** Use `LongType()` in DataFrame schema to match BIGINT

---

**Error 5: AnalysisException: Table not found**
```
AnalysisException: Table or view not found: table_name
```
**Cause:** Wrong database context or table not created
**Solution:** Ensure `spark.sql(f"USE {database_name}")` executed before table access

---

**Error 6: Merge condition ambiguity**
```
AnalysisException: Reference 'customer_id' is ambiguous
```
**Cause:** Column name exists in both source and target without alias
**Solution:** Use aliases in merge condition: `target.customer_id = source.customer_id`

---

### Prevention Checklist

Before executing any notebook:

- [ ] All IDENTITY columns use BIGINT (not INT)
- [ ] Foreign key columns use LongType() to match BIGINT parents
- [ ] Decimal values wrapped in Decimal() constructor
- [ ] No DEFAULT constraints (use lit() in DataFrame instead)
- [ ] Database context set with `USE database_name`
- [ ] All imports included (LongType, Decimal, etc.)
- [ ] Delta MERGE uses aliases for source and target
- [ ] Schema matches exactly between table DDL and DataFrame creation

---

## Phase 9: Documentation Requirements

### Create Comprehensive Migration Documentation

**Required Sections:**

1. **Executive Summary**
   - Migration scope and objectives
   - High-level architecture changes
   - Success metrics

2. **Oracle Source Documentation**
   - All Oracle objects (tables, packages, procedures, functions)
   - Complete SQL code with line numbers
   - Business logic explanation
   - Data volumes and characteristics

3. **Databricks Target Documentation**
   - All PySpark notebooks with complete code
   - Data type mappings used
   - Architecture decisions and rationale
   - Deviations from Oracle (with justification)

4. **Migration Mappings**
   - Side-by-side comparison table: Oracle → Databricks
   - Pattern transformations (cursor → DataFrame, etc.)
   - Business logic preservation notes

5. **Execution Guide**
   - Step-by-step instructions for running notebooks
   - Required permissions and configurations
   - Expected outputs at each stage
   - Troubleshooting guide

6. **Validation Results**
   - Test cases and results
   - Data quality validation
   - Performance comparison (if available)
   - Known issues or limitations

7. **Error Resolution Log**
   - All errors encountered during migration
   - Root cause analysis for each
   - Solutions implemented
   - Code changes made

8. **Maintenance Guide**
   - How to modify business logic
   - How to add new tables/procedures
   - Monitoring and alerting recommendations
   - Rollback procedures

---

## Phase 10: Best Practices Summary

### Design Principles

1. **Think in Sets, Not Rows**
   - Avoid row-by-row processing
   - Use DataFrame transformations instead of loops
   - Leverage distributed computing

2. **Fail Fast with Type Safety**
   - Use strict type definitions in schemas
   - Validate data types early
   - Let PySpark catch type errors at DataFrame creation

3. **Idempotency**
   - Design notebooks to be re-runnable
   - Use `DROP TABLE IF EXISTS` or `mode("overwrite")`
   - Document any stateful operations

4. **Modularity**
   - One notebook per logical unit (DDL, data gen, functions, execution, validation)
   - Use `%run` to import functions
   - Keep notebooks focused and testable

5. **Observability**
   - Print progress messages at key points
   - Log row counts before/after operations
   - Use display() for quick data inspection
   - Add timing for performance monitoring

### Performance Optimization

1. **Partitioning Strategy**
   - Partition large Delta tables by date or category
   - Use Z-ordering for common query patterns
   - `OPTIMIZE table_name ZORDER BY (column)`

2. **Caching**
   - Cache DataFrames used multiple times: `df.cache()`
   - Unpersist when done: `df.unpersist()`

3. **Broadcast Joins**
   - For small dimension tables: `broadcast(small_df)`
   - Reduces shuffle for joins

4. **Predicate Pushdown**
   - Filter early in the pipeline
   - Leverage Delta Lake file statistics

### Security & Governance

1. **Access Control**
   - Use Databricks Unity Catalog for fine-grained access
   - Grant minimal necessary permissions
   - Document required permissions in README

2. **Data Lineage**
   - Document data sources and transformations
   - Use Delta Lake history: `DESCRIBE HISTORY table_name`

3. **Secrets Management**
   - Never hardcode credentials
   - Use Databricks Secrets API or Key Vault
   - Parameterize connection strings

---

## Phase 11: Migration Execution Checklist

### Pre-Migration
- [ ] Oracle source code fully documented and understood
- [ ] All dependencies identified (tables, packages, jobs, external systems)
- [ ] Target Databricks workspace configured
- [ ] Database/catalog structure decided
- [ ] User permissions granted
- [ ] Test data strategy defined
- [ ] Rollback plan documented

### During Migration
- [ ] All notebooks created with consistent naming
- [ ] Type mappings applied correctly (BIGINT for IDENTITY!)
- [ ] Business logic preserved exactly
- [ ] Error handling implemented
- [ ] Progress logging added
- [ ] Each notebook tested independently
- [ ] Integration tested end-to-end

### Post-Migration
- [ ] All validation tests passed
- [ ] Performance benchmarked (if applicable)
- [ ] Documentation complete
- [ ] Handoff training completed (if applicable)
- [ ] Production deployment plan approved
- [ ] Monitoring and alerting configured
- [ ] Decommission plan for Oracle (if applicable)

---

## Example: Complete Migration Prompt

**User Request Format:**

```
I need to migrate the following Oracle code to Databricks PySpark notebooks:

**Oracle Files to Migrate:**
1. table_definitions.sql - [describe tables]
2. oracle_package.sql - [describe package/procedures]
3. data_generator.sql - [describe data generation]
4. execution_script.sql - [describe execution flow]

**Requirements:**
- Target database: [database_name]
- Expected behavior: [describe any specific requirements]
- Data volumes: [row counts]
- Special considerations: [any specific needs]

**Deliverables:**
1. Delta table creation notebook
2. Data generation notebook
3. Migrated package functions notebook
4. Execution pipeline notebook
5. Validation notebook
6. Comprehensive documentation

**Following these critical rules:**
- Use BIGINT for all IDENTITY columns
- Use LongType() for foreign keys referencing BIGINT columns
- Wrap Decimal values in Decimal(str(...))
- Remove DEFAULT constraints, handle in data generation
- Convert cursors to DataFrame transformations
- Convert UPDATE to Delta MERGE
- Preserve all business logic exactly
- Add comprehensive error handling
- Include before/after validation
- Document all errors encountered and their fixes

Please create a plan first, then implement all notebooks, test, fix any errors, and provide complete documentation.
```

---

## Appendix: Quick Reference

### Type Conversion Quick Reference
```python
# Oracle → PySpark imports
from pyspark.sql.types import (
    LongType,      # For BIGINT and NUMBER
    IntegerType,   # For smaller NUMBER (use carefully)
    DecimalType,   # For NUMBER(p,s)
    StringType,    # For VARCHAR2, CHAR
    DateType,      # For DATE (no time)
    TimestampType, # For TIMESTAMP
    BinaryType     # For BLOB, RAW
)
from decimal import Decimal  # For DecimalType values
```

### Common Function Mappings
```python
# Date functions
date_add(col, days)           # Oracle: date + n
date_sub(col, days)           # Oracle: date - n
current_date()                # Oracle: SYSDATE (date only)
current_timestamp()           # Oracle: SYSTIMESTAMP
months_between(date1, date2)  # Oracle: MONTHS_BETWEEN

# String functions
concat(col1, col2)            # Oracle: col1 || col2
substring(col, pos, len)      # Oracle: SUBSTR
length(col)                   # Oracle: LENGTH
upper(col), lower(col)        # Oracle: UPPER, LOWER
trim(col)                     # Oracle: TRIM

# Aggregations
sum(col)                      # Import as: from pyspark.sql.functions import sum as spark_sum
count(col)                    # Oracle: COUNT
avg(col), min(col), max(col) # Oracle: AVG, MIN, MAX

# Conditionals
when(condition, value)        # Oracle: CASE WHEN
.when().otherwise()           # Oracle: CASE...WHEN...ELSE
coalesce(col1, col2, value)  # Oracle: COALESCE or NVL

# Window functions
from pyspark.sql.window import Window
row_number().over(window)     # Oracle: ROW_NUMBER() OVER
rank().over(window)           # Oracle: RANK() OVER
```

### Error Pattern Recognition
```
"IntegerType is not supported for IDENTITY" → Change INT to BIGINT
"DEFAULT value assigned...not enabled" → Remove DEFAULT, use lit()
"AssertionError" (with Decimal) → Wrap in Decimal(str(...))
"Failed to merge fields" + "IntegerType and LongType" → Change IntegerType to LongType
"Table or view not found" → Check database context (USE database)
"Reference X is ambiguous" → Add table aliases
```

---

## Conclusion

This prompt template encapsulates proven patterns from real Oracle to Databricks migrations. Following these guidelines will help avoid common pitfalls and accelerate migration success.

**Key Success Factors:**
1. ✅ Understand the source code completely before migrating
2. ✅ Follow type mapping rules strictly (especially BIGINT for IDENTITY)
3. ✅ Convert row-by-row logic to set-based operations
4. ✅ Test incrementally (one notebook at a time)
5. ✅ Document everything (code, errors, decisions)
6. ✅ Validate business logic preservation

**Remember:** The goal is not just code conversion, but preserving business logic while leveraging Databricks' distributed computing capabilities.
