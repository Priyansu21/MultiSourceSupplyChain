# Databricks notebook source
# MAGIC %md
# MAGIC # **nb_func_vendor_master_dim**
# MAGIC
# MAGIC This ETL notebook loads and conforms Vendor Master data from the Bronze layer into the Silver `vendor_master_dim` table. The process prepares the source data using a temporary view and loads the conformed records into the Silver dimension using incremental MERGE processing.
# MAGIC
# MAGIC **Created by:** Priyansu Panigrahi  
# MAGIC **Created on:** 18-Sep-2026  
# MAGIC **ETL function:** `nb_func_vendor_master_dim`  
# MAGIC **Target table:** `vendor_master_dim`  
# MAGIC **Target schema:** `sot_silver`  
# MAGIC
# MAGIC ### Source Object
# MAGIC
# MAGIC - `supply_chain.ing_bronze.vendor_master`
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC - One row per `vendor_id`

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Parameterization
# Add utils/create parameter table to hold parameters
prj_catalog = "supply_chain"
ing_schema = "ing_bronze"
sot_schema = "sot_silver"
target_table = "vendor_master_dim"

SOT_table = f"{prj_catalog}.{sot_schema}.{target_table}"
print(f"Target : {SOT_table}")

# COMMAND ----------

# DBTITLE 1,Create temp view
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW tmp_vendor_master AS

SELECT
    TRIM(vendor_id) AS vendor_id,
    TRIM(vendor_name) AS vendor_name,
    TRIM(country) AS country,
    TRIM(vendor_category) AS vendor_category,
    UPPER(TRIM(vendor_status)) AS vendor_status

FROM {prj_catalog}.{ing_schema}.vendor_master
""")

# COMMAND ----------

# DBTITLE 1,Validate temp view
query = spark.sql(f"""
        SELECT 
          COUNT(*) AS row_count
        FROM tmp_vendor_master
        """).collect()[0]["row_count"]

print(f"Temporary view row count: {query}")

# COMMAND ----------

# DBTITLE 1,Validate grain
grain_check = spark.sql(f"""
        SELECT 
            vendor_id
        FROM tmp_vendor_master
        GROUP BY vendor_id
        HAVING COUNT(*) > 1
        """).count()

if grain_check > 0:
    raise ValueError(
        f"Vendor Master grain validation failed: "
        f"{grain_check} duplicate vendor_id values found."
    )

print("Temporary view grain validation passed.")

# COMMAND ----------

# DBTITLE 1,MERGE into Silver
spark.sql(f"""
MERGE INTO {prj_catalog}.{sot_schema}.vendor_master_dim AS target

USING tmp_vendor_master AS source
    ON target.vendor_id = source.vendor_id

WHEN MATCHED THEN
    UPDATE SET
        target.vendor_name = source.vendor_name,
        target.country = source.country,
        target.vendor_category = source.vendor_category,
        target.vendor_status = source.vendor_status

WHEN NOT MATCHED THEN
    INSERT
    (
        vendor_id,
        vendor_name,
        country,
        vendor_category,
        vendor_status
    )
    VALUES
    (
        source.vendor_id,
        source.vendor_name,
        source.country,
        source.vendor_category,
        source.vendor_status
    )
""")

print("Vendor Master MERGE completed successfully.")

# COMMAND ----------

# DBTITLE 1,Target table validation
count_check = spark.sql(f"""
    SELECT 
        COUNT(*) AS row_count
    FROM {prj_catalog}.{sot_schema}.vendor_master_dim 
    """).collect()[0]["row_count"]

duplicate_target_count = spark.sql(f"""
    SELECT 
        vendor_id
    FROM {prj_catalog}.{sot_schema}.vendor_master_dim 
    GROUP BY vendor_id
    HAVING COUNT(*) > 1
    """).count()

if duplicate_target_count > 0:
    raise ValueError(
        f"Silver target grain validation failed: "
        f"{duplicate_target_count} duplicate vendor_id values found."
    )

print(f"Silver target row count: {count_check}")
print("Silver target grain validation passed.")
print("Vendor Master Silver ETL completed successfully.")