# Databricks notebook source
# MAGIC %md
# MAGIC # **nb_func_fx_exchange_rates_ref**
# MAGIC
# MAGIC This ETL notebook loads and conforms Currency Rate data from the Bronze layer into the Silver `fx_exchange_rates_ref` reference table. The process prepares the source data using a temporary view and loads the conformed records into the Silver reference table using incremental MERGE processing.
# MAGIC
# MAGIC **Created by:** Priyansu Panigrahi  
# MAGIC **Created on:** 18-Sep-2026  
# MAGIC **ETL function:** `nb_func_fx_exchange_rates_ref`  
# MAGIC **Target table:** `fx_exchange_rates_ref`  
# MAGIC **Target schema:** `sot_silver`  
# MAGIC
# MAGIC ### Source Object
# MAGIC
# MAGIC - `supply_chain.ing_bronze.currency_rates`
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC - One row per `(rate_date, from_currency, to_currency)`

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Parameterization
# Add utils/create parameter table to hold parameters
prj_catalog = "supply_chain"
ing_schema = "ing_bronze"
sot_schema = "sot_silver"
target_table = "fx_exchange_rates_ref"

SOT_table = f"{prj_catalog}.{sot_schema}.{target_table}"

print(f"Target : {SOT_table}")

# COMMAND ----------

# DBTITLE 1,Create temp view
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW tmp_fx_exchange_rates AS

SELECT
    rate_date,
    TRIM(from_currency) AS from_currency,
    TRIM(to_currency) AS to_currency,
    exchange_rate

FROM {prj_catalog}.{ing_schema}.currency_rates
""")

# COMMAND ----------

# DBTITLE 1,Validate temp view
query = spark.sql("""
    SELECT
        COUNT(*) AS row_count
    FROM tmp_fx_exchange_rates
""").collect()[0]["row_count"]

print(f"Temporary view row count: {query}")

# COMMAND ----------

# DBTITLE 1,Validate grain
grain_check = spark.sql("""
    SELECT
        rate_date,
        from_currency,
        to_currency
    FROM tmp_fx_exchange_rates
    GROUP BY
        rate_date,
        from_currency,
        to_currency
    HAVING COUNT(*) > 1
""").count()

if grain_check > 0:
    raise ValueError(
        f"FX Reference grain validation failed: "
        f"{grain_check} duplicate logical keys found."
    )

print("Temporary view grain validation passed.")

# COMMAND ----------

# DBTITLE 1,MERGE into Silver
spark.sql(f"""
MERGE INTO {prj_catalog}.{sot_schema}.fx_exchange_rates_ref AS target

USING tmp_fx_exchange_rates AS source

ON  target.rate_date = source.rate_date
AND target.from_currency = source.from_currency
AND target.to_currency = source.to_currency

WHEN MATCHED THEN
    UPDATE SET
        target.exchange_rate = source.exchange_rate

WHEN NOT MATCHED THEN
    INSERT
    (
        rate_date,
        from_currency,
        to_currency,
        exchange_rate
    )
    VALUES
    (
        source.rate_date,
        source.from_currency,
        source.to_currency,
        source.exchange_rate
    )
""")

print("FX Exchange Rates MERGE completed successfully.")

# COMMAND ----------

# DBTITLE 1,Target table validation
count_check = spark.sql(f"""
    SELECT
        COUNT(*) AS row_count
    FROM {prj_catalog}.{sot_schema}.fx_exchange_rates_ref
""").collect()[0]["row_count"]

duplicate_target_count = spark.sql(f"""
    SELECT
        rate_date,
        from_currency,
        to_currency
    FROM {prj_catalog}.{sot_schema}.fx_exchange_rates_ref
    GROUP BY
        rate_date,
        from_currency,
        to_currency
    HAVING COUNT(*) > 1
""").count()

if duplicate_target_count > 0:
    raise ValueError(
        f"Silver target grain validation failed: "
        f"{duplicate_target_count} duplicate logical keys found."
    )

print(f"Silver target row count: {count_check}")
print("Silver target grain validation passed.")
print("FX Exchange Rates Silver ETL completed successfully.")