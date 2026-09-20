# Databricks notebook source
# MAGIC %md
# MAGIC # **nb_purchase_order_fulfillment_sot**
# MAGIC
# MAGIC This DDL notebook defines the schema and data types for the `purchase_order_fulfillment_sot` Silver SOT table. The SOT contains integrated Purchase Order Fulfillment data by combining Procurement, Vendor Master, Manufacturing, Logistics, and FX Reference data at one row per Purchase Order.
# MAGIC
# MAGIC **Created by:** Priyansu Panigrahi  
# MAGIC **Created on:** 17-Sep-2026  
# MAGIC **ETL function:** `nb_func_purchase_order_fulfillment_sot`  
# MAGIC **Target table:** `purchase_order_fulfillment_sot`  
# MAGIC **Target schema:** `sot_silver`  
# MAGIC
# MAGIC ### Source Objects
# MAGIC
# MAGIC - `supply_chain.ing_bronze.procurement_orders`
# MAGIC - `supply_chain.ing_bronze.vendor_master`
# MAGIC - `supply_chain.ing_bronze.manufacturing_batches`
# MAGIC - `supply_chain.ing_bronze.logistics_shipments`
# MAGIC - `supply_chain.ing_bronze.currency_rates`

# COMMAND ----------

# DBTITLE 1,Table creation
# ============================================================
# PURCHASE ORDER FULFILLMENT SOT
# Grain: 1 row per purchase order
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS supply_chain.sot_silver.purchase_order_fulfillment_sot
(
    -- Procurement
    purchase_order_id STRING NOT NULL,
    vendor_id STRING,
    vendor_name STRING,
    vendor_country STRING,
    purchase_order_date DATE,
    order_quantity BIGINT,
    unit_cost_usd DOUBLE,
    unit_cost_inr DOUBLE,
    product_type STRING,
    payment_terms STRING,

    -- Manufacturing
    manufacturing_order_id STRING,
    production_date DATE,
    materials_consumed_kg DOUBLE,
    production_hours DOUBLE,
    manufacturing_batch_id STRING,

    -- Logistics
    shipment_id STRING,
    shipment_date DATE,
    actual_delivery_date DATE,
    promised_delivery_date DATE,
    carrier_name STRING,
    logistics_cost_inr DOUBLE,

    -- FX
    usd_to_inr_exchange_rate DOUBLE,

    -- Derived business metrics
    production_lead_time_days INT,
    shipping_lead_time_days INT,
    total_lead_time_days INT,
    delivery_status STRING,

    -- Load audit metadata
    dl_creation_date DATE,
    dl_create_by STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql("""
DESCRIBE TABLE supply_chain.sot_silver.purchase_order_fulfillment_sot
""").display()