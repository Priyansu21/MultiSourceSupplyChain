# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Order Fulfillment SOT Gold View
# MAGIC
# MAGIC **Notebook Name:** `nb_purchase_order_fulfillment_sot_vw`  
# MAGIC **Purpose:** Create the standardized Gold view over the trusted Purchase Order Fulfillment SOT.  
# MAGIC **Target View:** `purchase_order_fulfillment_sot_vw`  
# MAGIC **Target schema:** `sot_gold_views`  
# MAGIC **Source:** supply_chain.sot_silver.purchase_order_fulfillment_sot  
# MAGIC **Grain:** One row per Purchase Order

# COMMAND ----------

# DBTITLE 1,Create view
spark.sql("""
CREATE OR REPLACE VIEW supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw AS
    SELECT
        purchase_order_id,
        vendor_id,
        vendor_name,
        vendor_country,
        purchase_order_date,
        order_quantity,
        unit_cost_usd,
        unit_cost_inr,
        product_type,
        payment_terms,
        manufacturing_order_id,
        production_date,
        materials_consumed_kg,
        production_hours,
        manufacturing_batch_id,
        shipment_id,
        shipment_date,
        actual_delivery_date,
        promised_delivery_date,
        carrier_name,
        logistics_cost_inr,
        usd_to_inr_exchange_rate,
        production_lead_time_days,
        shipping_lead_time_days,
        total_lead_time_days,
        delivery_status,
        dl_creation_date,
        dl_create_by
        
    FROM supply_chain.sot_silver.purchase_order_fulfillment_sot
""")

# COMMAND ----------

# DBTITLE 1,View validate
spark.sql("""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT purchase_order_id) AS distinct_purchase_orders
    FROM supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw
""").display()

# COMMAND ----------

# DBTITLE 1,Data sample
spark.sql("""
    SELECT *
    FROM supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw
    ORDER BY purchase_order_id
""").display()