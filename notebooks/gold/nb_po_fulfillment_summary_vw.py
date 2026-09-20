# Databricks notebook source
# MAGIC %md
# MAGIC # PO Fulfillment Summary Gold View
# MAGIC
# MAGIC **Notebook Name:** `nb_po_fulfillment_summary_vw`  
# MAGIC **Purpose:** Provide a business-friendly summary of purchase order fulfillment using the trusted Purchase Order Fulfillment SOT.  
# MAGIC **Target View:** `po_fulfillment_summary_vw`  
# MAGIC **Target Schema:** `sot_gold_views`  
# MAGIC **Source:** `supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw`  
# MAGIC **Grain:** One row per Purchase Order

# COMMAND ----------

# DBTITLE 1,Create View
spark.sql("""CREATE OR REPLACE VIEW supply_chain.sot_gold_views.po_fulfillment_summary_vw AS
    SELECT
        purchase_order_id AS `PO ID`,
        vendor_name AS `Supplier`,
        product_type AS `Product`,
        order_quantity AS `Qty`,
        ROUND(unit_cost_usd, 2) AS `Unit Cost USD`,
        ROUND(unit_cost_inr, 2) AS `Unit Cost INR`,
        purchase_order_date AS `Order Date`,
        production_date AS `Production Date`,
        actual_delivery_date AS `Delivery Date`,
        promised_delivery_date AS `Promised Date`,
        delivery_status AS `Status`,
        total_lead_time_days AS `Days to Deliver`

    FROM supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw
""")

# COMMAND ----------

# DBTITLE 1,Validation
spark.sql("""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT `PO ID`) AS distinct_po_count
    FROM supply_chain.sot_gold_views.po_fulfillment_summary_vw
""").display()

# COMMAND ----------

# DBTITLE 1,Data sample - run success
spark.sql("""
    SELECT *
    FROM supply_chain.sot_gold_views.po_fulfillment_summary_vw
    ORDER BY `Order Date` DESC
""").display()