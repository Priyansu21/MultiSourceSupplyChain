# Databricks notebook source
# MAGIC %md
# MAGIC # On-Time Delivery Analysis Gold View
# MAGIC
# MAGIC **Notebook Name:** `nb_on_time_delivery_analysis_vw`  
# MAGIC **Purpose:** Provide supplier and carrier level delivery performance metrics using the trusted Purchase Order Fulfillment SOT.  
# MAGIC **Target View:** `on_time_delivery_analysis_vw`  
# MAGIC **Target Schema:** `sot_gold_views`  
# MAGIC **Source:** `supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw`  
# MAGIC **Grain:** One row per Supplier, Carrier and Delivery Status combination

# COMMAND ----------

# DBTITLE 1,Create View
spark.sql("""
CREATE OR REPLACE VIEW supply_chain.sot_gold_views.on_time_delivery_analysis_vw AS
    SELECT
        vendor_id,
        vendor_name,
        carrier_name,
        delivery_status,
        COUNT(DISTINCT purchase_order_id) AS shipment_count,
        ROUND(
            AVG(
                DATEDIFF(
                    actual_delivery_date,
                    promised_delivery_date
                )
            ),
            1
        ) AS avg_days_vs_promise,
        MIN(actual_delivery_date) AS earliest_delivery,
        MAX(actual_delivery_date) AS latest_delivery,
        ROUND(
            AVG(total_lead_time_days),
            1
        ) AS avg_total_lead_time_days
    FROM supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw
    WHERE shipment_id IS NOT NULL
    GROUP BY
        vendor_id,
        vendor_name,
        carrier_name,
        delivery_status
""")

# COMMAND ----------

# DBTITLE 1,Validation
spark.sql("""
    SELECT
        COUNT(*) AS analysis_rows
    FROM supply_chain.sot_gold_views.on_time_delivery_analysis_vw
""").display()

# COMMAND ----------

# DBTITLE 1,Sample data
spark.sql("""
    SELECT *
    FROM supply_chain.sot_gold_views.on_time_delivery_analysis_vw
    ORDER BY
        vendor_name,
        carrier_name,
        delivery_status
""").display()