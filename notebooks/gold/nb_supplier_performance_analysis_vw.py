# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier Performance Analysis Gold View
# MAGIC
# MAGIC **Notebook Name:** `nb_supplier_performance_analysis_vw`  
# MAGIC **Purpose:** Provide supplier-level performance metrics using the trusted Purchase Order Fulfillment SOT.  
# MAGIC **Target View:** `supplier_performance_analysis_vw`  
# MAGIC **Target Schema:** `sot_gold_views`  
# MAGIC **Source:** `supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw`  
# MAGIC **Grain:** One row per Supplier

# COMMAND ----------

# DBTITLE 1,Create View
spark.sql("""CREATE OR REPLACE VIEW supply_chain.sot_gold_views.supplier_performance_analysis_vw AS
    SELECT
        vendor_id,
        vendor_name,
        COUNT(DISTINCT purchase_order_id) AS total_orders,
        SUM(COALESCE(order_quantity, 0)) AS total_quantity,
        ROUND(
            SUM(COALESCE(unit_cost_usd * order_quantity, 0)),
            2
        ) AS total_procurement_cost_usd,
        ROUND(
            SUM(COALESCE(unit_cost_inr * order_quantity, 0)),
            2
        ) AS total_procurement_cost_inr,
        ROUND(
            AVG(total_lead_time_days),
            1
        ) AS avg_lead_time_days,
        COUNT(CASE
            WHEN delivery_status = 'ON_TIME' THEN 1
        END) AS on_time_deliveries,
        COUNT(CASE
            WHEN delivery_status = 'LATE' THEN 1
        END) AS late_deliveries,
        COUNT(CASE
            WHEN delivery_status IN ('NOT_SHIPPED', 'IN_TRANSIT') THEN 1
        END) AS pending_deliveries,
        ROUND(
            100.0 *
            COUNT(CASE WHEN delivery_status = 'ON_TIME' THEN 1 END)
            /
            NULLIF(
                COUNT(CASE WHEN shipment_id IS NOT NULL THEN 1 END),
                0
            ),
            2
        ) AS on_time_pct
    FROM supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw
    GROUP BY
        vendor_id,
        vendor_name
""")

# COMMAND ----------

# DBTITLE 1,Validation
spark.sql("""
    SELECT
        COUNT(*) AS supplier_count
    FROM supply_chain.sot_gold_views.supplier_performance_analysis_vw
""").display()

# COMMAND ----------

# DBTITLE 1,Sample data
spark.sql("""
    SELECT *
    FROM supply_chain.sot_gold_views.supplier_performance_analysis_vw
    ORDER BY vendor_name
""").display()