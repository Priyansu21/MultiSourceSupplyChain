# Databricks notebook source
# MAGIC %md
# MAGIC # Cost Impact Analysis Gold View
# MAGIC
# MAGIC **Notebook Name:** `nb_cost_impact_analysis_vw`  
# MAGIC **Purpose:** Provide supplier and product level procurement, logistics and landed cost analysis using the trusted Purchase Order Fulfillment SOT.  
# MAGIC **Target View:** `cost_impact_analysis_vw`  
# MAGIC **Target Schema:** `sot_gold_views`  
# MAGIC **Source:** `supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw`  
# MAGIC **Grain:** One row per Supplier and Product

# COMMAND ----------

# DBTITLE 1,Create View
spark.sql("""CREATE OR REPLACE VIEW supply_chain.sot_gold_views.cost_impact_analysis_vw AS
    SELECT
        vendor_id,
        vendor_name,
        product_type,
        COUNT(DISTINCT purchase_order_id) AS order_count,
        ROUND(
            AVG(unit_cost_usd),
            2
        ) AS avg_unit_cost_usd,
        ROUND(
            AVG(unit_cost_inr),
            2
        ) AS avg_unit_cost_inr,
        ROUND(
            SUM(COALESCE(unit_cost_usd * order_quantity, 0)),
            2
        ) AS total_procurement_cost_usd,
        ROUND(
            SUM(COALESCE(unit_cost_inr * order_quantity, 0)),
            2
        ) AS total_procurement_cost_inr,
        ROUND(
            SUM(COALESCE(logistics_cost_inr, 0)),
            2
        ) AS total_logistics_cost_inr,
        ROUND(
            SUM(COALESCE(unit_cost_inr * order_quantity, 0))
            +
            SUM(COALESCE(logistics_cost_inr, 0)),
            2
        ) AS total_landed_cost_inr
    FROM supply_chain.sot_gold_views.purchase_order_fulfillment_sot_vw
    GROUP BY
        vendor_id,
        vendor_name,
        product_type
""")

# COMMAND ----------

# DBTITLE 1,Validation
spark.sql("""
    SELECT
        COUNT(*) AS analysis_rows
    FROM supply_chain.sot_gold_views.cost_impact_analysis_vw
""").display()

# COMMAND ----------

# DBTITLE 1,Sample data
spark.sql("""
    SELECT *
    FROM supply_chain.sot_gold_views.cost_impact_analysis_vw
    ORDER BY total_landed_cost_inr DESC
""").display()