# Databricks notebook source
# MAGIC %md
# MAGIC # **nb_func_purchase_order_fulfillment_sot**
# MAGIC
# MAGIC This ETL notebook integrates Procurement, Vendor Master, Manufacturing, Logistics, and FX Reference data into the Silver `purchase_order_fulfillment_sot` Single Source of Truth. The process conforms source-specific structures, validates the SOT business grain, applies business data-quality rules, routes failed records to quarantine, and performs incremental MERGE processing into the trusted SOT.
# MAGIC
# MAGIC **Created by:** Priyansu Panigrahi  
# MAGIC **Created on:** 18-Sep-2026  
# MAGIC **ETL function:** `nb_func_purchase_order_fulfillment_sot`  
# MAGIC **Target table:** `purchase_order_fulfillment_sot`  
# MAGIC **Target schema:** `sot_silver`  
# MAGIC
# MAGIC ### Source Objects
# MAGIC
# MAGIC - `supply_chain.ing_bronze.procurement_orders`
# MAGIC - `supply_chain.ing_bronze.manufacturing_batches`
# MAGIC - `supply_chain.ing_bronze.logistics_shipments`
# MAGIC - `supply_chain.sot_silver.vendor_master_dim`
# MAGIC - `supply_chain.sot_silver.fx_exchange_rates_ref`
# MAGIC
# MAGIC ### Quarantine Object
# MAGIC
# MAGIC - `supply_chain.sot_silver.quarantine_supply_chain`
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC - One row per `purchase_order_id`

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Parameterization
# Add utils/create parameter table to hold parameters
prj_catalog = "supply_chain"
ing_schema = "ing_bronze"
sot_schema = "sot_silver"

# This functions produces 2 tables - business use and data quarantine
target_table = "purchase_order_fulfillment_sot"
quarantine_table = "quarantine_supply_chain"

# COMMAND ----------

# DBTITLE 1,SOT Temporary View
spark.sql(f"""CREATE OR REPLACE TEMP VIEW tmp_purchase_order_fulfillment AS
    SELECT
        -- Procurement
        p.order_id AS purchase_order_id,
        p.vendor_id,
        v.vendor_name AS vendor_name,
        v.country AS vendor_country,
        p.order_date AS purchase_order_date,
        p.quantity AS order_quantity,
        p.cost_usd AS unit_cost_usd,
        p.product_type,
        p.payment_terms,

        -- Manufacturing
        m.mfg_order_id AS manufacturing_order_id,
        m.production_date,
        m.materials_used_kg AS materials_consumed_kg,
        m.production_hours,
        m.batch_id AS manufacturing_batch_id,

        -- Logistics
        l.shipment_id,
        l.ship_date AS shipment_date,
        l.delivery_date AS actual_delivery_date,
        l.promised_delivery_date,
        l.carrier AS carrier_name,
        l.cost_inr AS logistics_cost_inr,

        -- FX column to check exchange rate
        c.exchange_rate AS usd_to_inr_exchange_rate,
        CASE
            WHEN m.production_date IS NOT NULL
            THEN DATEDIFF(m.production_date, p.order_date)
        END AS production_lead_time_days,

        CASE
            WHEN l.ship_date IS NOT NULL
                AND m.production_date IS NOT NULL
            THEN DATEDIFF(l.ship_date, m.production_date)
        END AS shipping_lead_time_days,

        CASE
            WHEN l.delivery_date IS NOT NULL
            THEN DATEDIFF(l.delivery_date, p.order_date)
        END AS total_lead_time_days,

        CASE
            WHEN l.shipment_id IS NULL THEN 'NOT_SHIPPED'
            WHEN l.delivery_date IS NULL THEN 'IN_TRANSIT'
            WHEN l.delivery_date <= l.promised_delivery_date THEN 'ON_TIME'
            ELSE 'LATE'
        END AS delivery_status,

        CASE
            WHEN c.exchange_rate IS NOT NULL
            THEN p.cost_usd * c.exchange_rate
        END AS unit_cost_inr

    FROM {prj_catalog}.{ing_schema}.procurement_orders p
    LEFT JOIN {prj_catalog}.{sot_schema}.vendor_master_dim v
        ON p.vendor_id = v.vendor_id
    LEFT JOIN {prj_catalog}.{ing_schema}.manufacturing_batches m
        ON p.order_id = m.order_ref
    LEFT JOIN {prj_catalog}.{ing_schema}.logistics_shipments l
        ON p.order_id = l.po_number
    LEFT JOIN {prj_catalog}.{sot_schema}.fx_exchange_rates_ref c
        ON p.order_date = c.rate_date
        AND c.from_currency = 'USD'
        AND c.to_currency = 'INR'
""")

# COMMAND ----------

# DBTITLE 1,Validate SOT Grain
grain_check = spark.sql("""
    SELECT
        purchase_order_id
    FROM tmp_purchase_order_fulfillment
    GROUP BY purchase_order_id
    HAVING COUNT(*) > 1
""").count()

if grain_check > 0:
    raise ValueError(
        f"Integrated SOT grain validation failed: "
        f"{grain_check} Purchase Orders produced multiple rows."
    )

print("Integrated SOT grain validation passed.")

# COMMAND ----------

# DBTITLE 1,Quality Gates
spark.sql("""CREATE OR REPLACE TEMP VIEW tmp_purchase_order_fulfillment_quality AS
    SELECT *,  
        CASE
            WHEN purchase_order_id IS NULL
            -- OR order_quantity <= 0
            -- OR unit_cost_usd <= 0
            -- OR usd_to_inr_exchange_rate <= 0
            OR order_quantity IS NULL -- is added to check for NULL values in the columns
            OR order_quantity <= 0 
            OR unit_cost_usd IS NULL  -- is added to check for NULL values in the columns
            OR unit_cost_usd <= 0
            OR usd_to_inr_exchange_rate IS NULL  -- is added to check for NULL values in the columns
            OR usd_to_inr_exchange_rate <= 0
            OR production_date < purchase_order_date
            OR shipment_date < production_date
            OR actual_delivery_date < shipment_date
            OR promised_delivery_date < purchase_order_date
            OR vendor_id IS NULL
            OR vendor_name IS NULL
            THEN 'FAIL'
            ELSE 'PASS'
        END AS _quality_flag,
        CONCAT_WS(
            '; ',
            CASE
                WHEN purchase_order_id IS NULL
                THEN 'NULL_PURCHASE_ORDER_ID'
            END,
            -- CASE
            --     WHEN order_quantity <= 0
            --     THEN 'INVALID_ORDER_QUANTITY'
            -- END,
            -- CASE
            --     WHEN unit_cost_usd <= 0
            --     THEN 'INVALID_UNIT_COST_USD'
            -- END,
            -- CASE
            --     WHEN usd_to_inr_exchange_rate <= 0
            --     THEN 'INVALID_EXCHANGE_RATE'
            -- END,
            CASE
                WHEN order_quantity IS NULL OR order_quantity <= 0
                THEN 'INVALID_ORDER_QUANTITY'
            END,

            CASE
                WHEN unit_cost_usd IS NULL OR unit_cost_usd <= 0
                THEN 'INVALID_UNIT_COST_USD'
            END,

            CASE
                WHEN usd_to_inr_exchange_rate IS NULL
                THEN 'MISSING_EXCHANGE_RATE'

                WHEN usd_to_inr_exchange_rate <= 0
                THEN 'INVALID_EXCHANGE_RATE'
            END,
            CASE
                WHEN production_date < purchase_order_date
                THEN 'PRODUCTION_DATE_BEFORE_ORDER_DATE'
            END,
            CASE
                WHEN shipment_date < production_date
                THEN 'SHIPMENT_DATE_BEFORE_PRODUCTION_DATE'
            END,
            CASE
                WHEN actual_delivery_date < shipment_date
                THEN 'DELIVERY_DATE_BEFORE_SHIPMENT_DATE'
            END,
            CASE
                WHEN promised_delivery_date < purchase_order_date
                THEN 'PROMISED_DELIVERY_BEFORE_ORDER_DATE'
            END,
            CASE
                WHEN vendor_id IS NULL
                THEN 'NULL_VENDOR_ID'
            END,
            CASE
                WHEN vendor_name IS NULL
                THEN 'VENDOR_NOT_FOUND'
            END
        ) AS _quality_reasons

    FROM tmp_purchase_order_fulfillment
""")

# COMMAND ----------

# DBTITLE 1,Accepted - Failed - Reconciliation
spark.sql("""
    CREATE OR REPLACE TEMP VIEW tmp_purchase_order_fulfillment_accepted AS
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
            delivery_status

        FROM tmp_purchase_order_fulfillment_quality
        WHERE _quality_flag = 'PASS'
    """)

spark.sql("""
    CREATE OR REPLACE TEMP VIEW tmp_purchase_order_fulfillment_failed AS
        SELECT * FROM tmp_purchase_order_fulfillment_quality
        WHERE _quality_flag = 'FAIL'
    """)

accepted_count = spark.sql("""
        SELECT 
            COUNT(*) AS row_count
        FROM tmp_purchase_order_fulfillment_accepted
    """).collect()[0]["row_count"]

failed_count = spark.sql("""
        SELECT 
            COUNT(*) AS row_count
        FROM tmp_purchase_order_fulfillment_failed
    """).collect()[0]["row_count"]

total_count = spark.sql("""
        SELECT 
            COUNT(*) AS row_count
        FROM tmp_purchase_order_fulfillment_quality
    """).collect()[0]["row_count"]

if accepted_count + failed_count != total_count:
    raise ValueError(
        "Quality reconciliation failed: "
        "accepted + failed records do not equal integrated records."
    )

print(f"Integrated records : {total_count}")
print(f"Accepted records   : {accepted_count}")
print(f"Failed records     : {failed_count}")
print("Quality reconciliation passed.")

# COMMAND ----------

# DBTITLE 1,MERGE Accepted → SOT
spark.sql(f"""MERGE INTO {prj_catalog}.{sot_schema}.purchase_order_fulfillment_sot AS target
    USING tmp_purchase_order_fulfillment_accepted AS source
        ON target.purchase_order_id = source.purchase_order_id

    WHEN MATCHED THEN
        UPDATE SET
            target.vendor_id = source.vendor_id,
            target.vendor_name = source.vendor_name,
            target.vendor_country = source.vendor_country,
            target.purchase_order_date = source.purchase_order_date,
            target.order_quantity = source.order_quantity,
            target.unit_cost_usd = source.unit_cost_usd,
            target.unit_cost_inr = source.unit_cost_inr,
            target.product_type = source.product_type,
            target.payment_terms = source.payment_terms,
            target.manufacturing_order_id = source.manufacturing_order_id,
            target.production_date = source.production_date,
            target.materials_consumed_kg = source.materials_consumed_kg,
            target.production_hours = source.production_hours,
            target.manufacturing_batch_id = source.manufacturing_batch_id,
            target.shipment_id = source.shipment_id,
            target.shipment_date = source.shipment_date,
            target.actual_delivery_date = source.actual_delivery_date,
            target.promised_delivery_date = source.promised_delivery_date,
            target.carrier_name = source.carrier_name,
            target.logistics_cost_inr = source.logistics_cost_inr,
            target.usd_to_inr_exchange_rate = source.usd_to_inr_exchange_rate,
            target.production_lead_time_days = source.production_lead_time_days,
            target.shipping_lead_time_days = source.shipping_lead_time_days,
            target.total_lead_time_days = source.total_lead_time_days,
            target.delivery_status = source.delivery_status

    WHEN NOT MATCHED THEN
        INSERT
        (
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
        )
        VALUES
        (
            source.purchase_order_id,
            source.vendor_id,
            source.vendor_name,
            source.vendor_country,
            source.purchase_order_date,
            source.order_quantity,
            source.unit_cost_usd,
            source.unit_cost_inr,
            source.product_type,
            source.payment_terms,
            source.manufacturing_order_id,
            source.production_date,
            source.materials_consumed_kg,
            source.production_hours,
            source.manufacturing_batch_id,
            source.shipment_id,
            source.shipment_date,
            source.actual_delivery_date,
            source.promised_delivery_date,
            source.carrier_name,
            source.logistics_cost_inr,
            source.usd_to_inr_exchange_rate,
            source.production_lead_time_days,
            source.shipping_lead_time_days,
            source.total_lead_time_days,
            source.delivery_status,
            current_date(),
            current_user()
        )
    """)

print("Purchase Order Fulfillment SOT MERGE completed successfully.")

# COMMAND ----------

# DBTITLE 1,MERGE Failed → Quarantine
spark.sql(f"""MERGE INTO {prj_catalog}.{sot_schema}.quarantine_supply_chain AS target
    USING tmp_purchase_order_fulfillment_failed AS source
        ON  target.business_key = source.purchase_order_id
        AND target._quality_reasons = source._quality_reasons

    WHEN MATCHED THEN
        UPDATE SET
            target._quality_flag = source._quality_flag,
            target.quarantine_timestamp = current_timestamp(),
            target.purchase_order_id = source.purchase_order_id,
            target.vendor_id = source.vendor_id,
            target.vendor_name = source.vendor_name,
            target.vendor_country = source.vendor_country,
            target.purchase_order_date = source.purchase_order_date,
            target.order_quantity = source.order_quantity,
            target.unit_cost_usd = source.unit_cost_usd,
            target.manufacturing_order_id = source.manufacturing_order_id,
            target.production_date = source.production_date,
            target.materials_consumed_kg = source.materials_consumed_kg,
            target.production_hours = source.production_hours,
            target.manufacturing_batch_id = source.manufacturing_batch_id,
            target.shipment_id = source.shipment_id,
            target.shipment_date = source.shipment_date,
            target.actual_delivery_date = source.actual_delivery_date,
            target.promised_delivery_date = source.promised_delivery_date,
            target.carrier_name = source.carrier_name,
            target.logistics_cost_inr = source.logistics_cost_inr,
            target.usd_to_inr_exchange_rate = source.usd_to_inr_exchange_rate,
            target.production_lead_time_days = source.production_lead_time_days,
            target.shipping_lead_time_days = source.shipping_lead_time_days,
            target.total_lead_time_days = source.total_lead_time_days,
            target.delivery_status = source.delivery_status,
            target.dl_create_by = current_user()

    WHEN NOT MATCHED THEN
        INSERT
        (
            source_table,
            business_key,
            _quality_flag,
            _quality_reasons,
            quarantine_timestamp,
            purchase_order_id,
            vendor_id,
            vendor_name,
            vendor_country,
            purchase_order_date,
            order_quantity,
            unit_cost_usd,
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
            dl_create_by
        )
        VALUES
        (
            'purchase_order_fulfillment_sot',
            source.purchase_order_id,
            source._quality_flag,
            source._quality_reasons,
            current_timestamp(),
            source.purchase_order_id,
            source.vendor_id,
            source.vendor_name,
            source.vendor_country,
            source.purchase_order_date,
            source.order_quantity,
            source.unit_cost_usd,
            source.manufacturing_order_id,
            source.production_date,
            source.materials_consumed_kg,
            source.production_hours,
            source.manufacturing_batch_id,
            source.shipment_id,
            source.shipment_date,
            source.actual_delivery_date,
            source.promised_delivery_date,
            source.carrier_name,
            source.logistics_cost_inr,
            source.usd_to_inr_exchange_rate,
            source.production_lead_time_days,
            source.shipping_lead_time_days,
            source.total_lead_time_days,
            source.delivery_status,
            current_user()
        )
    """)

print("Quarantine MERGE completed successfully.")

# COMMAND ----------

# DBTITLE 1,Final validation
target_count = spark.sql(f"""
    SELECT 
        COUNT(*) AS row_count
    FROM {prj_catalog}.{sot_schema}.purchase_order_fulfillment_sot
""").collect()[0]["row_count"]

duplicate_target_count = spark.sql(f"""
    SELECT
        purchase_order_id
    FROM {prj_catalog}.{sot_schema}.purchase_order_fulfillment_sot
    GROUP BY purchase_order_id
    HAVING COUNT(*) > 1
""").count()

if duplicate_target_count > 0:
    raise ValueError(
        f"SOT target grain validation failed: "
        f"{duplicate_target_count} Purchase Orders have multiple rows."
    )
print(f"SOT target row count : {target_count}")
print(f"Accepted records     : {accepted_count}")
print(f"Quarantined records  : {failed_count}")
print("Grain                : One row per purchase_order_id")
print("Target grain         : PASSED")
print("Status               : SUCCESS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from supply_chain.sot_silver.purchase_order_fulfillment_sot

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from supply_chain.sot_silver.quarantine_supply_chain