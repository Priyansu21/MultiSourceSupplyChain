# MultiSourceSupplyChain

A production-style data engineering project built on Databricks that demonstrates an end-to-end multi-source supply chain data platform using Medallion Architecture.

The project simulates a realistic supply chain environment where Procurement, Vendor Master, Manufacturing, Logistics, and Currency Rate sources have different schemas, business keys, data types, and data quality characteristics.

The solution ingests the source data into Bronze, creates reusable Silver supporting objects, integrates the sources into a centralized Purchase Order Fulfillment Single Source of Truth (SOT), applies business quality gates with quarantine handling, and exposes standardized Gold analytical views.

The project also demonstrates Databricks Workflows, Delta MERGE processing, Unity Catalog, Git/GitHub integration, and Declarative Automation Bundles using YAML.

---

## Overview

Supply chain data is commonly distributed across multiple source systems. Each source may use different column names, business keys, data types, currencies, and lifecycle dates.

This project simulates that environment using five independent CSV sources:

- **Procurement**
- **Vendor Master**
- **Manufacturing**
- **Logistics**
- **Currency Rates**

The objective is to build a centralized and reusable business data model where downstream consumers do not repeatedly implement the same multi-source joins, currency conversion logic, lead-time calculations, and business quality rules.

### Pipeline Characteristics

- Multi-source Medallion Architecture
- Full daily snapshot ingestion into Bronze
- Schema-aware ingestion
- Silver conformance and integration
- Centralized Purchase Order Fulfillment SOT
- Explicit business grain: 1 row per Purchase Order
- Business-level quality gates
- Quarantine handling for failed records
- Snapshot-aware Delta MERGE processing
- Idempotent processing and safe re-runs
- Reusable Gold analytical views
- Databricks Workflows orchestration
- Git/GitHub version control
- Databricks Declarative Automation Bundles
- Unity Catalog managed tables and views

---

## Problem Statement

The five source systems do not follow a common data model.

For example, the same Purchase Order is represented using different column names:

| Source | Purchase Order Key |
|---|---|
| Procurement | `order_id` |
| Manufacturing | `order_ref` |
| Logistics | `po_number` |

Additional challenges include:

- Different source column names
- Different source data types
- Different business key definitions
- Different lifecycle dates
- USD procurement costs vs. INR logistics costs
- Orders without Manufacturing records
- Orders without Logistics records
- Vendor reference data requiring conformance
- FX rates requiring date-based enrichment
- Invalid date relationships
- Potential source cardinality issues
- Need for a trusted business grain
- Need to prevent invalid records from entering trusted analytical data

---

## Design Objective

The platform is designed to:

1. Preserve source data in Bronze without business transformation
2. Maintain expected source schemas through a Schema Registry
3. Create reusable Silver supporting objects
4. Conform and integrate multiple source systems
5. Establish a centralized Purchase Order Fulfillment SOT
6. Maintain a strict business grain of one row per Purchase Order
7. Apply business quality gates before trusted data is published
8. Route failed records to a dedicated quarantine table
9. Process trusted data using Delta MERGE
10. Handle full-snapshot changes correctly
11. Provide standardized Gold analytical views
12. Orchestrate the pipeline through Databricks Workflows
13. Manage job definitions through Declarative Automation Bundles and YAML
14. Maintain the complete project through Git/GitHub

---

## Source Data

Synthetic source data is generated using Python.

The current validation snapshot contains:

| Source | Records | Business Meaning |
|---|---:|---|
| Procurement | 75 | Purchase orders |
| Vendor Master | 12 | Vendor reference/master data |
| Manufacturing | 65 | Manufacturing batches |
| Logistics | 55 | Shipment and delivery data |
| Currency Rates | 181 | Daily USD → INR exchange rates |

### Current Source Relationships

```text
75 Procurement Orders
│
├── 65 have Manufacturing records
│   └── 10 do not have Manufacturing records
│
└── 55 have Logistics records
    └── 20 do not have Logistics records
```

Missing Manufacturing or Logistics records are treated as legitimate lifecycle gaps rather than automatic data-quality failures.

#### Current FX Coverage

- Date Range: 2026-01-01 → 2026-06-30
- Daily USD → INR records: 181
- Coverage: Complete daily rates for all procurement order dates

---

## High-Level Architecture

```
                         SOURCE LAYER
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   Procurement          Vendor Master         Manufacturing
        │                     │                     │
   Logistics            Currency Rates             │
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │   BRONZE LAYER    │
                    │                   │
                    │ Raw source tables │
                    │ Full snapshot     │
                    │ ingestion         │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │  SILVER SUPPORT   │
                    │                   │
                    │ Vendor Dimension  │
                    │ FX Reference      │
                    └─────────┬─────────┘
                              │
              ┌───────────────┴───────────────┐
              │                               │
              ▼                               ▼
       Procurement                     Manufacturing
              │                               │
              └───────────────┬───────────────┘
                              │
                         Logistics
                              │
                              ▼
                ┌──────────────────────────┐
                │ PURCHASE ORDER           │
                │ FULFILLMENT SOT          │
                │                          │
                │ Grain: 1 row / PO        │
                │                          │
                │ Conformance              │
                │ FX conversion            │
                │ Lead-time calculations   │
                │ Delivery status          │
                └────────────┬─────────────┘
                             │
                             ▼
                      ┌──────────────┐
                      │ QUALITY GATE │
                      └──────┬───────┘
                             │
                   ┌─────────┴─────────┐
                   │                   │
                   ▼                   ▼
               ACCEPTED             FAILED
                   │                   │
                   ▼                   ▼
              Trusted SOT          Quarantine
                   │
                   ▼
              GOLD VIEWS
                   │
       ┌───────────┼────────────┬───────────────┐
       │           │            │               │
       ▼           ▼            ▼               ▼
    SOT View   Supplier      On-Time          Cost
               Performance    Delivery        Impact
                                               
                   │
                   ▼
             PO Summary View
```

---

## Medallion Architecture

### Bronze Layer

The Bronze layer preserves the incoming source structure and performs ingestion-level validation.

Bronze does not perform business joins, business calculations, or business-level filtering.

**Bronze tables:**

- `supply_chain.ing_bronze.procurement_orders`
- `supply_chain.ing_bronze.vendor_master`
- `supply_chain.ing_bronze.manufacturing_batches`
- `supply_chain.ing_bronze.logistics_shipments`
- `supply_chain.ing_bronze.currency_rates`

Bronze ingestion uses full snapshot refreshes:

```
CSV → Read with explicit schema → Minimal ingestion validation → Overwrite Bronze Delta table
```

### Silver Layer

Silver contains reusable supporting objects and the centralized business SOT.

#### Supporting Silver Objects

- `supply_chain.sot_silver.vendor_master_dim` - Conformed vendor reference data
- `supply_chain.sot_silver.fx_exchange_rates_ref` - Daily USD → INR exchange rates
- `supply_chain.sot_silver.quarantine_supply_chain` - Failed records with quality reasons

#### Vendor Master Dimension

Conforms Vendor Master reference data for reuse by the integrated SOT.

#### FX Exchange Rate Reference

Provides daily USD → INR exchange rates used to convert Procurement costs into INR using the Purchase Order date.

#### Quarantine

Stores records that fail Silver business-quality rules.

The quarantine record retains business context and the quality failure reason rather than silently dropping the record.

### Purchase Order Fulfillment SOT

The central Silver business object is:

```
supply_chain.sot_silver.purchase_order_fulfillment_sot
```

#### Business Grain

**1 row = 1 Purchase Order**

The Procurement dataset is the driving source.

The SOT integrates:

- Procurement
- Vendor Master
- Manufacturing
- Logistics
- FX Reference

#### Key Conformance

| Source Column | SOT Column |
|---|---|
| `order_id` | `purchase_order_id` |
| `country` | `vendor_country` |
| `cost_usd` | `unit_cost_usd` |
| `mfg_order_id` | `manufacturing_order_id` |
| `materials_used_kg` | `materials_consumed_kg` |
| `batch_id` | `manufacturing_batch_id` |
| `ship_date` | `shipment_date` |
| `delivery_date` | `actual_delivery_date` |
| `carrier` | `carrier_name` |
| `cost_inr` | `logistics_cost_inr` |
| `exchange_rate` | `usd_to_inr_exchange_rate` |

#### SOT Business Logic

**Currency Conversion:**

```
unit_cost_inr = unit_cost_usd × USD_TO_INR exchange rate
```

The FX rate is selected using the Purchase Order date.

**Lead-Time Metrics:**

The SOT derives:

- `production_lead_time_days`
- `shipping_lead_time_days`
- `total_lead_time_days`

**Delivery Status:**

The SOT derives delivery status based on shipment and promised delivery dates.

Possible states include:

- `ON_TIME`
- `LATE`
- `NOT_SHIPPED`
- `IN_TRANSIT`

---

## Data Quality Framework

Quality validation is performed at the Silver business layer.

### Quality Rules

The pipeline validates:

- Purchase Order ID is not null
- Order quantity is greater than zero
- Unit cost is greater than zero
- FX exchange rate is greater than zero
- Production date is not before Purchase Order date
- Shipment date is not before Production date
- Delivery date is not before Shipment date
- Promised delivery date is not before Purchase Order date
- Vendor reference integrity
- Integrated SOT cardinality
- Final SOT grain of one row per Purchase Order

### Important Design Decision

Missing Manufacturing or Logistics records are not automatically treated as failures.

LEFT JOIN logic allows legitimate incomplete lifecycle states to remain in the integrated dataset.

### Quality Gate Flow

```
Integrated Multi-Source Dataset
             │
             ▼
        Quality Gates
             │
       ┌─────┴─────┐
       │           │
       ▼           ▼
     PASS         FAIL
       │           │
       ▼           ▼
 Trusted SOT   Quarantine
```

Invalid records are routed to quarantine rather than silently deleted.

### Snapshot-Aware Delta MERGE

Bronze is refreshed as a full snapshot.

Therefore the Silver SOT must represent the current accepted snapshot rather than retaining stale records from previous snapshots.

The SOT uses Delta MERGE semantics:

```sql
WHEN MATCHED
    → UPDATE

WHEN NOT MATCHED
    → INSERT

WHEN NOT MATCHED BY SOURCE
    → DELETE
```

This ensures that records removed from the current accepted snapshot do not remain as stale trusted SOT records.

The quarantine table is also refreshed as the current processing snapshot before loading the current failed records.

This gives the pipeline the following invariant:

```
Current Source POs = Trusted SOT POs + Current Quarantine POs

where: SOT ∩ Quarantine = 0
```

---

## Gold Layer

Gold consists of standardized analytical views over the trusted Silver SOT.

Gold objects are created under:

```
supply_chain.sot_gold_views
```

### Technical View

**`supply_order_fulfillment_sot_vw`**

Provides a direct consumption view over the trusted SOT while maintaining clear lineage.

### Supplier Performance

**`supplier_performance_analysis_vw`**

Provides supplier-level metrics including:

- Total orders
- Total quantity
- Procurement cost in USD
- Procurement cost in INR
- Average lead time
- On-time deliveries
- Late deliveries
- Pending deliveries
- On-time percentage

### On-Time Delivery Analysis

**`on_time_delivery_analysis_vw`**

Provides delivery analysis by:

- Vendor
- Carrier
- Delivery status
- Shipment count
- Average days versus promised date
- Earliest delivery
- Latest delivery
- Average total lead time

### Cost Impact Analysis

**`cost_impact_analysis_vw`**

Provides:

- Order count
- Average unit cost USD
- Average unit cost INR
- Total procurement cost USD
- Total procurement cost INR
- Total logistics cost INR
- Total landed cost INR

### Purchase Order Fulfillment Summary

**`po_fulfillment_summary_vw`**

Provides a business-friendly Purchase Order view with fields such as:

- PO ID
- Supplier
- Product
- Quantity
- Unit Cost USD
- Unit Cost INR
- Order Date
- Production Date
- Delivery Date
- Promised Date
- Status
- Days to Deliver

---

## Orchestration

The project uses two Databricks Workflow Jobs.

### Job 1 — Bronze Ingestion

**Job Name:** `jb_supply_chain_bronze_ingestion`

**Tasks:**

- `bronze_procurement`
- `bronze_manufacturing`
- `bronze_logistics`
- `bronze_currency`
- `bronze_vendor_master`

These tasks are independent and can execute in parallel.

**Schedule:**

- 06:00 IST
- 14:00 IST
- 22:00 IST

### Job 2 — Silver + Gold

**Job Name:** `jb_supply_chain_silver_gold`

**Tasks:**

- `silver_vendor_master`
- `silver_fx_exchange_rates`
- `silver_purchase_order_fulfillment`
- `gold_purchase_order_fulfillment_sot_vw`
- `gold_supplier_performance`
- `gold_on_time_delivery`
- `gold_cost_impact`
- `gold_po_fulfillment_summary`

**Dependency Structure:**

```
silver_vendor_master ───────┐
                            │
                            ▼
                  silver_purchase_order
                  _fulfillment
                            ▲
                            │
silver_fx_exchange_rates ───┘
                            │
                            ▼
             gold_purchase_order_fulfillment_sot_vw
                            │
          ┌─────────────────┼──────────────────┐
          │                 │                  │
          ▼                 ▼                  ▼
   supplier_performance   on_time_delivery   cost_impact
          │
          └──────────────────────┐
                                 ▼
                       po_fulfillment_summary
```

**Schedule:**

- 19:00 IST

The 19:00 Silver/Gold job consumes the latest completed Bronze snapshot.

---

## Databricks Declarative Automation Bundles

The project uses Databricks Declarative Automation Bundles to define and deploy job configuration as code.

**Root bundle configuration:**

```
databricks.yml
```

**Job resource definitions:**

```
resources/jb_supply_chain_bronze_ingestion.job.yml
resources/jb_supply_chain_silver_gold.job.yml
```

The bundle is validated and deployed using the Databricks CLI.

**Example commands:**

```bash
databricks bundle validate --target dev --profile dev
databricks bundle deploy --target dev --profile dev
```

The existing Databricks jobs are bound to the bundle so deployment updates the existing resources rather than creating duplicate jobs.

---

## Version Control

The project is maintained using Git and GitHub.

The repository contains:

- Source data
- Python data generation scripts
- Databricks notebooks
- Schema Registry
- Workflow resource YAML
- Bundle configuration
- Documentation

Databricks Git folders are used for workspace-based development and synchronization with the Git repository.

**Data Flow:**

```
GitHub Repository
        ↓
Databricks Git Folder (Pull)
        ↓
Workspace data/ and notebooks/
        ↓
Bronze Notebooks (read from workspace)
```

---

## Schema Registry

The project maintains a separate schema registry:

```
schema_registry/schema_registry.csv
```

The registry documents expected source metadata including:

- Source table
- Column name
- Data type
- Nullability
- Business key
- Column description

This separates source schema expectations from transformation logic.

---

## Project Structure

```
MultiSourceSupplyChain/
│
├── data/
│   ├── procurement.csv
│   ├── vendor_master.csv
│   ├── manufacturing.csv
│   ├── logistics.csv
│   └── currency_rates.csv
│
├── data_generation/
│   ├── generate_supply_chain_data.py
│   └── generate_supply_chain_data_v2.py
│
├── notebooks/
│   ├── bronze/
│   │   ├── nb_func_procurement_orders.py
│   │   ├── nb_func_manufacturing_batches.py
│   │   ├── nb_func_logistics_shipments.py
│   │   ├── nb_func_currency_rates.py
│   │   └── nb_func_vendor_master.py
│   │
│   ├── silver/
│   │   ├── ddl/
│   │   │   ├── nb_vendor_master_dim_ddl.py
│   │   │   ├── nb_fx_exchange_rates_ref_ddl.py
│   │   │   ├── nb_quarantine_supply_chain_ddl.py
│   │   │   └── nb_purchase_order_fulfillment_sot_ddl.py
│   │   │
│   │   └── etl/
│   │       ├── nb_func_vendor_master_dim.py
│   │       ├── nb_func_fx_exchange_rates_ref.py
│   │       └── nb_func_purchase_order_fulfillment_sot.py
│   │
│   └── gold/
│       ├── nb_purchase_order_fulfillment_sot_vw.py
│       ├── nb_supplier_performance_analysis_vw.py
│       ├── nb_on_time_delivery_analysis_vw.py
│       ├── nb_cost_impact_analysis_vw.py
│       └── nb_po_fulfillment_summary_vw.py
│
├── resources/
│   ├── jb_supply_chain_bronze_ingestion.job.yml
│   └── jb_supply_chain_silver_gold.job.yml
│
├── schema_registry/
│   └── schema_registry.csv
│
├── databricks.yml
├── .gitignore
└── README.md
```

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Data engineering platform |
| Unity Catalog | Catalog and governance |
| Delta Lake | Managed Silver/Bronze tables |
| PySpark | Data ingestion and transformation |
| SQL | Data transformation, validation and analytics |
| Python | Synthetic data generation |
| Databricks Workflows | Pipeline orchestration |
| Databricks Declarative Automation Bundles | Infrastructure/job configuration as code |
| Databricks CLI | Bundle validation and deployment |
| Git | Version control |
| GitHub | Remote repository |
| VS Code | Local development |

---

## Implementation Stages

### Stage 1 — Synthetic Source Data Generation 

Generated controlled multi-source supply chain data with:

- 75 Procurement records
- 12 Vendor records
- 65 Manufacturing records
- 55 Logistics records
- 181 Currency records
- Controlled source relationships
- Intentional temporal data-quality issues

### Stage 2 — Bronze Ingestion 

Implemented five independent Bronze ingestion notebooks.

Each notebook:

- Reads the source CSV using an explicit schema
- Performs basic ingestion validation
- Writes the source data into a Bronze Delta table
- Validates the target row count

### Stage 3 — Schema Registry 

Implemented:

```
schema_registry/schema_registry.csv
```

to document expected source schemas and metadata.

### Stage 4 — Silver Supporting Objects 

Implemented:

- `vendor_master_dim`
- `fx_exchange_rates_ref`
- `quarantine_supply_chain`

These provide reusable reference and quality-handling capabilities for the main SOT.

### Stage 5 — Purchase Order Fulfillment SOT 

Implemented:

```
purchase_order_fulfillment_sot
```

with:

- Multi-source integration
- Explicit one-row-per-PO grain
- Vendor enrichment
- Manufacturing enrichment
- Logistics enrichment
- FX conversion
- Lead-time calculations
- Delivery status
- Audit metadata

### Stage 6 — Quality Gates and Quarantine 

Implemented Silver business-quality validation.

Failed records are routed to:

```
quarantine_supply_chain
```

with explicit quality reasons.

### Stage 7 — Snapshot-Aware Delta MERGE 

Implemented:

- `WHEN MATCHED`
- `WHEN NOT MATCHED`
- `WHEN NOT MATCHED BY SOURCE THEN DELETE`

to keep the trusted SOT aligned with the current accepted snapshot.

The quarantine table is refreshed for the current processing snapshot.

### Stage 8 — Gold Analytics 

Implemented five Gold views:

- `purchase_order_fulfillment_sot_vw`
- `supplier_performance_analysis_vw`
- `on_time_delivery_analysis_vw`
- `cost_impact_analysis_vw`
- `po_fulfillment_summary_vw`

### Stage 9 — Databricks Workflows 

Implemented two Databricks Jobs:

- `jb_supply_chain_bronze_ingestion`
- `jb_supply_chain_silver_gold`

with scheduled execution and task dependencies.

### Stage 10 — Declarative Automation Bundles 

Implemented:

- `databricks.yml`
- `resources/*.job.yml`

and deployed the existing Databricks jobs through the Databricks CLI.

### Stage 11 — End-to-End Validation 

Validated the complete pipeline using a fresh V2 source snapshot.

---

## Final Validation Results

### Bronze Validation

| Table | Expected | Actual |
|---|---|---|
| Procurement Orders | 75 | 75 |
| Manufacturing Batches | 65 | 65 |
| Logistics Shipments | 55 | 55 |
| Currency Rates | 181 | 181 |
| Vendor Master | 12 | 12 |

All Bronze counts matched the generated source data.

### Silver Validation

Final processing result:

```
Source Procurement POs      75
Trusted SOT POs             67
Quarantined POs              8
SOT / Quarantine overlap     0
```

The final SOT grain was validated:

```
SOT rows                    67
Distinct Purchase Orders    67
Duplicate Purchase Orders    0
```

The reconciliation condition was satisfied:

```
67 Trusted SOT
+
8 Quarantine
=
75 Source POs

and:

SOT ∩ Quarantine = 0
```

### Gold Validation

Final Gold validation:

| Gold Object | Rows | Distinct Business Entities |
|---|---|---|
| `purchase_order_fulfillment_sot_vw` | 67 | 67 Purchase Orders |
| `supplier_performance_analysis_vw` | 12 | 12 Vendors |
| `on_time_delivery_analysis_vw` | 37 | 12 Vendors |
| `cost_impact_analysis_vw` | 30 | 12 Vendors |
| `po_fulfillment_summary_vw` | 67 | 67 Purchase Orders |

### Gold Reconciliation

```
Silver SOT                         67
Gold SOT View                      67
Gold PO Summary                    67
```

The technical Gold view and business summary both preserve the expected 1-row-per-PO grain.

---

## Key Design Decisions

### Why a Centralized SOT?

A single Purchase Order Fulfillment SOT was chosen instead of forcing each downstream consumer to repeatedly join Procurement, Vendor, Manufacturing, Logistics, and FX data.

The SOT centralizes:

- Source conformance
- Multi-source joins
- FX conversion
- Lead-time calculations
- Delivery status
- Quality validation

This creates a reusable business integration point for downstream consumers.

### Why One Row Per Purchase Order?

The Purchase Order is the central business entity for this use case.

Maintaining:

```
1 row = 1 Purchase Order
```

prevents downstream consumers from unintentionally multiplying records when combining Manufacturing and Logistics data.

The SOT explicitly validates the final integrated grain.

### Why Quarantine Instead of Dropping Records?

Invalid records are not silently deleted.

Instead:

```
Invalid Record
      ↓
Quality Rule
      ↓
Quarantine
      ↓
Failure Reason
```

This preserves visibility into data-quality failures and makes troubleshooting easier.

### Why Full Snapshot Processing?

The source simulation represents recurring source snapshots rather than a true CDC stream.

Therefore Bronze is refreshed as a full snapshot.

The Silver SOT is designed to reflect the current accepted snapshot, including deletion of target records that are no longer present in the accepted source.

This avoids stale trusted records remaining in the SOT.

### Why Delta MERGE?

MERGE provides controlled insert/update behavior using the Purchase Order business key.

It supports:

- Existing record updates
- New record inserts
- Snapshot cleanup
- Safe repeated execution

Audit metadata is preserved as part of the SOT processing design.

### Data Quality Failure Examples

The generated source data intentionally contains invalid temporal relationships such as:

- `production_date < purchase_order_date`
- `shipment_date < production_date`
- `delivery_date < shipment_date`
- `promised_delivery_date < purchase_order_date`

These conditions are detected by the Silver quality gates and routed to quarantine.

This allows the project to demonstrate actual failure handling rather than only processing clean synthetic data.

---

## Operational Flow

The complete operational flow is:

```
1. Generate / receive source CSV snapshot
                    ↓
2. Bronze ingestion job
                    ↓
3. Bronze validation
                    ↓
4. Silver supporting reference ETLs
                    ↓
5. Purchase Order Fulfillment SOT
                    ↓
6. Quality gates
                    ↓
             ┌──────┴──────┐
             ↓             ↓
          Trusted       Quarantine
             ↓
7. Gold analytical views
             ↓
8. Consumers
```

---

## Key Interview Takeaways

This project demonstrates practical Data Engineering concepts including:

- Medallion Architecture
- Bronze/Silver/Gold design
- Multi-source integration
- Source-to-business schema conformance
- Business grain definition
- Cardinality validation
- Data-quality gates
- Quarantine handling
- Delta Lake
- Delta MERGE
- Snapshot-aware processing
- Idempotent pipeline design
- PySpark
- SQL
- Unity Catalog
- Databricks Workflows
- Job dependencies
- Scheduled pipelines
- Git/GitHub integration
- Databricks Declarative Automation Bundles
- YAML-based job configuration
- Reconciliation and pipeline validation

---

## Project Summary

This project demonstrates how multiple heterogeneous supply chain sources can be transformed into a centralized and trusted business data platform.

The key design is the Purchase Order Fulfillment SOT, which integrates five source systems at a controlled one-row-per-Purchase-Order grain.

The pipeline preserves raw source data in Bronze, creates reusable Silver reference objects, applies business-quality validation, routes failures to quarantine, maintains the current trusted snapshot through Delta MERGE, and exposes standardized Gold analytical views.

The solution is orchestrated through Databricks Workflows and managed through Git and Declarative Automation Bundles.

The final end-to-end validation confirms:

```
75 source Purchase Orders
        ↓
67 trusted Purchase Orders
        +
8 quarantined Purchase Orders
        ↓
67 Gold Purchase Order records

with:

0 SOT / Quarantine overlap
0 duplicate Purchase Orders in SOT
67 Silver SOT records
67 Gold SOT view records
67 Gold PO summary records
```

---

## Current Project Status

- [x] Synthetic source data generation
- [x] Bronze ingestion
- [x] Schema Registry
- [x] Silver supporting objects
- [x] Purchase Order Fulfillment SOT
- [x] Business quality gates
- [x] Quarantine handling
- [x] Snapshot-aware Delta MERGE
- [x] Gold analytical views
- [x] End-to-end reconciliation
- [x] Databricks Workflow orchestration
- [x] Scheduled jobs
- [x] Git/GitHub integration
- [x] Databricks Declarative Automation Bundle
- [x] Fresh-data end-to-end validation
- [x] Project documentation

---

## Future Enhancements

The current project intentionally focuses on batch-oriented multi-source processing.

Potential future enhancements include:

- Auto Loader for incremental file ingestion
- Change Data Capture
- Streaming ingestion
- More advanced schema evolution handling
- Metadata-driven pipeline configuration
- Centralized audit/control tables
- Automated data-quality reporting
- Alerting and notifications
- CI/CD integration
- Environment promotion from development to production
- Consumer data exports
- Dashboard integration

These are future extensions rather than required components of the current implementation.