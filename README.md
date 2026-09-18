# MultiSourceSupplyChain

A production-grade data platform built on Databricks that demonstrates enterprise medallion architecture patterns. This project simulates a realistic multi-source supply chain scenario where heterogeneous data sources are conformed into a centralized, business-trusted Single Source of Truth (SOT).

---

## Overview

Supply chain data is typically distributed across multiple source systems, each with its own structure, naming conventions, and data quality standards. This project simulates that real-world scenario using five independent CSV sources and builds an end-to-end pipeline that brings them into a cohesive analytical model.

**Five Source Systems:**
- Procurement (purchase order transactions)
- Vendor Master (vendor reference data)
- Manufacturing (production batch data)
- Logistics (shipment and delivery data)
- Currency Rates (FX reference data)

**Pipeline Characteristics:**
- Reproducible and incremental
- Data-quality aware with quarantine handling
- Idempotent (safe to re-run)
- Explicitly defined business grain
- Production-ready patterns (SOT, conformance, MERGE)

---

## Problem Statement

The source systems do not follow a common data model:

**Example:** A purchase order is identified by:
- `order_id` in Procurement
- `order_ref` in Manufacturing  
- `po_number` in Logistics

Additional challenges:
- Inconsistent column names and data types
- Different business key definitions
- Mismatched date fields (order_date vs. mfg_date vs. ship_date)
- Multiple currencies (USD in Procurement, INR in Logistics)
- Source relationship gaps (some orders have no manufacturing or logistics records)
- Intentional data-quality issues to simulate real-world scenarios

**Design Objective:**

Build a platform that:
1. Preserves original source data without modification (Bronze)
2. Maintains explicit source schema definitions (Schema Registry)
3. Conforms heterogeneous sources into a unified business model (Silver)
4. Establishes a single, trusted integration point (Purchase Order Fulfillment SOT)
5. Applies business-level validation and quarantine handling
6. Supports incremental, idempotent processing (MERGE)
7. Provides standardized analytical views (Gold)

---

## What We Have

**Synthetic Source Data** (Generated using Python)

| Source | Records | Business Meaning |
|--------|--------:|------------------|
| Procurement | 50 | Purchase orders (master records) |
| Vendor Master | 5 | Vendor reference data |
| Manufacturing | 45 | Manufacturing batches (5 orders without mfg records) |
| Logistics | 40 | Shipments (5 mfg batches without logistics) |
| Currency Rates | 90 | USD to INR daily rates |

**Source Relationships:**
```
50 Procurement Orders
        ├─ 45 have Manufacturing records (1:0-1 relationship)
        └─ 40 have Logistics records (1:0-1 relationship)
```

The intentional gaps allow the pipeline to demonstrate handling of legitimate real-world scenarios where not all orders complete the full lifecycle.

---

## What We're Building

The pipeline transforms source data through three layers:

```
Source CSVs (5 files)
    ↓
BRONZE LAYER
(Source preservation, basic validation)
    ├── procurement_orders
    ├── vendor_master
    ├── manufacturing_batches
    ├── logistics_shipments
    └── currency_rates
    ↓
SILVER LAYER
(Conformance, integration, quality gates)
    ├── Vendor Master Dimension
    ├── FX Exchange Rates Reference
    ├── Quarantine (failures)
    └── Purchase Order Fulfillment SOT ← Single Source of Truth
            ↓
GOLD LAYER
(Analytical views for consumers)
    └── purchase_order_fulfillment_sot_vw
    └── (Optional: supplier_performance, on_time_delivery, cost_impact)
```

**Key Design: The Purchase Order Fulfillment SOT**

The Silver SOT integrates five data sources into a single, business-trusted dataset:

- **Grain:** 1 row per Purchase Order ID
- **Driving Source:** Procurement (transformed and enriched)
- **Enrichment:** Vendor Master, Manufacturing, Logistics, FX Reference
- **Business Logic:**
  - FX conversion (USD cost → INR cost at order date)
  - Lead time calculations (order → production → shipment → delivery)
  - Delivery status (on-time vs. late)
  - Quality flags (pass/fail) and quarantine routing

Instead of allowing downstream consumers to repeatedly join these five sources and implement the same business rules, the SOT provides a reusable, conformed integration point.

---

The project follows the Medallion Architecture:

```mermaid

flowchart LR

    A[Procurement CSV]
    B[Vendor Master CSV]
    C[Manufacturing CSV]
    D[Logistics CSV]
    E[Currency Rates CSV]

    A --> BR[Bronze]
    B --> BR
    C --> BR
    D --> BR
    E --> BR

    BR --> SV[Silver Validation & Conformance]

    SV --> V[Vendor Master Dimension]
    SV --> FX[FX Reference]

    V --> SOT[Purchase Order Fulfillment SOT]
    FX --> SOT
    SV --> SOT

    SOT --> DQ[Quality Gates]

    DQ --> AC[Accepted Data]
    DQ --> Q[Quarantine]

    AC --> G[Gold Standard Views]

    G --> C1[Consumers]

 ```   

**Medallion Architecture Principles Applied:**

- **Bronze:** Preserves raw source data and schema exactly as received
- **Silver:** Applies conformance, integration, quality validation, and quarantine routing
- **Gold:** Provides standardized, consumption-ready views over trusted Silver data

---

## Implementation Stages

### Stage 1: Source Data Generation ✅
Generate controlled synthetic datasets with:
- Defined relationships (45 of 50 orders have mfg data, 40 have logistics data)
- Intentional data-quality issues (invalid date relationships, nulls, cardinality violations)
- Realistic schema diversity (different column names, types, currencies)

### Stage 2: Bronze Ingestion ✅
Load five CSV sources into Databricks Bronze layer without transformation:

```
supply_chain.ing_bronze.procurement_orders
supply_chain.ing_bronze.vendor_master
supply_chain.ing_bronze.manufacturing_batches
supply_chain.ing_bronze.logistics_shipments
supply_chain.ing_bronze.currency_rates
```

### Stage 3: Schema Registry ✅
Maintain expected source schemas separately from the pipeline:

```
schema_registry/schema_registry.csv
```

Defines: columns, data types, nullability, business keys, descriptions.

### Stage 4: Silver Conformance 🔄 (In Progress)

**Supporting Objects:**
- `vendor_master_dim` — Conformed, deduplicated vendor reference
- `fx_exchange_rates_ref` — Daily USD → INR rates
- `quarantine_supply_chain` — Failed records with failure reason, source, timestamp

**Main Business SOT:**
- `purchase_order_fulfillment_sot` — Integrated 1-row-per-PO dataset

**Quality Gates:**
- Structural validation (Bronze layer validates source arrival and basic schema)
- Business validation (Silver validates cardinality, business rules, data relationships)
- Cardinality check: Prevent 1 PO → N batches/shipments from silently multiplying SOT rows
- Quarantine routing: Invalid records diverted rather than deleted

### Stage 5: Delta MERGE Processing ⏳
Implement idempotent insert/update logic:

- **Business Key:** `purchase_order_id`
- **MERGE Logic:**
  - `WHEN MATCHED` → UPDATE changed attributes
  - `WHEN NOT MATCHED` → INSERT new records
  - Preserve audit columns (`dl_creation_date`, `dl_create_by`)

### Stage 6: Gold Analytics ⏳

**Standard Views:**

### Gold Layer Strategy

The Gold layer uses a **two-tier consumption model**:

### Technical Tier
Provides clear lineage and source-to-SOT traceability:
- `purchase_order_fulfillment_sot_vw` — Direct SOT view showing all business context

### Business Tier
Renames columns to business language and aggregates for specific use cases:
- `po_fulfillment_summary_vw` — Purchase order status for dashboards (friendly column names)
- `supplier_scorecard_vw` — Supplier SLA metrics (on-time %, lead time trends)
- Optional: Additional analytical views as business requirements evolve

This separation ensures data engineers understand lineage while business users see business-meaningful names.

**Optional Analytical Views:**
- `supplier_performance_vw` — Supplier SLAs, delivery rates, cost trends
- `on_time_delivery_vw` — Delivery performance by supplier/region
- `cost_impact_vw` — Cost variance analysis (USD vs. INR impact, lead time cost)

### Stage 7: Orchestration ⏳
Databricks Workflows orchestrate the pipeline:

```
Bronze Job (ingest all 5 sources)
    ↓ [SUCCESS]
Silver Job (conform, validate, merge)
    ↓ [SUCCESS]
Gold Standard Views (automatically reflect latest Silver)
    ↓
Consumers
```

### Stage 8: Documentation & Demo ⏳
- Reconciliation queries
- Data lineage documentation
- 2-minute demo video
- GitHub push with clean history

---

## Technology Stack

- **Compute:** Databricks
- **Data Format:** Delta Lake / Parquet
- **Languages:** SQL, PySpark, Python
- **Data Catalog:** Unity Catalog
- **Orchestration:** Databricks Workflows
- **Version Control:** Git / GitHub
- **IDE:** VS Code

---

## Project Structure

```
MultiSourceSupplyChain/
│
├── data/
│   ├── procurement.csv
│   ├── vendor_master.csv
│   ├── manufacturing_batches.csv
│   ├── logistics_shipments.csv
│   └── currency_rates.csv
│
├── data_generation/
│   └── generate_supply_chain_data.py
│
├── schema_registry/
│   └── schema_registry.csv
│
├── notebooks/
│   ├── 01_bronze_ingestion.sql
│   ├── 02_schema_validation.sql
│   ├── 03_silver_conformance.sql
│   ├── 04_sot_merge.sql
│   ├── 05_gold_views.sql
│   └── 06_reconciliation.sql
│
├── workflows/
│   └── medallion_pipeline.yaml
│
├── .gitignore
├── README.md
└── LICENSE

```

---

## Key Design Decisions

### Why One SOT Instead of Three Separate Silver Tables?

**Alternative (Not Chosen):** Keep procurement, manufacturing, and logistics as separate Silver tables. Gold layer joins them as needed.

**Why We Chose Single SOT:**

1. **Business Reality:** In your enterprise, a Purchase Order Fulfillment is a single business process with defined relationships.
2. **Cardinality Safety:** By validating relationships upfront in Silver, we prevent accidental row multiplication downstream.
3. **Reusability:** Downstream consumers don't repeatedly implement the same five-source join + FX conversion + lead time logic.
4. **Interview Story:** "I centralized recurring multi-source business logic into a reusable SOT rather than allowing it to scatter across downstream processes."

### Cardinality Validation Strategy

**Problem:** One Procurement Order might match multiple Manufacturing records or Logistics shipments. A blind LEFT JOIN multiplies rows.

**Solution:**
1. Before joining Manufacturing and Logistics, validate that each PO appears at most once in each source.
2. If a PO has 2+ manufacturing records (unexpected), route that entire PO to quarantine.
3. The SOT grain remains 1 row per PO — never silently multiplied.

### Quality Gates: Bronze vs. Silver

**Bronze Validation (Structural):**
- Did the source files arrive?
- Are expected tables populated?
- Do columns match expected schema?

**Silver Quality Gates (Business):**
- Are business keys non-null and unique?
- Are date relationships valid (order_date ≤ production_date ≤ ship_date)?
- Are cardinality assumptions met?
- Are currency conversions possible?
- Do vendor_ids exist in vendor_master?

Invalid records are routed to quarantine with reason codes, not silently deleted.

---

## Future Enhancements - Data export and orchestration

While this project focuses on building the trusted SOT and Gold views, a production version would 
include consumer distribution patterns (exports to Box/SharePoint, Slack notifications, email 
distributions) orchestrated through a metadata-driven EDL framework.

---

## Interview Narrative - just for prep remove later

**Project Summary:**

"I built a multi-source supply chain data platform on Databricks using medallion architecture principles. The challenge was that five independent sources (procurement, vendor, manufacturing, logistics, FX) used different column names, schemas, and relationships. Instead of allowing downstream consumers to repeatedly join these sources, I created a centralized Purchase Order Fulfillment SOT in Silver that integrates all sources at a defined grain of one row per purchase order. The pipeline performs cardinality validation to prevent row multiplication, routes invalid records to quarantine with failure reasons, and uses Delta MERGE for idempotent processing. Gold provides standardized views over the SOT. This demonstrates understanding of medallion patterns, data quality handling, business-driven schema design, and production-grade ETL."

---

## Current Status

- [x] Synthetic data generation
- [x] Bronze ingestion
- [x] Schema registry
- [ ] Silver supporting objects (vendor_dim, FX reference)
- [ ] Purchase order fulfillment SOT
- [ ] Quality gates and quarantine routing
- [ ] MERGE idempotency testing
- [ ] Gold analytical views
- [ ] Data reconciliation
- [ ] Databricks Workflow orchestration
- [ ] Documentation and demo

---

## Contact

**GitHub:** [Priyansu21](https://github.com/Priyansu21)

---

## License

This project is for educational and portfolio purposes.