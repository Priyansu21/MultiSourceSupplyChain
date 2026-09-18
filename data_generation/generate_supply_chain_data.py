import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

random.seed(42)

PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "data"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 3, 31)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def random_date(start_date, end_date):
    """Generate a random date between start_date and end_date."""
    days = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, days))


def random_choice(values):
    """Select a reproducible random value."""
    return random.choice(values)


# ============================================================
# 1. VENDOR MASTER DATASET
# ============================================================
# Source system: Vendor Master
# Grain: One row per vendor
# Rows: 5
#
# This is a master/reference dataset used to enrich
# Procurement transactions.
#
# Vendor distribution across Procurement:
# V001 -> 12 orders
# V002 -> 10 orders
# V003 -> 12 orders
# V004 -> 10 orders
# V005 -> 6 orders
# ============================================================

vendor_master_rows = [
    {
        "vendor_id": "V001",
        "vendor_name": "Vendor A",
        "country": "India",
        "vendor_category": "Raw Materials",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V002",
        "vendor_name": "Vendor B",
        "country": "India",
        "vendor_category": "Components",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V003",
        "vendor_name": "Vendor C",
        "country": "India",
        "vendor_category": "Packaging",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V004",
        "vendor_name": "Vendor D",
        "country": "USA",
        "vendor_category": "Components",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V005",
        "vendor_name": "Vendor E",
        "country": "Germany",
        "vendor_category": "Specialty Materials",
        "vendor_status": "ACTIVE"
    }
]

vendor_master = pd.DataFrame(vendor_master_rows)

vendor_master.to_csv(
    OUTPUT_DIR / "vendor_master.csv",
    index=False
)


# ============================================================
# 2. PROCUREMENT DATASET
# ============================================================
# Source system: Procurement
# Grain: One row per purchase order
# Rows: 50
#
# Procurement now contains vendor_id so that the transaction
# can be linked to the Vendor Master using a proper identifier.
# ============================================================

procurement_rows = []


# Exact vendor distribution
vendor_assignments = (
    ["V001"] * 12
    + ["V002"] * 10
    + ["V003"] * 12
    + ["V004"] * 10
    + ["V005"] * 6
)

# Shuffle while keeping the exact distribution.
random.shuffle(vendor_assignments)


vendors_by_id = {
    row["vendor_id"]: row
    for row in vendor_master_rows
}


product_types = [
    "Raw Materials",
    "Components",
    "Finished Goods"
]


payment_terms = [
    "NET30",
    "NET45",
    "NET60"
]


for i in range(1, 51):

    vendor_id = vendor_assignments[i - 1]

    vendor = vendors_by_id[vendor_id]

    order_date = random_date(
        START_DATE,
        datetime(2026, 3, 15)
    )

    quantity = random.randint(10, 500)

    unit_cost_usd = round(
        random.uniform(5, 100),
        2
    )

    procurement_rows.append({
        "order_id": f"PO-{str(i).zfill(3)}",
        "vendor_id": vendor_id,
        "vendor_name": vendor["vendor_name"],
        "country": vendor["country"],
        "cost_usd": unit_cost_usd,
        "order_date": order_date,
        "quantity": quantity,
        "product_type": random_choice(product_types),
        "payment_terms": random_choice(payment_terms)
    })


procurement = pd.DataFrame(procurement_rows)


procurement.to_csv(
    OUTPUT_DIR / "procurement.csv",
    index=False
)


# ============================================================
# 3. MANUFACTURING DATASET
# ============================================================
# Source system: Manufacturing
# Grain: One row per production batch
# Rows: 45
#
# Controlled relationship:
#
# 50 Procurement orders
#       ↓
# 45 selected orders get manufacturing records
#
# Schema intentionally differs from Procurement.
# ============================================================

manufacturing_rows = []


# Select exactly 45 procurement orders
manufacturing_orders = random.sample(
    list(procurement["order_id"]),
    45
)


for i, po_number in enumerate(
    manufacturing_orders,
    start=1
):

    procurement_record = procurement[
        procurement["order_id"] == po_number
    ].iloc[0]

    order_date = procurement_record["order_date"]

    # Production normally happens after procurement order.
    production_delay = random.randint(3, 10)

    production_date = (
        order_date +
        timedelta(days=production_delay)
    )

    manufacturing_rows.append({
        "mfg_order_id": f"MO-{str(i).zfill(3)}",

        # Join key with DIFFERENT column name
        "order_ref": po_number,

        "production_date": production_date,

        "materials_used_kg": random.randint(
            50,
            500
        ),

        "production_hours": random.randint(
            8,
            48
        ),

        "batch_id": (
            f"BATCH-{production_date.strftime('%b').upper()}-"
            f"{str(i).zfill(3)}"
        )
    })


manufacturing = pd.DataFrame(
    manufacturing_rows
)


manufacturing.to_csv(
    OUTPUT_DIR / "manufacturing.csv",
    index=False
)


# ============================================================
# 4. LOGISTICS DATASET
# ============================================================
# Source system: Logistics
# Grain: One row per shipment
# Rows: 40
#
# Controlled cascading relationship:
#
# Procurement 50
#       ↓
# Manufacturing 45
#       ↓
# Logistics 40
#
# Logistics only references 40 of the 45 manufacturing orders.
# ============================================================

logistics_rows = []


# Select exactly 40 manufacturing records
logistics_records = random.sample(
    manufacturing_rows,
    40
)


for i, mfg_record in enumerate(
    logistics_records,
    start=1
):

    production_date = mfg_record["production_date"]

    # Shipment normally happens after production.
    shipping_delay = random.randint(1, 5)

    ship_date = (
        production_date +
        timedelta(days=shipping_delay)
    )

    # Actual transit time
    transit_days = random.randint(2, 10)

    delivery_date = (
        ship_date +
        timedelta(days=transit_days)
    )

    # Promised delivery is deliberately based around
    # the expected delivery date.
    promised_offset = random.randint(-2, 2)

    promised_delivery_date = (
        delivery_date +
        timedelta(days=promised_offset)
    )

    logistics_rows.append({
        "shipment_id": f"SHIP-{str(i).zfill(3)}",

        # Different column name for same business key
        "po_number": mfg_record["order_ref"],

        "ship_date": ship_date,

        "delivery_date": delivery_date,

        "promised_delivery_date": promised_delivery_date,

        "carrier": random_choice([
            "Carrier A",
            "Carrier B",
            "Carrier C"
        ]),

        "cost_inr": random.randint(
            5000,
            50000
        )
    })


logistics = pd.DataFrame(
    logistics_rows
)


# ============================================================
# 5. INJECT INTENTIONAL BAD RECORDS
# ============================================================
# We modify 5 EXISTING logistics/manufacturing records.
#
# Total logistics rows remain exactly 40.
# Total manufacturing rows remain exactly 45.
#
# Each failure represents a different data-quality problem.
# ============================================================


# ------------------------------------------------------------
# BAD RECORD 1
# delivery_date < ship_date
# ------------------------------------------------------------

logistics.loc[0, "delivery_date"] = (
    logistics.loc[0, "ship_date"] -
    timedelta(days=1)
)


# ------------------------------------------------------------
# BAD RECORD 2
# ship_date < production_date
#
# This requires the corresponding manufacturing production
# date to be read before modifying the logistics record.
# ------------------------------------------------------------

bad_po_2 = logistics.loc[1, "po_number"]


production_date_2 = manufacturing.loc[
    manufacturing["order_ref"] == bad_po_2,
    "production_date"
].iloc[0]


logistics.loc[1, "ship_date"] = (
    production_date_2 -
    timedelta(days=1)
)


# ------------------------------------------------------------
# BAD RECORD 3
# delivery_date < order_date
# ------------------------------------------------------------

bad_po_3 = logistics.loc[2, "po_number"]


order_date_3 = procurement.loc[
    procurement["order_id"] == bad_po_3,
    "order_date"
].iloc[0]


logistics.loc[2, "delivery_date"] = (
    order_date_3 -
    timedelta(days=1)
)


# ------------------------------------------------------------
# BAD RECORD 4
# promised_delivery_date < order_date
# ------------------------------------------------------------

bad_po_4 = logistics.loc[3, "po_number"]


order_date_4 = procurement.loc[
    procurement["order_id"] == bad_po_4,
    "order_date"
].iloc[0]


logistics.loc[3, "promised_delivery_date"] = (
    order_date_4 -
    timedelta(days=2)
)


# ------------------------------------------------------------
# BAD RECORD 5
# production_date < order_date
#
# This modifies Manufacturing, just like the original script.
# ------------------------------------------------------------

bad_po_5 = logistics.loc[4, "po_number"]


order_date_5 = procurement.loc[
    procurement["order_id"] == bad_po_5,
    "order_date"
].iloc[0]


manufacturing.loc[
    manufacturing["order_ref"] == bad_po_5,
    "production_date"
] = (
    order_date_5 -
    timedelta(days=2)
)


# ============================================================
# SAVE MANUFACTURING + LOGISTICS AFTER BAD RECORD INJECTION
# ============================================================

manufacturing.to_csv(
    OUTPUT_DIR / "manufacturing.csv",
    index=False
)


logistics.to_csv(
    OUTPUT_DIR / "logistics.csv",
    index=False
)


# ============================================================
# 6. CURRENCY RATE DATASET
# ============================================================
# Source:
# USD → INR
#
# Date range:
# 2026-01-01 → 2026-03-31
#
# One row per day.
#
# This is synthetic reference data, NOT real historical data.
# ============================================================

currency_rows = []


current_date = START_DATE

base_rate = 86.50


while current_date <= END_DATE:

    # Small daily movement around base rate
    rate = round(
        base_rate +
        random.uniform(-1.5, 1.5),
        4
    )

    currency_rows.append({
        "rate_date": current_date,
        "from_currency": "USD",
        "to_currency": "INR",
        "exchange_rate": rate
    })

    current_date += timedelta(days=1)


currency_rates = pd.DataFrame(
    currency_rows
)


currency_rates.to_csv(
    OUTPUT_DIR / "currency_rates.csv",
    index=False
)


# ============================================================
# 7. VALIDATION / DATA PROFILE
# ============================================================

print("=" * 60)
print("SUPPLY CHAIN DATA GENERATION COMPLETE")
print("=" * 60)


print("\nROW COUNTS")
print("-" * 60)

print(f"Vendor Master : {len(vendor_master)}")
print(f"Procurement   : {len(procurement)}")
print(f"Manufacturing : {len(manufacturing)}")
print(f"Logistics     : {len(logistics)}")
print(f"Currency      : {len(currency_rates)}")


print("\nVENDOR DISTRIBUTION")
print("-" * 60)

vendor_distribution = (
    procurement
    .groupby("vendor_id")
    .size()
    .sort_index()
)

for vendor_id, count in vendor_distribution.items():

    vendor_name = vendors_by_id[
        vendor_id
    ]["vendor_name"]

    print(
        f"{vendor_id} ({vendor_name}): {count}"
    )


print("\nRELATIONSHIP CHECKS")
print("-" * 60)

procurement_ids = set(
    procurement["order_id"]
)

manufacturing_ids = set(
    manufacturing["order_ref"]
)

logistics_ids = set(
    logistics["po_number"]
)


print(
    "Manufacturing → Procurement:",
    len(
        manufacturing_ids.intersection(
            procurement_ids
        )
    ),
    "/",
    len(manufacturing_ids)
)


print(
    "Logistics → Procurement:",
    len(
        logistics_ids.intersection(
            procurement_ids
        )
    ),
    "/",
    len(logistics_ids)
)


print(
    "Manufacturing orders without Logistics:",
    len(
        manufacturing_ids -
        logistics_ids
    )
)


print(
    "Procurement orders without Manufacturing:",
    len(
        procurement_ids -
        manufacturing_ids
    )
)


print(
    "Procurement orders without Logistics:",
    len(
        procurement_ids -
        logistics_ids
    )
)


print("\nVENDOR REFERENTIAL INTEGRITY")
print("-" * 60)

procurement_vendor_ids = set(
    procurement["vendor_id"]
)

master_vendor_ids = set(
    vendor_master["vendor_id"]
)

invalid_vendor_ids = (
    procurement_vendor_ids -
    master_vendor_ids
)

print(
    "Invalid vendor IDs:",
    len(invalid_vendor_ids)
)


print("\nFILES CREATED")
print("-" * 60)

print(OUTPUT_DIR / "vendor_master.csv")
print(OUTPUT_DIR / "procurement.csv")
print(OUTPUT_DIR / "manufacturing.csv")
print(OUTPUT_DIR / "logistics.csv")
print(OUTPUT_DIR / "currency_rates.csv")


print("\n" + "=" * 60)
print("✅ Data generation finished.")
print("=" * 60)