import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

random.seed(2026)

PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "data"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 6, 30)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def random_date(start_date, end_date):
    """Generate a reproducible random date."""
    days = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, days))


def random_choice(values):
    """Select a reproducible random value."""
    return random.choice(values)


# ============================================================
# 1. VENDOR MASTER
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
    },
    {
        "vendor_id": "V006",
        "vendor_name": "Vendor F",
        "country": "Japan",
        "vendor_category": "Electronics",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V007",
        "vendor_name": "Vendor G",
        "country": "Singapore",
        "vendor_category": "Components",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V008",
        "vendor_name": "Vendor H",
        "country": "India",
        "vendor_category": "Packaging",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V009",
        "vendor_name": "Vendor I",
        "country": "USA",
        "vendor_category": "Raw Materials",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V010",
        "vendor_name": "Vendor J",
        "country": "Germany",
        "vendor_category": "Components",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V011",
        "vendor_name": "Vendor K",
        "country": "South Korea",
        "vendor_category": "Electronics",
        "vendor_status": "ACTIVE"
    },
    {
        "vendor_id": "V012",
        "vendor_name": "Vendor L",
        "country": "India",
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
# 2. PROCUREMENT
# ============================================================

procurement_rows = []

vendor_ids = [vendor["vendor_id"] for vendor in vendor_master_rows]

vendors_by_id = {
    row["vendor_id"]: row
    for row in vendor_master_rows
}

# 75 purchase orders distributed across 12 vendors.
vendor_assignments = []

for vendor_id in vendor_ids:
    vendor_assignments.extend([vendor_id] * 6)

# 12 vendors × 6 = 72.
# Add 3 additional orders.
vendor_assignments.extend(["V001", "V004", "V008"])

random.shuffle(vendor_assignments)

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

for i in range(1, 76):

    vendor_id = vendor_assignments[i - 1]
    vendor = vendors_by_id[vendor_id]

    order_date = random_date(
        START_DATE,
        END_DATE - timedelta(days=20)
    )

    quantity = random.randint(10, 750)

    unit_cost_usd = round(
        random.uniform(5, 150),
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
# 3. MANUFACTURING
# ============================================================

manufacturing_rows = []

# 65 of 75 POs receive manufacturing records.
manufacturing_orders = random.sample(
    list(procurement["order_id"]),
    65
)

for i, po_number in enumerate(
    manufacturing_orders,
    start=1
):

    procurement_record = procurement[
        procurement["order_id"] == po_number
    ].iloc[0]

    order_date = procurement_record["order_date"]

    production_delay = random.randint(3, 12)

    production_date = (
        order_date +
        timedelta(days=production_delay)
    )

    manufacturing_rows.append({
        "mfg_order_id": f"MO-{str(i).zfill(3)}",
        "order_ref": po_number,
        "production_date": production_date,
        "materials_used_kg": random.randint(50, 750),
        "production_hours": random.randint(8, 72),
        "batch_id": (
            f"BATCH-{production_date.strftime('%b').upper()}-"
            f"{str(i).zfill(3)}"
        )
    })


manufacturing = pd.DataFrame(
    manufacturing_rows
)


# ============================================================
# 4. LOGISTICS
# ============================================================

logistics_rows = []

# 55 of the 65 manufactured POs receive shipments.
logistics_records = random.sample(
    manufacturing_rows,
    55
)

carriers = [
    "Carrier A",
    "Carrier B",
    "Carrier C",
    "Carrier D",
    "Carrier E"
]

for i, mfg_record in enumerate(
    logistics_records,
    start=1
):

    production_date = mfg_record["production_date"]

    shipping_delay = random.randint(1, 6)

    ship_date = (
        production_date +
        timedelta(days=shipping_delay)
    )

    transit_days = random.randint(2, 12)

    delivery_date = (
        ship_date +
        timedelta(days=transit_days)
    )

    promised_offset = random.randint(-3, 3)

    promised_delivery_date = (
        delivery_date +
        timedelta(days=promised_offset)
    )

    logistics_rows.append({
        "shipment_id": f"SHIP-{str(i).zfill(3)}",
        "po_number": mfg_record["order_ref"],
        "ship_date": ship_date,
        "delivery_date": delivery_date,
        "promised_delivery_date": promised_delivery_date,
        "carrier": random_choice(carriers),
        "cost_inr": random.randint(5000, 75000)
    })


logistics = pd.DataFrame(logistics_rows)


# ============================================================
# 5. INTENTIONAL DATA-QUALITY ISSUES
# ============================================================

# Eight bad records are introduced deliberately.
# These should be detected by the Silver SOT quality gates.


# BAD RECORD 1
# delivery_date < ship_date

logistics.loc[0, "delivery_date"] = (
    logistics.loc[0, "ship_date"] -
    timedelta(days=1)
)


# BAD RECORD 2
# delivery_date < ship_date

logistics.loc[1, "delivery_date"] = (
    logistics.loc[1, "ship_date"] -
    timedelta(days=2)
)


# BAD RECORD 3
# ship_date < production_date

bad_po_3 = logistics.loc[2, "po_number"]

production_date_3 = manufacturing.loc[
    manufacturing["order_ref"] == bad_po_3,
    "production_date"
].iloc[0]

logistics.loc[2, "ship_date"] = (
    production_date_3 -
    timedelta(days=1)
)


# BAD RECORD 4
# delivery_date < order_date

bad_po_4 = logistics.loc[3, "po_number"]

order_date_4 = procurement.loc[
    procurement["order_id"] == bad_po_4,
    "order_date"
].iloc[0]

logistics.loc[3, "delivery_date"] = (
    order_date_4 -
    timedelta(days=1)
)


# BAD RECORD 5
# promised_delivery_date < order_date

bad_po_5 = logistics.loc[4, "po_number"]

order_date_5 = procurement.loc[
    procurement["order_id"] == bad_po_5,
    "order_date"
].iloc[0]

logistics.loc[4, "promised_delivery_date"] = (
    order_date_5 -
    timedelta(days=2)
)


# BAD RECORD 6
# production_date < order_date

bad_po_6 = logistics.loc[5, "po_number"]

order_date_6 = procurement.loc[
    procurement["order_id"] == bad_po_6,
    "order_date"
].iloc[0]

manufacturing.loc[
    manufacturing["order_ref"] == bad_po_6,
    "production_date"
] = (
    order_date_6 -
    timedelta(days=2)
)


# BAD RECORD 7
# ship_date < production_date

bad_po_7 = logistics.loc[6, "po_number"]

production_date_7 = manufacturing.loc[
    manufacturing["order_ref"] == bad_po_7,
    "production_date"
].iloc[0]

logistics.loc[6, "ship_date"] = (
    production_date_7 -
    timedelta(days=2)
)


# BAD RECORD 8
# promised_delivery_date < order_date

bad_po_8 = logistics.loc[7, "po_number"]

order_date_8 = procurement.loc[
    procurement["order_id"] == bad_po_8,
    "order_date"
].iloc[0]

logistics.loc[7, "promised_delivery_date"] = (
    order_date_8 -
    timedelta(days=3)
)


# Save final manufacturing/logistics datasets.

manufacturing.to_csv(
    OUTPUT_DIR / "manufacturing.csv",
    index=False
)

logistics.to_csv(
    OUTPUT_DIR / "logistics.csv",
    index=False
)


# ============================================================
# 6. CURRENCY RATES
# ============================================================

currency_rows = []

current_date = START_DATE

base_rate = 86.50

while current_date <= END_DATE:

    rate = round(
        base_rate +
        random.uniform(-2.0, 2.0),
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
# 7. VALIDATION
# ============================================================

print("=" * 65)
print("SUPPLY CHAIN V2 DATA GENERATION COMPLETE")
print("=" * 65)

print("\nROW COUNTS")
print("-" * 65)

print(f"Vendor Master : {len(vendor_master)}")
print(f"Procurement   : {len(procurement)}")
print(f"Manufacturing : {len(manufacturing)}")
print(f"Logistics     : {len(logistics)}")
print(f"Currency      : {len(currency_rates)}")


print("\nVENDOR DISTRIBUTION")
print("-" * 65)

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
print("-" * 65)

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
print("-" * 65)

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
print("-" * 65)

print(OUTPUT_DIR / "vendor_master.csv")
print(OUTPUT_DIR / "procurement.csv")
print(OUTPUT_DIR / "manufacturing.csv")
print(OUTPUT_DIR / "logistics.csv")
print(OUTPUT_DIR / "currency_rates.csv")

print("\n" + "=" * 65)
print("V2 DATA GENERATION FINISHED")
print("=" * 65)