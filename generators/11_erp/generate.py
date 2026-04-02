"""
generators/11_erp/generate.py — Staff DE Journey: Source 11

Simulates ERP system exports for finance ledger, purchase orders, invoices.
Generates both JSON and XML formats — real ERPs export both.

Why XML matters:
  - SAP, Oracle, Microsoft Dynamics all export XML by default
  - XML parsing in Spark requires spark-xml library (not built-in)
  - Nested XML with attributes is harder than nested JSON
  - Many enterprises have ERP exports as their only finance data source

Real ERP export pattern:
  1. ERP system runs nightly batch export (2am)
  2. Drops files to network share / SFTP / S3
  3. Airflow polls S3 for new files
  4. PySpark reads XML with spark-xml → Bronze table
  5. Silver layer normalises ledger codes, currencies, cost centres

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps backdated, nightly export pattern
    Rule 4  — invoice status reflects age
    Rule 5  — idempotent (export_date in filename)
    Rule 6  — stream: 1 file/day (~nightly)
    Rule 7  — dirty data in both JSON and XML
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/erp_raw
    python generate.py --mode burst --dirty --output-dir /tmp/erp_raw
    python generate.py --mode stream --output-dir /tmp/erp_raw
"""

import os
import json
import time
import random
import logging
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from decimal import Decimal

from faker import Faker
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

fake = Faker("en_GB")
Faker.seed(42)

STREAM_SLEEP = 86400  # Once per day (nightly export)

LEDGER_CODES = [
    "4000-SALES-UK", "4001-SALES-EU", "4002-SALES-INT",
    "5000-COGS",     "5001-SHIPPING", "5002-RETURNS",
    "6000-OPEX",     "6001-MARKETING","6002-TECH",
    "7000-TAX-VAT",  "7001-TAX-CORP",
]

COST_CENTRES = ["CC-LONDON", "CC-MANC", "CC-BRUM", "CC-TECH", "CC-OPS"]
CURRENCIES   = ["GBP", "EUR", "USD"]

INVOICE_STATUSES = ["paid", "pending", "overdue", "cancelled", "disputed"]

SUPPLIERS = [
    {"id": "SUP001", "name": "TechSupply Ltd",    "vat": "GB123456789"},
    {"id": "SUP002", "name": "GlobalParts Co",     "vat": "GB987654321"},
    {"id": "SUP003", "name": "PremiumGoods GmbH",  "vat": "DE123456789"},
]


# ─────────────────────────────────────────────────────────────
# RULE 3 + 4 — TIMESTAMPS AND STATUS
# ─────────────────────────────────────────────────────────────

def backdate(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)

def invoice_status_for_age(invoice_date: datetime) -> str:
    age_days = (datetime.now(timezone.utc) - invoice_date).days
    if age_days < 1:
        return "pending"
    elif age_days < 30:
        return random.choices(
            ["pending", "paid"], weights=[0.4, 0.6]
        )[0]
    elif age_days < 60:
        return random.choices(
            ["paid", "overdue", "pending"], weights=[0.7, 0.2, 0.1]
        )[0]
    else:
        return random.choices(
            ["paid", "overdue", "disputed", "cancelled"],
            weights=[0.80, 0.10, 0.05, 0.05]
        )[0]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def dirty_amount(amount, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice([None, "", "N/A", -999.99])
    return amount

def dirty_vat(vat, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice(["", "INVALID", None, "GB000000000"])
    return vat

def maybe_null(value, dirty, base=0.05, elev=0.20):
    if random.random() < dr(base, dirty, elev):
        return None
    return value


# ─────────────────────────────────────────────────────────────
# JSON FORMAT — Ledger entries
# ─────────────────────────────────────────────────────────────

def build_ledger_entry(entry_date: datetime, dirty: bool = False) -> dict:
    amount = round(random.uniform(10.0, 50000.0), 2)
    return {
        "entry_id":      f"JE-{random.randint(100000, 999999)}",
        "entry_date":    entry_date.strftime("%Y-%m-%d"),
        "posting_date":  (entry_date + timedelta(days=random.randint(0, 2))).strftime("%Y-%m-%d"),
        "ledger_code":   maybe_null(random.choice(LEDGER_CODES), dirty),
        "cost_centre":   maybe_null(random.choice(COST_CENTRES), dirty),
        "description":   maybe_null(fake.bs(), dirty),
        "debit_amount":  dirty_amount(amount, dirty),
        "credit_amount": dirty_amount(0.0, dirty),
        "currency":      random.choice(CURRENCIES),
        "fx_rate":       round(random.uniform(0.85, 1.35), 4) if random.random() > 0.7 else 1.0,
        "period":        entry_date.strftime("%Y-%m"),
        "created_by":    maybe_null(fake.user_name(), dirty),
        "approved_by":   maybe_null(fake.user_name(), dirty),
        "reference":     maybe_null(f"REF-{random.randint(10000, 99999)}", dirty),
    }


def generate_json_export(export_date: datetime, dirty: bool = False) -> bytes:
    """Generate a JSON ledger export — typically 50-200 entries per day."""
    num_entries = random.randint(50, 200)
    entries = []

    for _ in range(num_entries):
        offset     = random.uniform(0, 23)
        entry_date = export_date - timedelta(hours=offset)
        entries.append(build_ledger_entry(entry_date, dirty))

    # Dirty mode: 3% duplicate entries
    if dirty:
        dupes = random.sample(entries, int(len(entries) * 0.03))
        entries.extend(dupes)

    payload = {
        "export_type":    "general_ledger",
        "export_date":    export_date.strftime("%Y-%m-%d"),
        "system":         "ERP-ORACLE-CLOUD",
        "company_code":   "EC-UK-001",
        "total_entries":  len(entries),
        "entries":        entries,
    }

    return json.dumps(payload, default=str, indent=2).encode("utf-8")


# ─────────────────────────────────────────────────────────────
# XML FORMAT — Purchase orders and invoices
# XML is harder — attributes, namespaces, nested elements
# ─────────────────────────────────────────────────────────────

def build_invoice_xml(invoice_date: datetime, dirty: bool = False) -> ET.Element:
    """
    Build a single invoice XML element.
    Uses attributes AND child elements — both patterns appear in real ERP XML.
    """
    inv_id  = f"INV-{random.randint(100000, 999999)}"
    status  = invoice_status_for_age(invoice_date)
    net_amt = round(random.uniform(50.0, 10000.0), 2)
    vat_amt = round(net_amt * 0.20, 2)
    gross   = round(net_amt + vat_amt, 2)
    supplier = random.choice(SUPPLIERS)

    # Invoice element with attributes (common in ERP XML)
    inv = ET.Element("Invoice", attrib={
        "id":       inv_id,
        "status":   status,
        "currency": random.choice(CURRENCIES),
    })

    # Supplier sub-element
    sup_el = ET.SubElement(inv, "Supplier")
    ET.SubElement(sup_el, "SupplierID").text  = supplier["id"]
    ET.SubElement(sup_el, "SupplierName").text = maybe_null(supplier["name"], dirty) or ""
    ET.SubElement(sup_el, "VATNumber").text    = dirty_vat(supplier["vat"], dirty) or ""

    # Dates
    dates_el = ET.SubElement(inv, "Dates")
    ET.SubElement(dates_el, "InvoiceDate").text = invoice_date.strftime("%Y-%m-%d")
    ET.SubElement(dates_el, "DueDate").text     = (
        invoice_date + timedelta(days=30)).strftime("%Y-%m-%d")
    ET.SubElement(dates_el, "PaidDate").text    = (
        (invoice_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
        if status == "paid" else ""
    )

    # Amounts
    amt_el = ET.SubElement(inv, "Amounts")
    ET.SubElement(amt_el, "NetAmount").text   = str(dirty_amount(net_amt, dirty) or "")
    ET.SubElement(amt_el, "VATAmount").text   = str(dirty_amount(vat_amt, dirty) or "")
    ET.SubElement(amt_el, "GrossAmount").text = str(dirty_amount(gross, dirty) or "")
    ET.SubElement(amt_el, "VATRate").text     = "20.00"

    # Line items
    lines_el   = ET.SubElement(inv, "LineItems")
    num_lines  = random.randint(1, 5)
    for i in range(num_lines):
        line = ET.SubElement(lines_el, "LineItem", attrib={"lineNumber": str(i + 1)})
        ET.SubElement(line, "Description").text  = maybe_null(fake.bs(), dirty) or ""
        ET.SubElement(line, "Quantity").text      = str(random.randint(1, 100))
        ET.SubElement(line, "UnitPrice").text     = str(round(net_amt / num_lines, 2))
        ET.SubElement(line, "LedgerCode").text    = maybe_null(
            random.choice(LEDGER_CODES), dirty) or ""
        ET.SubElement(line, "CostCentre").text    = maybe_null(
            random.choice(COST_CENTRES), dirty) or ""

    return inv


def generate_xml_export(export_date: datetime, dirty: bool = False) -> bytes:
    """Generate an XML invoice export — 10-50 invoices per file."""
    root = ET.Element("ERPExport", attrib={
        "xmlns":       "http://erp.ecommerce.internal/v1",
        "exportDate":  export_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "system":      "ERP-SAP-S4HANA",
        "companyCode": "EC-UK-001",
        "exportType":  "AP_INVOICES",
    })

    header = ET.SubElement(root, "ExportHeader")
    ET.SubElement(header, "GeneratedAt").text = export_date.isoformat()
    ET.SubElement(header, "GeneratedBy").text = "erp-batch-export-service"
    ET.SubElement(header, "RecordCount").text = "0"  # Updated below

    invoices_el  = ET.SubElement(root, "Invoices")
    num_invoices = random.randint(10, 50)

    for _ in range(num_invoices):
        offset       = random.uniform(0, 23)
        invoice_date = export_date - timedelta(hours=offset)
        inv_el       = build_invoice_xml(invoice_date, dirty)
        invoices_el.append(inv_el)

    # Update record count
    header.find("RecordCount").text = str(num_invoices)

    # Dirty mode: malformed XML (truncate)
    xml_str = ET.tostring(root, encoding="unicode", xml_declaration=False)
    if dirty and random.random() < 0.02:
        # Truncate the XML — simulates interrupted file write
        xml_str = xml_str[:int(len(xml_str) * 0.8)]
        log.warning("  Generated truncated XML (dirty mode)")

    full_xml = f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'
    return full_xml.encode("utf-8")


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0    = time.time()
    stats = {"files": 0, "json_files": 0, "xml_files": 0}

    for day_offset in range(days):
        # ERP exports run at 2am nightly
        export_date = (datetime.now(timezone.utc) - timedelta(days=day_offset)).replace(
            hour=2, minute=random.randint(0, 30), second=0, microsecond=0
        )

        date_path = output_dir / export_date.strftime("%Y/%m/%d")
        date_path.mkdir(parents=True, exist_ok=True)

        # JSON ledger export
        json_content = generate_json_export(export_date, dirty)
        json_filename = f"ledger_{export_date.strftime('%Y%m%d_%H%M%S')}.json"
        (date_path / json_filename).write_bytes(json_content)
        stats["files"]      += 1
        stats["json_files"] += 1
        log.info(f"  Written: {json_filename} ({len(json_content):,} bytes)")

        # XML invoice export
        xml_content  = generate_xml_export(export_date, dirty)
        xml_filename = f"invoices_{export_date.strftime('%Y%m%d_%H%M%S')}.xml"
        (date_path / xml_filename).write_bytes(xml_content)
        stats["files"]     += 1
        stats["xml_files"] += 1
        log.info(f"  Written: {xml_filename} ({len(xml_content):,} bytes)")

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE
# ─────────────────────────────────────────────────────────────

def run_stream(output_dir: Path, dirty: bool = False):
    log.info(f"STREAM MODE | 1 export/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"files": 0}, 0

    try:
        while True:
            i  += 1
            now = datetime.now(timezone.utc)

            date_path = output_dir / now.strftime("%Y/%m/%d")
            date_path.mkdir(parents=True, exist_ok=True)

            # Alternate JSON and XML
            if i % 2 == 0:
                content  = generate_json_export(now, dirty)
                filename = f"ledger_{now.strftime('%Y%m%d_%H%M%S')}.json"
            else:
                content  = generate_xml_export(now, dirty)
                filename = f"invoices_{now.strftime('%Y%m%d_%H%M%S')}.xml"

            (date_path / filename).write_bytes(content)
            stats["files"] += 1
            log.info(f"Exported: {filename} ({len(content):,} bytes)")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 11 — ERP Export generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("ERP_OUTPUT_DIR", "/tmp/erp_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 11 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()
