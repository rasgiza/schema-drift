"""Synthetic claims batches. No names, dates of birth, member ids, or diagnoses."""

from __future__ import annotations

import csv
import json
from pathlib import Path

CLEAN_HEADER = ["claim_id", "payer_id", "service_date", "billed_amount", "claim_status"]
CLEAN_ROWS = [
    ["CLM-1001", "PYR-01", "2026-01-15", "120.50", "paid"],
    ["CLM-1002", "PYR-02", "2026-01-16", "80.00", "submitted"],
]
CLEAN_TYPES = [
    {"name": "claim_id", "data_type": "string"},
    {"name": "payer_id", "data_type": "string"},
    {"name": "service_date", "data_type": "date"},
    {"name": "billed_amount", "data_type": "decimal"},
    {"name": "claim_status", "data_type": "string"},
]


def write_batch(
    directory: Path,
    name: str,
    header: list[str],
    rows: list[list[str]],
    observed: list[dict[str, str]],
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    (directory / f"{name}.schema.json").write_text(json.dumps(observed, indent=2), encoding="utf-8")
    return path


def materialize(directory: Path) -> dict[str, Path]:
    added_header = CLEAN_HEADER + ["place_of_service"]
    added_rows = [row + ["11"] for row in CLEAN_ROWS]
    added_schema = CLEAN_TYPES + [{"name": "place_of_service", "data_type": "string"}]
    missing_header = [name for name in CLEAN_HEADER if name != "claim_id"]
    missing_rows = [row[1:] for row in CLEAN_ROWS]
    missing_schema = [field for field in CLEAN_TYPES if field["name"] != "claim_id"]
    type_schema = [
        field if field["name"] != "billed_amount" else {"name": "billed_amount", "data_type": "string"}
        for field in CLEAN_TYPES
    ]
    type_rows = [
        ["CLM-1001", "PYR-01", "2026-01-15", "120.50 USD", "paid"],
        ["CLM-1002", "PYR-02", "2026-01-16", "80.00 USD", "submitted"],
    ]
    return {
        "clean": write_batch(directory, "batch-clean", CLEAN_HEADER, CLEAN_ROWS, CLEAN_TYPES),
        "added": write_batch(directory, "batch-added", added_header, added_rows, added_schema),
        "missing": write_batch(directory, "batch-missing", missing_header, missing_rows, missing_schema),
        "type_change": write_batch(directory, "batch-type-change", CLEAN_HEADER, type_rows, type_schema),
    }
