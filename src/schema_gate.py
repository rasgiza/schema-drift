"""Schema-drift gate for the medallion demo.

Fabric does not compare an incoming file to a versioned contract. This module
is the control a notebook or pipeline activity has to implement.

Detect, record, and approve stay separate:

- Bronze keeps the file and an observed-schema sidecar. Nothing is rejected there.
- Silver compares that observed schema to the active contract, writes an audit
  event, then either promotes the batch or quarantines it.
- Gold is a separate explicit contract. An approved Silver column is not
  released to Gold until that Gold contract version says so.

Audit rows store field names and types only. They never store column values.
"""

from __future__ import annotations

import csv
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping


@dataclass(frozen=True)
class FieldContract:
    name: str
    data_type: str
    required: bool = True


@dataclass(frozen=True)
class SchemaContract:
    dataset: str
    layer: str
    version: int
    fields: tuple[FieldContract, ...]
    allowed_changes: tuple[str, ...]
    owner: str
    approved_at: str

    def required_names(self) -> list[str]:
        return [field.name for field in self.fields if field.required]


@dataclass(frozen=True)
class SchemaDiff:
    added: tuple[str, ...]
    missing: tuple[str, ...]
    changed: tuple[dict[str, str], ...]

    @property
    def is_empty(self) -> bool:
        return not self.added and not self.missing and not self.changed


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def compare_schemas(
    expected: Iterable[FieldContract],
    observed: Iterable[Mapping[str, str]],
) -> SchemaDiff:
    """Compare top-level field names and types. Column order is not drift."""
    expected_map = {field.name: field.data_type for field in expected}
    observed_map = {field["name"]: field["data_type"] for field in observed}
    added = tuple(sorted(observed_map.keys() - expected_map.keys()))
    missing = tuple(sorted(expected_map.keys() - observed_map.keys()))
    changed = tuple(
        {
            "name": name,
            "expected": expected_map[name],
            "observed": observed_map[name],
        }
        for name in sorted(expected_map.keys() & observed_map.keys())
        if expected_map[name] != observed_map[name]
    )
    return SchemaDiff(added=added, missing=missing, changed=changed)


def decide(diff: SchemaDiff, contract: SchemaContract) -> str:
    """Any unapproved difference is quarantined.

    An additive field promotes only when this contract version lists the field
    or records an explicit ``additive:<name>`` approval. A missing field plus
    an added field is not treated as a rename.
    """
    if diff.is_empty:
        return "allow"
    if diff.missing or diff.changed:
        return "quarantine"
    approved = set(contract.allowed_changes)
    if diff.added and all(f"additive:{name}" in approved for name in diff.added):
        return "allow"
    return "quarantine"


def observed_schema_from_header(
    header: Iterable[str],
    type_overrides: Mapping[str, str] | None = None,
) -> list[dict[str, str]]:
    overrides = dict(type_overrides or {})
    return [{"name": name, "data_type": overrides.get(name, "string")} for name in header]


def read_observed_schema(csv_path: Path, type_overrides: Mapping[str, str] | None = None) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as handle:
        header = next(csv.reader(handle))
    return observed_schema_from_header(header, type_overrides)


def project_columns(rows: list[dict[str, str]], contract: SchemaContract) -> list[dict[str, str]]:
    """Writes use the contract column list, never the incoming header."""
    names = [field.name for field in contract.fields]
    projected: list[dict[str, str]] = []
    for row in rows:
        missing = [name for name in names if name not in row]
        if missing:
            raise ValueError(f"Cannot project {contract.layer} columns; missing {missing}")
        projected.append({name: row[name] for name in names})
    return projected


def contract_to_record(contract: SchemaContract) -> dict[str, object]:
    return {
        "dataset": contract.dataset,
        "layer": contract.layer,
        "contract_version": contract.version,
        "approved_schema": [
            {"name": field.name, "data_type": field.data_type, "required": field.required}
            for field in contract.fields
        ],
        "required_fields": contract.required_names(),
        "allowed_changes": list(contract.allowed_changes),
        "owner": contract.owner,
        "approved_at": contract.approved_at,
    }


def audit_event(
    *,
    dataset: str,
    layer: str,
    batch_id: str,
    run_id: str,
    contract: SchemaContract,
    observed: list[dict[str, str]],
    diff: SchemaDiff,
    decision: str,
) -> dict[str, object]:
    return {
        "event_id": str(uuid.uuid4()),
        "dataset": dataset,
        "layer": layer,
        "batch_id": batch_id,
        "run_id": run_id,
        "expected_schema": [
            {"name": field.name, "data_type": field.data_type} for field in contract.fields
        ],
        "observed_schema": observed,
        "added_fields": list(diff.added),
        "missing_fields": list(diff.missing),
        "changed_fields": list(diff.changed),
        "decision": decision,
        "contract_version": contract.version,
        "event_time": utc_now(),
    }


def assert_metadata_only(event: Mapping[str, object]) -> None:
    banned_keys = {"rows", "sample", "payload", "values", "billed_amount"}
    found = banned_keys & set(event)
    if found:
        raise ValueError(f"Audit event contains value-bearing keys: {sorted(found)}")
    blob = json.dumps(event)
    if "CLM-" in blob or "120.50" in blob:
        raise ValueError("Audit event contains synthetic claim values")


class DemoLake:
    """File stand-in for the Delta tables the Fabric notebook writes."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.contracts_path = root / "control" / "schema_contracts.jsonl"
        self.events_path = root / "control" / "schema_audit_events.jsonl"
        self.silver_path = root / "silver" / "claims.jsonl"
        self.gold_path = root / "gold" / "claims.jsonl"

    def reset(self) -> None:
        if self.root.exists():
            for path in sorted(self.root.rglob("*"), reverse=True):
                if path.is_file():
                    path.unlink()
                else:
                    path.rmdir()
        for path in (self.contracts_path, self.events_path, self.silver_path, self.gold_path):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")

    def append_jsonl(self, path: Path, record: Mapping[str, object]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")

    def read_jsonl(self, path: Path) -> list[dict]:
        if not path.exists() or path.stat().st_size == 0:
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]

    def add_contract(self, contract: SchemaContract) -> None:
        self.append_jsonl(self.contracts_path, contract_to_record(contract))

    def active_contract(self, dataset: str, layer: str) -> SchemaContract:
        matches = [
            record
            for record in self.read_jsonl(self.contracts_path)
            if record["dataset"] == dataset and record["layer"] == layer
        ]
        if not matches:
            raise KeyError(f"No contract for {dataset}.{layer}")
        current = max(matches, key=lambda record: record["contract_version"])
        return SchemaContract(
            dataset=current["dataset"],
            layer=current["layer"],
            version=current["contract_version"],
            fields=tuple(
                FieldContract(
                    name=field["name"],
                    data_type=field["data_type"],
                    required=field["required"],
                )
                for field in current["approved_schema"]
            ),
            allowed_changes=tuple(current["allowed_changes"]),
            owner=current["owner"],
            approved_at=current["approved_at"],
        )

    def land_bronze(self, batch_id: str, csv_path: Path, observed: list[dict[str, str]]) -> None:
        target = self.root / "bronze" / "claims" / batch_id
        target.mkdir(parents=True, exist_ok=True)
        (target / "claims.csv").write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")
        (target / "observed_schema.json").write_text(json.dumps(observed, indent=2), encoding="utf-8")

    def quarantine(self, batch_id: str, csv_path: Path) -> None:
        target = self.root / "quarantine" / "claims" / batch_id
        target.mkdir(parents=True, exist_ok=True)
        (target / "claims.csv").write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")

    def record_event(self, event: Mapping[str, object]) -> None:
        assert_metadata_only(event)
        self.append_jsonl(self.events_path, event)

    def write_layer(self, layer: str, rows: list[dict[str, str]], batch_id: str) -> None:
        path = self.silver_path if layer == "silver" else self.gold_path
        for row in rows:
            stamped = dict(row)
            stamped["batch_id"] = batch_id
            self.append_jsonl(path, stamped)

    def layer_rows(self, layer: str) -> list[dict]:
        path = self.silver_path if layer == "silver" else self.gold_path
        return self.read_jsonl(path)


def load_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def evaluate_batch(
    lake: DemoLake,
    *,
    csv_path: Path,
    batch_id: str,
    run_id: str,
    dataset: str = "claims",
    type_overrides: Mapping[str, str] | None = None,
    promote_gold: bool = True,
) -> dict[str, object]:
    """Land Bronze, compare, record, then promote or hold.

    The audit event is written before any Silver or Gold append. Promotion
    uses the contract column list. Delta mergeSchema is not used.
    """
    observed = read_observed_schema(csv_path, type_overrides)
    lake.land_bronze(batch_id, csv_path, observed)
    silver_contract = lake.active_contract(dataset, "silver")
    diff = compare_schemas(silver_contract.fields, observed)
    decision = decide(diff, silver_contract)
    event = audit_event(
        dataset=dataset,
        layer="silver",
        batch_id=batch_id,
        run_id=run_id,
        contract=silver_contract,
        observed=observed,
        diff=diff,
        decision=decision,
    )
    lake.record_event(event)
    if decision != "allow":
        lake.quarantine(batch_id, csv_path)
        return {"decision": decision, "event": event, "silver_written": False, "gold_written": False}

    rows = load_csv_rows(csv_path)
    lake.write_layer("silver", project_columns(rows, silver_contract), batch_id)
    gold_written = False
    if promote_gold:
        gold_contract = lake.active_contract(dataset, "gold")
        lake.write_layer("gold", project_columns(rows, gold_contract), batch_id)
        gold_written = True
    return {
        "decision": decision,
        "event": event,
        "silver_written": True,
        "gold_written": gold_written,
    }
