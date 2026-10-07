"""Approved claims contracts. Synthetic column names and types only."""

from __future__ import annotations

from schema_gate import FieldContract, SchemaContract

SOURCE_FIELDS_V1 = (
    FieldContract("claim_id", "string", required=True),
    FieldContract("payer_id", "string", required=True),
    FieldContract("service_date", "date", required=True),
    FieldContract("billed_amount", "decimal", required=True),
    FieldContract("claim_status", "string", required=True),
)

CLEAN_TYPES = {
    "claim_id": "string",
    "payer_id": "string",
    "service_date": "date",
    "billed_amount": "decimal",
    "claim_status": "string",
    "place_of_service": "string",
}


def silver_v1() -> SchemaContract:
    return SchemaContract(
        dataset="claims",
        layer="silver",
        version=1,
        fields=SOURCE_FIELDS_V1,
        allowed_changes=(),
        owner="revenue-data-steward",
        approved_at="2026-09-01T00:00:00+00:00",
    )


def silver_v2() -> SchemaContract:
    """Approve one additive column. Version 1 is left unchanged."""
    return SchemaContract(
        dataset="claims",
        layer="silver",
        version=2,
        fields=SOURCE_FIELDS_V1 + (FieldContract("place_of_service", "string", required=False),),
        allowed_changes=("additive:place_of_service",),
        owner="revenue-data-steward",
        approved_at="2026-09-30T18:00:00+00:00",
    )


def gold_v1() -> SchemaContract:
    """Gold stays on the released columns even after Silver v2 is approved."""
    return SchemaContract(
        dataset="claims",
        layer="gold",
        version=1,
        fields=SOURCE_FIELDS_V1,
        allowed_changes=(),
        owner="finance-analytics",
        approved_at="2026-09-01T00:00:00+00:00",
    )
