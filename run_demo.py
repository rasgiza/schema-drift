"""Run the schema-drift story locally before opening Fabric.

Usage:
    python run_demo.py
    python run_demo.py --as-pipeline batch-missing
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from batches import materialize  # noqa: E402
from contracts import CLEAN_TYPES, gold_v1, silver_v1, silver_v2  # noqa: E402
from schema_gate import DemoLake, evaluate_batch  # noqa: E402

STATEMENT = (
    "Schema drift is not a single Fabric feature. Bronze preserves the source data. "
    "In Silver, many organizations implement schema validation against a versioned data "
    "contract and stop or quarantine data when unexpected changes are detected. Gold and "
    "the semantic model change only when those contract changes are intentionally released. "
    "Git, deployment pipelines, and lineage help us understand what changed, who changed it, "
    "and what downstream assets could be affected, but they don't automatically inspect "
    "last night's file for schema drift."
)


def run_story(lake_root: Path, incoming: Path) -> dict[str, object]:
    batches = materialize(incoming)
    lake = DemoLake(lake_root)
    lake.reset()
    lake.add_contract(silver_v1())
    lake.add_contract(gold_v1())

    clean = evaluate_batch(
        lake, csv_path=batches["clean"], batch_id="batch-clean", run_id="demo", type_overrides=CLEAN_TYPES
    )
    added = evaluate_batch(
        lake, csv_path=batches["added"], batch_id="batch-added", run_id="demo", type_overrides=CLEAN_TYPES
    )
    missing = evaluate_batch(
        lake, csv_path=batches["missing"], batch_id="batch-missing", run_id="demo", type_overrides=CLEAN_TYPES
    )
    typed = evaluate_batch(
        lake,
        csv_path=batches["type_change"],
        batch_id="batch-type-change",
        run_id="demo",
        type_overrides={**CLEAN_TYPES, "billed_amount": "string"},
    )
    silver_before = len(lake.layer_rows("silver"))
    gold_before = len(lake.layer_rows("gold"))
    lake.add_contract(silver_v2())
    replay = evaluate_batch(
        lake,
        csv_path=batches["added"],
        batch_id="batch-added-replay",
        run_id="demo",
        type_overrides=CLEAN_TYPES,
    )
    return {
        "clean": clean["decision"],
        "added": added["decision"],
        "missing": missing["decision"],
        "type_change": typed["decision"],
        "replay": replay["decision"],
        "silver_before_approval": silver_before,
        "gold_before_approval": gold_before,
        "silver_after": lake.layer_rows("silver"),
        "gold_after": lake.layer_rows("gold"),
        "events": lake.read_jsonl(lake.events_path),
        "silver_versions": [
            row["contract_version"]
            for row in lake.read_jsonl(lake.contracts_path)
            if row["layer"] == "silver"
        ],
    }


def print_story(result: dict[str, object]) -> None:
    print(STATEMENT)
    print()
    print("Local rehearsal of the gate. The Fabric notebook implements the same decisions.")
    print(f"clean batch:        {result['clean']}")
    print(f"added column:       {result['added']}  (held; Silver and Gold unchanged)")
    print(f"missing claim_id:   {result['missing']}")
    print(f"type change:        {result['type_change']}  (no automatic cast)")
    print(f"rows before approval  silver={result['silver_before_approval']} gold={result['gold_before_approval']}")
    print(f"silver contract versions after approval insert: {result['silver_versions']}")
    print(f"replay against v2:  {result['replay']}")
    silver_cols = sorted(result["silver_after"][-1].keys())
    gold_cols = sorted(result["gold_after"][-1].keys())
    print(f"silver columns:     {silver_cols}")
    print(f"gold columns:       {gold_cols}")
    print(f"place_of_service in gold: {'place_of_service' in gold_cols}")
    print()
    print("audit events (field names and types only):")
    for event in result["events"]:
        print(
            f"  {event['batch_id']}: {event['decision']} "
            f"added={event['added_fields']} missing={event['missing_fields']} "
            f"changed={[item['name'] for item in event['changed_fields']]} "
            f"contract=v{event['contract_version']}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-pipeline", help="Evaluate one batch and exit 1 when quarantined.")
    args = parser.parse_args()
    incoming = ROOT / "data" / "incoming"
    lake_root = ROOT / "data" / "lake"
    if args.as_pipeline:
        from schema_gate import DemoLake, evaluate_batch
        from contracts import CLEAN_TYPES, gold_v1, silver_v1

        batches = materialize(incoming)
        key = args.as_pipeline.removeprefix("batch-").replace("-", "_")
        if key == "type_change":
            overrides = {**CLEAN_TYPES, "billed_amount": "string"}
        else:
            overrides = CLEAN_TYPES
        if key not in batches and args.as_pipeline not in batches:
            print(f"Unknown batch {args.as_pipeline}")
            return 2
        csv_path = batches.get(key, batches.get(args.as_pipeline))
        lake = DemoLake(lake_root)
        lake.reset()
        lake.add_contract(silver_v1())
        lake.add_contract(gold_v1())
        result = evaluate_batch(
            lake,
            csv_path=csv_path,
            batch_id=args.as_pipeline,
            run_id="pipeline",
            type_overrides=overrides,
            promote_gold=False,
        )
        print(json.dumps({"batch": args.as_pipeline, "decision": result["decision"]}, indent=2))
        return 0 if result["decision"] == "allow" else 1

    result = run_story(lake_root, incoming)
    print_story(result)
    expected = {
        "clean": "allow",
        "added": "quarantine",
        "missing": "quarantine",
        "type_change": "quarantine",
        "replay": "allow",
    }
    for name, decision in expected.items():
        if result[name] != decision:
            print(f"STORY FAILED: {name} was {result[name]}")
            return 1
    if "place_of_service" not in result["silver_after"][-1]:
        print("STORY FAILED: approved column did not land in Silver")
        return 1
    if "place_of_service" in result["gold_after"][-1]:
        print("STORY FAILED: unreleased column landed in Gold")
        return 1
    print()
    print("Local gate passed. Safe to run the same story in Fabric.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
