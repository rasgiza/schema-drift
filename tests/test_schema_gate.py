"""Unit tests for the schema gate. No Fabric tenant required."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from batches import materialize  # noqa: E402
from contracts import CLEAN_TYPES, gold_v1, silver_v1, silver_v2  # noqa: E402
from schema_gate import compare_schemas, decide, evaluate_batch, read_observed_schema, DemoLake  # noqa: E402


class SchemaGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.batches = materialize(self.root / "incoming")
        self.lake = DemoLake(self.root / "lake")
        self.lake.reset()
        self.lake.add_contract(silver_v1())
        self.lake.add_contract(gold_v1())

    def test_clean_batch_promotes_and_records_metadata_only(self) -> None:
        result = evaluate_batch(
            self.lake,
            csv_path=self.batches["clean"],
            batch_id="batch-clean",
            run_id="run-1",
            type_overrides=CLEAN_TYPES,
        )
        self.assertEqual(result["decision"], "allow")
        self.assertEqual(len(self.lake.layer_rows("silver")), 2)
        self.assertEqual(len(self.lake.layer_rows("gold")), 2)
        event = self.lake.read_jsonl(self.lake.events_path)[0]
        self.assertNotIn("120.50", json.dumps(event))
        self.assertNotIn("CLM-1001", json.dumps(event))

    def test_new_column_is_recorded_and_not_promoted(self) -> None:
        evaluate_batch(
            self.lake,
            csv_path=self.batches["clean"],
            batch_id="batch-clean",
            run_id="run-1",
            type_overrides=CLEAN_TYPES,
        )
        result = evaluate_batch(
            self.lake,
            csv_path=self.batches["added"],
            batch_id="batch-added",
            run_id="run-2",
            type_overrides=CLEAN_TYPES,
        )
        self.assertEqual(result["decision"], "quarantine")
        self.assertEqual(result["event"]["added_fields"], ["place_of_service"])
        self.assertTrue((self.lake.root / "bronze" / "claims" / "batch-added" / "claims.csv").exists())
        self.assertEqual(len(self.lake.layer_rows("silver")), 2)
        self.assertEqual(len(self.lake.layer_rows("gold")), 2)

    def test_missing_required_column_quarantines(self) -> None:
        result = evaluate_batch(
            self.lake,
            csv_path=self.batches["missing"],
            batch_id="batch-missing",
            run_id="run-3",
            type_overrides=CLEAN_TYPES,
        )
        self.assertEqual(result["decision"], "quarantine")
        self.assertIn("claim_id", result["event"]["missing_fields"])
        self.assertEqual(self.lake.layer_rows("silver"), [])

    def test_type_change_is_not_an_automatic_cast(self) -> None:
        observed = read_observed_schema(
            self.batches["type_change"],
            {**CLEAN_TYPES, "billed_amount": "string"},
        )
        diff = compare_schemas(silver_v1().fields, observed)
        self.assertEqual(decide(diff, silver_v1()), "quarantine")
        self.assertEqual(diff.changed[0]["name"], "billed_amount")

    def test_approval_inserts_v2_and_replay_does_not_release_gold(self) -> None:
        evaluate_batch(
            self.lake,
            csv_path=self.batches["added"],
            batch_id="batch-added",
            run_id="run-2",
            type_overrides=CLEAN_TYPES,
        )
        self.lake.add_contract(silver_v2())
        versions = [
            row["contract_version"]
            for row in self.lake.read_jsonl(self.lake.contracts_path)
            if row["layer"] == "silver"
        ]
        self.assertEqual(versions, [1, 2])
        replay = evaluate_batch(
            self.lake,
            csv_path=self.batches["added"],
            batch_id="batch-added-replay",
            run_id="run-5",
            type_overrides=CLEAN_TYPES,
        )
        self.assertEqual(replay["decision"], "allow")
        self.assertIn("place_of_service", self.lake.layer_rows("silver")[0])
        self.assertNotIn("place_of_service", self.lake.layer_rows("gold")[0])
        self.assertEqual(self.lake.active_contract("claims", "gold").version, 1)

    def test_rename_is_not_inferred(self) -> None:
        observed = [
            {"name": "claim_number", "data_type": "string"},
            {"name": "payer_id", "data_type": "string"},
            {"name": "service_date", "data_type": "date"},
            {"name": "billed_amount", "data_type": "decimal"},
            {"name": "claim_status", "data_type": "string"},
        ]
        diff = compare_schemas(silver_v1().fields, observed)
        self.assertIn("claim_id", diff.missing)
        self.assertIn("claim_number", diff.added)
        self.assertEqual(decide(diff, silver_v1()), "quarantine")

    def test_notebook_does_not_enable_merge_schema(self) -> None:
        notebook = (ROOT / "workspace" / "NB_Schema_Gate.Notebook" / "notebook-content.py").read_text(encoding="utf-8")
        self.assertNotIn('.option("mergeSchema"', notebook)
        self.assertIn("schema_audit_events", notebook)
        self.assertIn("ALTER TABLE", notebook)
        self.assertIn("raise ValueError", notebook)


if __name__ == "__main__":
    unittest.main()
