# Schema-drift demo for a Fabric medallion architecture

Synthetic claims only. No PHI. Nothing here is a Microsoft product feature.

Download everything as a zip: [schema-drift-demo.zip](./schema-drift-demo.zip)

## The point

Schema drift is not a single Fabric feature. Bronze preserves the source data. In Silver, many organizations implement schema validation against a versioned data contract and stop or quarantine data when unexpected changes are detected. Gold and the semantic model change only when those contract changes are intentionally released. Git, deployment pipelines, and lineage help us understand what changed, who changed it, and what downstream assets could be affected, but they don't automatically inspect last night's file for schema drift.

A contract is the schema you agreed to. The file is what arrived. The gate compares the two. The last successful load never becomes the contract.

## What gets created

| Item | Role |
|---|---|
| `lh_schema_drift` | Lakehouse. Bronze files land in `Files/incoming`. |
| `NB_Schema_Gate` | The control. Compares the observed schema to the active Silver contract, writes an audit event, then promotes or quarantines. |
| `NB_Gold_Release` | Projects a promoted batch onto the Gold contract. Extra Silver columns are not released. |
| `PL_Schema_Gate` | Runs the gate, then the Gold release only if the gate succeeds. |

Control tables created on first run: `schema_contracts`, `schema_audit_events`, `quarantine_claims`, plus `silver_claims` and `gold_claims`.

## Run it locally first

No Fabric tenant needed. This is the rehearsal and the fallback.

```powershell
python -m unittest tests.test_schema_gate
python run_demo.py
```

## Publish to your own Fabric tenant

Requires the Azure CLI and permission to create a workspace on a Fabric capacity.

```powershell
az login
python deploy_fabric.py --capacity-id <your-fabric-capacity-guid>
```

Find your capacity id:

```powershell
az rest --method get --url "https://api.fabric.microsoft.com/v1/capacities" --resource https://api.fabric.microsoft.com
```

Optional: `--workspace-name <name>` to change the default `schema-drift-demo`. Re-running updates the items in place.

## Walk through

1. Open `NB_Schema_Gate`. Leave the defaults (`batch_name = all`, `fail_on_quarantine = false`, `write_gold = true`). Run all cells.
   - `batch-clean` matches contract v1 and is allowed.
   - `batch-added` is quarantined because `place_of_service` is not in the contract.
   - `batch-missing` is quarantined because the required `claim_id` is gone. A missing field plus a new field is not treated as a rename.
   - `batch-type-change` is quarantined because `billed_amount` arrived as text. No automatic cast.
   - A steward then inserts Silver contract v2. Version 1 is not overwritten. The same file is replayed and allowed.
   - Gold still does not contain `place_of_service`. Approving Silver is not a Gold release.
2. Open `schema_audit_events`. Field names, types, batch id, decision, and contract version. No column values.
3. Run `PL_Schema_Gate` with the pipeline parameter `batch_name = batch-missing`. The gate activity fails and `NB_Gold_Release` is skipped.

## Reset for a clean run

Drop the tables, then run `NB_Schema_Gate` once with the defaults. Leave `Files/incoming` alone.

```sql
DROP TABLE IF EXISTS schema_contracts;
DROP TABLE IF EXISTS schema_audit_events;
DROP TABLE IF EXISTS quarantine_claims;
DROP TABLE IF EXISTS silver_claims;
DROP TABLE IF EXISTS gold_claims;
```

## Adapting it to another feed

The gate is dataset-agnostic. For an HR feed instead of claims, change the contract rows and the required fields. The decision logic does not change.

Two things the contract does not cover, and should stay separate:

- File-level checks such as a missing user-defined key or a duplicate checksum. Those belong before the schema compare, and they are the right reason to stop a file in Bronze.
- Merge behavior. Daily delta, rolling two-week, and full snapshot files can share one schema and still need append, upsert, or snapshot-overwrite logic. That is a separate Silver step.

Architecture: [ARCHITECTURE.md](./ARCHITECTURE.md).
