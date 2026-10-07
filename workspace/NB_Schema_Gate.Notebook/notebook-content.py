# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "__LAKEHOUSE_ID__",
# META       "default_lakehouse_name": "lh_schema_drift",
# META       "default_lakehouse_workspace_id": "__WORKSPACE_ID__"
# META     }
# META   }
# META }

# MARKDOWN ********************

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# # NB_Schema_Gate
#
# Schema drift is not a single Fabric feature. This notebook is the control we add.
# Bronze files stay as they landed. Silver compares the observed schema to a versioned
# contract, writes an audit event, then appends only contract columns or quarantines
# the batch. Gold is a separate notebook and a separate contract.
#
# `mergeSchema` is intentionally not used. An approved additive column is applied
# with an explicit `ALTER TABLE` when that contract version is released.
#
# Synthetic claims only. Audit rows store field names and types, never values.
#
# Parameters:
# - `batch_name`: `all` runs the walkthrough. Any other value gates that one file.
# - `fail_on_quarantine`: `true` raises so the pipeline activity fails.
# - `write_gold`: `true` only for the interactive walkthrough. The pipeline leaves
#   this false so Gold is a later activity that never starts on failure.

# PARAMETERS CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

batch_name = "all"
fail_on_quarantine = "false"
write_gold = "true"

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

import json
import uuid
from datetime import datetime, timezone

import notebookutils

from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

FILES = "abfss://__WORKSPACE_ID__@onelake.dfs.fabric.microsoft.com/__LAKEHOUSE_ID__/Files/incoming"
V1_FIELDS = [
    {"name": "claim_id", "data_type": "string", "required": True},
    {"name": "payer_id", "data_type": "string", "required": True},
    {"name": "service_date", "data_type": "date", "required": True},
    {"name": "billed_amount", "data_type": "decimal", "required": True},
    {"name": "claim_status", "data_type": "string", "required": True},
]
V2_FIELDS = V1_FIELDS + [{"name": "place_of_service", "data_type": "string", "required": False}]
SPARK_TYPES = {"string": StringType(), "date": DateType(), "decimal": DecimalType(18, 2)}
SQL_TYPES = {"string": "STRING", "date": "DATE", "decimal": "DECIMAL(18,2)"}


def compare_schemas(expected, observed):
    exp = {field["name"]: field["data_type"] for field in expected}
    obs = {field["name"]: field["data_type"] for field in observed}
    added = sorted(set(obs) - set(exp))
    missing = sorted(set(exp) - set(obs))
    changed = [
        {"name": name, "expected": exp[name], "observed": obs[name]}
        for name in sorted(set(exp) & set(obs))
        if exp[name] != obs[name]
    ]
    return added, missing, changed


def decide(added, missing, changed, allowed_changes):
    if not added and not missing and not changed:
        return "allow"
    if missing or changed:
        return "quarantine"
    approved = set(allowed_changes)
    if added and all(f"additive:{name}" in approved for name in added):
        return "allow"
    return "quarantine"


def spark_schema(fields):
    return StructType(
        [StructField(field["name"], SPARK_TYPES[field["data_type"]], not field["required"]) for field in fields]
        + [StructField("batch_id", StringType(), False)]
    )


def ensure_control_tables():
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS schema_contracts (
            dataset STRING,
            layer STRING,
            contract_version INT,
            approved_schema STRING,
            required_fields STRING,
            allowed_changes STRING,
            owner STRING,
            approved_at STRING
        ) USING DELTA
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS schema_audit_events (
            event_id STRING,
            dataset STRING,
            layer STRING,
            batch_id STRING,
            run_id STRING,
            expected_schema STRING,
            observed_schema STRING,
            added_fields STRING,
            missing_fields STRING,
            changed_fields STRING,
            decision STRING,
            contract_version INT,
            event_time STRING
        ) USING DELTA
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS quarantine_claims (
            batch_id STRING,
            decision STRING,
            added_fields STRING,
            missing_fields STRING,
            changed_fields STRING,
            bronze_path STRING,
            event_time STRING
        ) USING DELTA
        """
    )


def seed_contracts():
    if spark.table("schema_contracts").count() > 0:
        return
    rows = [
        ("claims", "silver", 1, json.dumps(V1_FIELDS), json.dumps([f["name"] for f in V1_FIELDS if f["required"]]), "[]", "revenue-data-steward", "2026-09-01T00:00:00+00:00"),
        ("claims", "gold", 1, json.dumps(V1_FIELDS), json.dumps([f["name"] for f in V1_FIELDS if f["required"]]), "[]", "finance-analytics", "2026-09-01T00:00:00+00:00"),
    ]
    spark.createDataFrame(rows, "dataset STRING, layer STRING, contract_version INT, approved_schema STRING, required_fields STRING, allowed_changes STRING, owner STRING, approved_at STRING").write.format("delta").mode("append").saveAsTable("schema_contracts")


def active_contract(layer):
    row = (
        spark.table("schema_contracts")
        .where(f"dataset = 'claims' AND layer = '{layer}'")
        .orderBy("contract_version", ascending=False)
        .limit(1)
        .collect()[0]
    )
    return {
        "version": row.contract_version,
        "fields": json.loads(row.approved_schema),
        "allowed_changes": json.loads(row.allowed_changes),
    }


def ensure_managed_table(table, fields):
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], spark_schema(fields)).write.format("delta").mode("overwrite").saveAsTable(table)
        return
    existing = {field.name for field in spark.table(table).schema.fields}
    for field in fields:
        if field["name"] not in existing:
            # Approved evolution only. This is not mergeSchema.
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {field['name']} {SQL_TYPES[field['data_type']]}")


def record_event(batch_id, contract, observed, added, missing, changed, decision):
    event = {
        "event_id": str(uuid.uuid4()),
        "dataset": "claims",
        "layer": "silver",
        "batch_id": batch_id,
        "run_id": "fabric-demo",
        "expected_schema": json.dumps([{"name": f["name"], "data_type": f["data_type"]} for f in contract["fields"]]),
        "observed_schema": json.dumps(observed),
        "added_fields": json.dumps(added),
        "missing_fields": json.dumps(missing),
        "changed_fields": json.dumps(changed),
        "decision": decision,
        "contract_version": contract["version"],
        "event_time": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    if "CLM-" in json.dumps(event) or "120.50" in json.dumps(event):
        raise ValueError("Refusing to write claim values into the audit table")
    spark.createDataFrame([tuple(event.values())], ", ".join(f"{name} STRING" if name != "contract_version" else f"{name} INT" for name in event)).write.format("delta").mode("append").saveAsTable("schema_audit_events")
    return event


def read_observed(name):
    raw = notebookutils.fs.head(f"{FILES}/{name}.schema.json", 20000)
    return json.loads(raw)


def cast_to_contract(frame, fields):
    from pyspark.sql import functions as F

    for field in fields:
        column = F.col(field["name"])
        if field["data_type"] == "decimal":
            column = column.cast("decimal(18,2)")
        elif field["data_type"] == "date":
            column = column.cast("date")
        frame = frame.withColumn(field["name"], column)
    return frame


def gate_batch(name, promote_gold):
    observed = read_observed(name)
    contract = active_contract("silver")
    added, missing, changed = compare_schemas(contract["fields"], observed)
    decision = decide(added, missing, changed, contract["allowed_changes"])
    event = record_event(name, contract, observed, added, missing, changed, decision)
    print(f"{name}: {decision} added={added} missing={missing} changed={[item['name'] for item in changed]} contract=v{contract['version']}")
    if decision != "allow":
        spark.createDataFrame(
            [(name, decision, json.dumps(added), json.dumps(missing), json.dumps(changed), f"Files/incoming/{name}.csv", event["event_time"])],
            "batch_id STRING, decision STRING, added_fields STRING, missing_fields STRING, changed_fields STRING, bronze_path STRING, event_time STRING",
        ).write.format("delta").mode("append").saveAsTable("quarantine_claims")
        return decision
    ensure_managed_table("silver_claims", contract["fields"])
    frame = cast_to_contract(
        spark.read.option("header", True).csv(f"{FILES}/{name}.csv"),
        contract["fields"],
    ).select(*[field["name"] for field in contract["fields"]])
    frame = frame.withColumn("batch_id", spark_sql_lit(name))
    frame.write.format("delta").mode("append").saveAsTable("silver_claims")
    if promote_gold:
        gold = active_contract("gold")
        ensure_managed_table("gold_claims", gold["fields"])
        frame.select(*[field["name"] for field in gold["fields"]], "batch_id").write.format("delta").mode("append").saveAsTable("gold_claims")
    return decision


def spark_sql_lit(value):
    from pyspark.sql import functions as F
    return F.lit(value)


def approve_silver_v2():
    versions = [row.contract_version for row in spark.table("schema_contracts").where("layer = 'silver'").collect()]
    if 2 in versions:
        print("silver contract v2 already present; v1 was not overwritten")
        return
    row = [(
        "claims",
        "silver",
        2,
        json.dumps(V2_FIELDS),
        json.dumps([f["name"] for f in V2_FIELDS if f["required"]]),
        json.dumps(["additive:place_of_service"]),
        "revenue-data-steward",
        datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    )]
    spark.createDataFrame(row, "dataset STRING, layer STRING, contract_version INT, approved_schema STRING, required_fields STRING, allowed_changes STRING, owner STRING, approved_at STRING").write.format("delta").mode("append").saveAsTable("schema_contracts")
    print("inserted silver contract v2; gold contract remains v1")

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

ensure_control_tables()
seed_contracts()
promote_gold = str(write_gold).lower() == "true"
fail_pipeline = str(fail_on_quarantine).lower() == "true"

if batch_name == "all":
    for name in ("batch-clean", "batch-added", "batch-missing", "batch-type-change"):
        gate_batch(name, promote_gold)
    print("--- row counts before approval ---")
    print("silver", spark.table("silver_claims").count())
    if promote_gold:
        print("gold", spark.table("gold_claims").count())
    approve_silver_v2()
    gate_batch("batch-added", promote_gold)
    print("gold columns:", spark.table("gold_claims").columns)
    if "place_of_service" in spark.table("gold_claims").columns:
        raise ValueError("Unreleased column reached Gold")
    display(spark.table("schema_audit_events").select("batch_id", "decision", "added_fields", "missing_fields", "changed_fields", "contract_version"))
else:
    decision = gate_batch(batch_name, promote_gold)
    if fail_pipeline and decision != "allow":
        raise ValueError(f"Schema differs from the approved contract: {batch_name} -> {decision}")
