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

# # NB_Gold_Release
#
# Runs only after NB_Schema_Gate succeeds. Projects the Silver batch onto the
# active Gold contract. Extra Silver columns are not released here.

# PARAMETERS CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

batch_name = "batch-clean"

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

import json

from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

SPARK_TYPES = {"string": StringType(), "date": DateType(), "decimal": DecimalType(18, 2)}
row = (
    spark.table("schema_contracts")
    .where("dataset = 'claims' AND layer = 'gold'")
    .orderBy("contract_version", ascending=False)
    .limit(1)
    .collect()[0]
)
fields = json.loads(row.approved_schema)
schema = StructType(
    [StructField(field["name"], SPARK_TYPES[field["data_type"]], not field["required"]) for field in fields]
    + [StructField("batch_id", StringType(), False)]
)
if not spark.catalog.tableExists("gold_claims"):
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").saveAsTable("gold_claims")

names = [field["name"] for field in fields]
projected = spark.table("silver_claims").where(f"batch_id = '{batch_name}'").select(*names, "batch_id")
if projected.count() == 0:
    raise ValueError(f"No Silver rows for {batch_name}. The gate did not promote this batch.")
projected.write.format("delta").mode("append").saveAsTable("gold_claims")
print("gold columns:", spark.table("gold_claims").columns)
if "place_of_service" in spark.table("gold_claims").columns:
    raise ValueError("Unreleased column reached Gold")
