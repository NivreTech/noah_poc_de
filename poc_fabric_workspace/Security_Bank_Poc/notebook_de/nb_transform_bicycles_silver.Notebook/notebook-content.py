# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a2351cf8-9f0d-4d16-a495-5e4b8c64061d",
# META       "default_lakehouse_name": "lh_security_bank_poc",
# META       "default_lakehouse_workspace_id": "055f47cf-eeab-4f52-8d13-42a18be41cf8",
# META       "known_lakehouses": [
# META         {
# META           "id": "a2351cf8-9f0d-4d16-a495-5e4b8c64061d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Cell 1 — Parameters

# CELL ********************

# Accept parameters from pipeline (with defaults for manual run)
source_table = spark.conf.get("source_table", "silver.bicycles")
target_table = spark.conf.get("target_table", "gold.bicycles_curated")

# Deduping window
dedupe_key = "BikepointID"
ts_col = "ingestion_ts"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# Cell 2 — Read source

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table(source_table)
df.printSchema()
df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Cell 3 — Transform (standardize + derived fields + dedupe latest)

# CELL ********************

# Basic cleanup / standardization
df_clean = (
    df
    .withColumn("BikepointID", F.col("BikepointID").cast("string"))
    .withColumn("Street", F.trim(F.col("Street")))
    .withColumn("Neighbourhood", F.trim(F.col("Neighbourhood")))
    .withColumn("Latitude", F.col("Latitude").cast("double"))
    .withColumn("Longitude", F.col("Longitude").cast("double"))
    .withColumn("No_Bikes", F.col("No_Bikes").cast("long"))
    .withColumn("No_Empty_Docks", F.col("No_Empty_Docks").cast("long"))
)

# Derived metrics (useful for analytics)
df_enriched = (
    df_clean
    .withColumn("capacity_est", F.col("No_Bikes") + F.col("No_Empty_Docks"))
    .withColumn(
        "occupancy_rate",
        F.when(F.col("capacity_est") > 0, F.col("No_Bikes") / F.col("capacity_est"))
         .otherwise(F.lit(None))
    )
    .withColumn("load_date", F.to_date(F.col(ts_col)))
)

# Deduplicate: keep the latest record per BikepointID based on ingestion_ts
w = Window.partitionBy(dedupe_key).orderBy(F.col(ts_col).desc())

df_latest = (
    df_enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Cell 4 — Write to Gold (idempotent overwrite)

# CELL ********************

# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Write curated table
(
    df_latest
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(target_table)
)

print(f"✅ Wrote curated table: {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Cell 5 — Validate output

# CELL ********************

spark.table(target_table).select(
    "ingestion_ts", "BikepointID", "Street", "Neighbourhood",
    "Latitude", "Longitude", "No_Bikes", "No_Empty_Docks",
    "capacity_est", "occupancy_rate", "load_date"
).show(20, truncate=False)

spark.sql(f"SELECT COUNT(*) AS cnt FROM {target_table}").show()
#Validate output

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
