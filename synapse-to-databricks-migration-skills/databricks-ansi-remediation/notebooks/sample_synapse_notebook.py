# Databricks notebook source
# MAGIC %md
# MAGIC # Daily ridership rollup (Synapse origin)
# MAGIC Ran on Synapse Spark with `spark.sql.ansi.enabled=false`. Several casts
# MAGIC below silently produced NULL on bad input. On DBR (ANSI on) they raise.
# MAGIC This file is the **before** fixture for databricks-ansi-remediation.

# COMMAND ----------

# Explicit casts in PySpark — these are the highest-frequency ANSI break.
from pyspark.sql.functions import col, expr

df = spark.read.table("hive_metastore.transit.raw_taps")
clean = (
    df.withColumn("zone_id", col("zone_raw").cast("int"))
      .withColumn("fare", expr("cast(fare_text as decimal(10,2))"))
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL cell: explicit CAST + date parsing + integer division.
# MAGIC SELECT
# MAGIC   route_id,
# MAGIC   CAST(card_serial AS BIGINT)                      AS card_id,
# MAGIC   to_date(tap_day, 'yyyy-MM-dd')                   AS tap_date,
# MAGIC   total_taps div active_vehicles                   AS taps_per_vehicle
# MAGIC FROM hive_metastore.transit.raw_taps

# COMMAND ----------

# Insert into a typed target table — out-of-range values now raise.
spark.sql("""
  INSERT INTO hive_metastore.transit.daily_rollup
  SELECT route_id, CAST(tap_count AS SMALLINT) FROM tmp_counts
""")

# COMMAND ----------

# Implicit numeric coercion (low-confidence heuristic): quoted number in math.
result = spark.sql("SELECT revenue * '1.05' AS grossed_up FROM tmp_counts")

# COMMAND ----------

# TRAP: already remediated. The detector must NOT flag try_cast, and the
# remediator must NOT double-rewrite it into try_try_cast.
safe = df.withColumn("ok", expr("try_cast(maybe_num as int)"))
