# Databricks notebook source
# MAGIC %md
# MAGIC # DEMO — ANSI remediation, before → after
# MAGIC Runnable end-to-end on any DBR cluster (ANSI is on by default). Creates a
# MAGIC tiny table with one bad value, shows the cast **raising** the way a migrated
# MAGIC Synapse notebook does, then shows the **two remediations** the skill applies.
# MAGIC
# MAGIC Run top to bottom. Cell 2 is expected to FAIL — that is the point.

# COMMAND ----------

# Setup: a tiny dataset where one row can't cast to int ('N/A').
data = [("101", "12"), ("102", "7"), ("103", "N/A")]
df = spark.createDataFrame(data, ["route_id", "zone_raw"])
df.createOrReplaceTempView("raw_taps")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BEFORE — this is what breaks after migration
# MAGIC On Synapse (`ansi=false`) the bad row cast to NULL silently. On DBR (ANSI on)
# MAGIC the next cell raises `CAST_INVALID_INPUT`. Run it and read the error.

# COMMAND ----------

# EXPECTED TO RAISE on DBR: CAST_INVALID_INPUT ('N/A' is not a valid INT)
spark.sql("SELECT route_id, CAST(zone_raw AS INT) AS zone_id FROM raw_taps").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## AFTER, option A — try_cast (ANSI-native)
# MAGIC The skill's `--mode try_cast` rewrites the site. Bad value → NULL, matching
# MAGIC the original intent. The notebook stays ANSI-clean.

# COMMAND ----------

spark.sql("SELECT route_id, TRY_CAST(zone_raw AS INT) AS zone_id FROM raw_taps").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## AFTER, option B — session flag (exact legacy parity)
# MAGIC The skill's `--mode session-flag` injects this at the top of the notebook.
# MAGIC The *original, untouched* CAST then behaves exactly as it did on Synapse.

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")
# The original line, unchanged — now resolves the bad row to NULL like Synapse did.
spark.sql("SELECT route_id, CAST(zone_raw AS INT) AS zone_id FROM raw_taps").show()
spark.conf.set("spark.sql.ansi.enabled", "true")  # reset for the rest of the demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Takeaway
# MAGIC - **try_cast** changes the code, keeps it ANSI-native — use when NULL-on-bad
# MAGIC   is fine and you only have explicit casts.
# MAGIC - **session flag** leaves code untouched, restores exact parity for the whole
# MAGIC   notebook — use when parity must be bit-for-bit or constructs are mixed.
# MAGIC - The skill emits the per-cell diff; the engineer approves. Nothing is written
# MAGIC   without `--write`.
