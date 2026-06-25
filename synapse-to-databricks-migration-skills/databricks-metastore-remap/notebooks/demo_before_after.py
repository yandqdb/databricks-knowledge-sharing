# Databricks notebook source
# MAGIC %md
# MAGIC # DEMO — metastore remap, before → after
# MAGIC Shows the rename the skill performs and, crucially, the references it
# MAGIC **refuses** to touch. The "before" cell is what a Synapse notebook looks
# MAGIC like; the "after" cell is what the skill emits (a pure rename to UC).
# MAGIC
# MAGIC This demo is illustrative (no live tables needed). To run for real, point
# MAGIC the names at tables that exist in your workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## BEFORE — legacy hive_metastore references
# MAGIC Every form a Synapse notebook uses: fully-qualified in PySpark, in SQL,
# MAGIC and a bare name resolved against the default database. Plus two traps.

# COMMAND ----------

# A Python variable that happens to be named like a table — a TRAP.
routes = spark.read.table("hive_metastore.transit.routes")
raw = spark.table("hive_metastore.transit.raw_taps")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- daily_summary is a CTE (TRAP), raw_taps_2019_archive is UNMAPPED (TRAP).
# MAGIC WITH daily_summary AS (
# MAGIC   SELECT route_id, COUNT(*) c FROM hive_metastore.transit.raw_taps GROUP BY route_id
# MAGIC )
# MAGIC SELECT d.*, a.note
# MAGIC FROM daily_summary d
# MAGIC LEFT JOIN hive_metastore.transit.raw_taps_2019_archive a USING (route_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AFTER — what the skill emits
# MAGIC `python remap_refs.py demo_before_after.py --mapping ../assets/sample_mapping.yaml`
# MAGIC
# MAGIC - `hive_metastore.transit.routes` / `.raw_taps` → `transit_prod.bronze.*`
# MAGIC - the Python variable `routes` is **untouched** (not a table reference)
# MAGIC - the CTE `daily_summary` is **untouched** (locally defined)
# MAGIC - `raw_taps_2019_archive` is **reported as UNMAPPED**, left as-is until you
# MAGIC   add it to the mapping after confirming its UC path
# MAGIC
# MAGIC The skill prints the per-cell diff; you approve before anything is written.

# COMMAND ----------

# AFTER (equivalent of the first code cell, post-rewrite):
routes = spark.read.table("transit_prod.bronze.routes")
raw = spark.table("transit_prod.bronze.raw_taps")
