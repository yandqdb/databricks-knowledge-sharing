# Databricks notebook source
# MAGIC %md
# MAGIC # Route performance (Synapse origin)
# MAGIC References tables via `hive_metastore`. After dual-registration the same
# MAGIC data is in UC external tables on the same ADLS paths. This is the **before**
# MAGIC fixture for databricks-metastore-remap.

# COMMAND ----------

# Fully qualified hive_metastore refs in PySpark.
raw = spark.read.table("hive_metastore.transit.raw_taps")
routes = spark.table("hive_metastore.transit.routes")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fully qualified in SQL, plus a JOIN.
# MAGIC SELECT r.route_name, COUNT(*) AS taps
# MAGIC FROM hive_metastore.transit.raw_taps t
# MAGIC JOIN hive_metastore.transit.routes r ON t.route_id = r.route_id
# MAGIC GROUP BY r.route_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRAP: `daily_summary` is a CTE here, NOT the hive table. Must NOT be
# MAGIC -- remapped even though a bare-name rule for it could exist.
# MAGIC WITH daily_summary AS (
# MAGIC   SELECT route_id, COUNT(*) c FROM hive_metastore.transit.raw_taps GROUP BY route_id
# MAGIC )
# MAGIC SELECT * FROM daily_summary

# COMMAND ----------

# TRAP: an UNMAPPED hive_metastore table — no mapping entry. The skill must
# report it and leave it untouched, never guess a target.
archive = spark.table("hive_metastore.transit.raw_taps_2019_archive")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bare unqualified reference, resolved against the default database.
# MAGIC -- Remapped only because mapping declares default_database = hive_metastore.transit.
# MAGIC SELECT * FROM routes
