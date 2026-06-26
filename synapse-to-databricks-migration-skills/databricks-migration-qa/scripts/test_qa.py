#!/usr/bin/env python3
"""Stdlib unit tests for the migration-QA scripts. No network, no pytest.

Covers change_coverage, runspec, compare_baseline (mocked warehouse), qa_report
report math, and run_notebook execution coverage (mocked DatabricksRunner). The
two Databricks-touching scripts are exercised only through their mocked seams, so
this suite runs offline.

    python test_qa.py            # or: python -m unittest test_qa -v
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest

import change_coverage
import compare_baseline
import qa_report
import run_notebook
import runspec

HERE = os.path.dirname(os.path.abspath(__file__))
ASSETS = os.path.join(HERE, "..", "assets")
# The original demo fixture is owned by the sibling metastore-remap skill.
ORIG_FIXTURE = os.path.normpath(os.path.join(
    HERE, "..", "..", "databricks-metastore-remap", "assets",
    "sample_synapse_notebook.py"))

# The demo's migrated copy: metastore-remap + ANSI session-flag applied. The
# unmapped `raw_taps_2019_archive` is the deliberate trap left for a human, so
# exactly one metastore site survives (change coverage 5/6 = 0.833).
DEMO_MIGRATED = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Route performance (Synapse origin)

# COMMAND ----------
spark.conf.set("spark.sql.ansi.enabled", "false")
# COMMAND ----------

# Fully qualified hive_metastore refs in PySpark.
raw = spark.read.table("transit_prod.bronze.raw_taps")
routes = spark.table("transit_prod.bronze.routes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.route_name, COUNT(*) AS taps
# MAGIC FROM transit_prod.bronze.raw_taps t
# MAGIC JOIN transit_prod.bronze.routes r ON t.route_id = r.route_id
# MAGIC GROUP BY r.route_name

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH daily_summary AS (
# MAGIC   SELECT route_id, COUNT(*) c FROM transit_prod.bronze.raw_taps GROUP BY route_id
# MAGIC )
# MAGIC SELECT * FROM daily_summary

# COMMAND ----------

# TRAP: an UNMAPPED hive_metastore table, left untouched.
archive = spark.table("hive_metastore.transit.raw_taps_2019_archive")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transit_prod.bronze.routes
'''


def _write(text: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".py")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    return path


class ChangeCoverageTest(unittest.TestCase):
    def setUp(self):
        self._tmp = []

    def tearDown(self):
        for p in self._tmp:
            try:
                os.remove(p)
            except OSError:
                pass

    def _nb(self, text):
        p = _write(text)
        self._tmp.append(p)
        return p

    def test_demo_empirical_numbers(self):
        """The clean demo build is 0.833, not 1.0 (the unmapped archive trap)."""
        migrated = self._nb(DEMO_MIGRATED)
        rep = change_coverage.compute(ORIG_FIXTURE, migrated)
        self.assertEqual(rep["by_category"]["metastore"]["required"], 6)
        self.assertEqual(rep["by_category"]["metastore"]["residual"], 1)
        self.assertEqual(rep["by_category"]["ansi"]["required"], 0)
        self.assertEqual(rep["overall"]["required"], 6)
        self.assertEqual(rep["overall"]["resolved"], 5)
        self.assertEqual(rep["overall"]["coverage"], 0.8333)
        # The residual must be the archive table, named for the engineer.
        self.assertEqual(len(rep["residual_sites"]), 1)
        self.assertIn("raw_taps_2019_archive",
                      rep["residual_sites"][0]["snippet"])

    def test_fully_migrated_is_100pct(self):
        orig = self._nb(
            "# Databricks notebook source\n"
            "raw = spark.table('hive_metastore.transit.raw_taps')\n")
        migrated = self._nb(
            "# Databricks notebook source\n"
            "raw = spark.table('transit_prod.bronze.raw_taps')\n")
        rep = change_coverage.compute(orig, migrated)
        self.assertEqual(rep["overall"]["coverage"], 1.0)
        self.assertEqual(rep["overall"]["residual"], 0)
        self.assertEqual(rep["residual_sites"], [])

    def test_leftover_metastore_ref(self):
        orig = self._nb(
            "# Databricks notebook source\n"
            "a = spark.table('hive_metastore.transit.raw_taps')\n"
            "b = spark.table('hive_metastore.transit.routes')\n")
        migrated = self._nb(  # only one of the two got remapped
            "# Databricks notebook source\n"
            "a = spark.table('transit_prod.bronze.raw_taps')\n"
            "b = spark.table('hive_metastore.transit.routes')\n")
        rep = change_coverage.compute(orig, migrated)
        meta = rep["by_category"]["metastore"]
        self.assertEqual(meta["required"], 2)
        self.assertEqual(meta["residual"], 1)
        self.assertEqual(meta["coverage"], 0.5)
        self.assertLess(rep["overall"]["coverage"], 1.0)

    def test_leftover_cast_without_session_flag(self):
        orig = self._nb(
            "# Databricks notebook source\n"
            "df = src.select(col('x').cast('int'))\n")
        migrated_residual = self._nb(  # same cast, no try_cast, no flag
            "# Databricks notebook source\n"
            "df = src.select(col('x').cast('int'))\n")
        rep = change_coverage.compute(orig, migrated_residual)
        ansi = rep["by_category"]["ansi"]
        self.assertGreaterEqual(ansi["required"], 1)
        self.assertEqual(ansi["residual"], ansi["required"])
        self.assertLess(rep["overall"]["coverage"], 1.0)

    def test_different_migrated_ref_does_not_inflate_residual(self):
        # Both required refs were remapped, but migration introduced a DIFFERENT,
        # never-required hive_metastore ref. It must not be counted as a residual,
        # and residual must stay <= required.
        orig = self._nb(
            "# Databricks notebook source\n"
            "a = spark.table('hive_metastore.transit.raw_taps')\n"
            "b = spark.table('hive_metastore.transit.routes')\n")
        migrated = self._nb(
            "# Databricks notebook source\n"
            "a = spark.table('transit_prod.bronze.raw_taps')\n"
            "b = spark.table('transit_prod.bronze.routes')\n"
            "c = spark.table('hive_metastore.transit.brand_new_table')\n")
        rep = change_coverage.compute(orig, migrated)
        meta = rep["by_category"]["metastore"]
        self.assertEqual(meta["required"], 2)
        self.assertEqual(meta["residual"], 0)
        self.assertLessEqual(meta["residual"], meta["required"])
        self.assertEqual(meta["coverage"], 1.0)
        self.assertEqual(rep["residual_sites"], [])

    def test_residual_capped_at_required(self):
        # The migrated copy left the required ref AND introduced a duplicate of
        # the same text. Residual is capped at the 1 originally required, not 2.
        orig = self._nb(
            "# Databricks notebook source\n"
            "a = spark.table('hive_metastore.transit.raw_taps')\n")
        migrated = self._nb(
            "# Databricks notebook source\n"
            "a = spark.table('hive_metastore.transit.raw_taps')\n"
            "b = spark.table('hive_metastore.transit.raw_taps')\n")
        rep = change_coverage.compute(orig, migrated)
        meta = rep["by_category"]["metastore"]
        self.assertEqual(meta["required"], 1)
        self.assertEqual(meta["residual"], 1)
        self.assertLessEqual(meta["residual"], meta["required"])

    def test_session_flag_resolves_all_ansi(self):
        orig = self._nb(
            "# Databricks notebook source\n"
            "df = src.select(col('x').cast('int'))\n")
        migrated_flagged = self._nb(  # cast unchanged but flag cell present
            "# Databricks notebook source\n"
            "spark.conf.set('spark.sql.ansi.enabled', 'false')\n"
            "# COMMAND ----------\n"
            "df = src.select(col('x').cast('int'))\n")
        rep = change_coverage.compute(orig, migrated_flagged)
        self.assertEqual(rep["by_category"]["ansi"]["residual"], 0)
        self.assertEqual(rep["by_category"]["ansi"]["coverage"], 1.0)


class RunSpecTest(unittest.TestCase):
    def test_load_and_resolve_sample(self):
        spec = runspec.load_runspec(os.path.join(ASSETS, "sample_run_spec.yaml"))
        nb = runspec.resolve_notebook(spec, "ridership_rollup")
        self.assertIsNotNone(nb)
        # defaults merged in
        self.assertEqual(nb["workspace_profile"], "translink-demo")
        self.assertEqual(nb["catalog"], "translink")
        self.assertEqual(nb["compute"], {"type": "serverless"})
        # per-notebook fields preserved
        self.assertEqual(nb["name"], "ridership_rollup")
        self.assertIn("translink.compass.ridership_daily", nb["outputs"])

    def test_unknown_notebook_returns_none(self):
        spec = runspec.load_runspec(os.path.join(ASSETS, "sample_run_spec.yaml"))
        self.assertIsNone(runspec.resolve_notebook(spec, "does_not_exist"))

    def test_per_notebook_compute_overrides_default(self):
        spec = {
            "defaults": {"compute": {"type": "serverless"},
                         "workspace_profile": "p"},
            "notebooks": [{
                "name": "n",
                "compute": {"type": "existing_cluster",
                            "existing_cluster_id": "0612-x"},
            }],
        }
        nb = runspec.resolve_notebook(spec, "n")
        # wholesale replacement: the serverless default is gone, not merged
        self.assertEqual(nb["compute"],
                         {"type": "existing_cluster",
                          "existing_cluster_id": "0612-x"})
        self.assertEqual(nb["workspace_profile"], "p")

    def test_load_baseline_sample(self):
        base = runspec.load_baseline(os.path.join(ASSETS, "sample_baseline.yaml"))
        self.assertEqual(
            base["tables"]["translink.compass.ridership_daily"]["row_count"],
            12000)

    def test_minimal_yaml_reader_matches_pyyaml_runspec(self):
        # Force PyYAML unavailable and confirm the built-in reader returns the
        # same structure as PyYAML for the documented run-spec shape.
        path = os.path.join(ASSETS, "sample_run_spec.yaml")
        truth = runspec.load_runspec(path)  # via PyYAML (present in this env)
        saved = runspec.yaml
        try:
            runspec.yaml = None
            got = runspec.load_runspec(path)
        finally:
            runspec.yaml = saved
        self.assertEqual(got, truth)
        nb = runspec.resolve_notebook(got, "ridership_rollup")
        self.assertEqual(nb["compute"], {"type": "serverless"})
        self.assertEqual(nb["parameters"]["run_date"], "2026-06-01")
        self.assertIn("translink.compass.ridership_daily", nb["outputs"])

    def test_minimal_yaml_reader_matches_pyyaml_baseline(self):
        path = os.path.join(ASSETS, "sample_baseline.yaml")
        truth = runspec.load_baseline(path)
        saved = runspec.yaml
        try:
            runspec.yaml = None
            got = runspec.load_baseline(path)
        finally:
            runspec.yaml = saved
        self.assertEqual(got, truth)
        rc = got["tables"]["translink.compass.ridership_daily"]["row_count"]
        self.assertEqual(rc, 12000)
        self.assertIsInstance(rc, int)
        self.assertIsInstance(
            got["tables"]["translink.compass.ridership_daily"]
               ["null_rates"]["fare_amount"], float)

    def test_executable_cell_count(self):
        nb = _write(
            "# Databricks notebook source\n"
            "# MAGIC %md\n"
            "# MAGIC title\n"
            "# COMMAND ----------\n"
            "x = 1\n"
            "# COMMAND ----------\n"
            "# MAGIC %sql\n"
            "# MAGIC SELECT 1\n"
            "# COMMAND ----------\n")  # trailing empty cell
        try:
            self.assertEqual(runspec.executable_cell_count(nb), 2)
        finally:
            os.remove(nb)


class _FakeWarehouse:
    """Stand-in for compare_baseline.WarehouseClient with canned scalars."""

    def __init__(self, row_count=None, null_rates=None, checksum=None):
        self.row_count = row_count
        self.null_rates = null_rates or {}
        self.checksum = checksum

    def scalar(self, sql):
        if "md5(" in sql:
            return self.checksum
        if "CASE WHEN" in sql:
            import re
            col = re.search(r"CASE WHEN (\w+) IS NULL", sql).group(1)
            return self.null_rates.get(col)
        if "COUNT(*)" in sql:
            return self.row_count
        return None


class CompareBaselineTest(unittest.TestCase):
    NB = {"name": "n", "outputs": ["c.s.t"]}
    BASE = {
        "tolerances": {"row_count_tol": 0, "null_rate_tol": 0.0},
        "tables": {"c.s.t": {"row_count": 100,
                             "null_rates": {"a": 0.0, "b": 0.01}}},
    }

    def test_parity_pass(self):
        fake = _FakeWarehouse(row_count=100, null_rates={"a": 0.0, "b": 0.01})
        rep = compare_baseline.compare(fake, self.NB, self.BASE)
        self.assertEqual(rep["parity"], "pass")
        self.assertEqual(rep["tables"][0]["result"], "pass")

    def test_parity_fail_on_row_count(self):
        fake = _FakeWarehouse(row_count=101, null_rates={"a": 0.0, "b": 0.01})
        rep = compare_baseline.compare(fake, self.NB, self.BASE)
        self.assertEqual(rep["parity"], "fail")
        self.assertFalse(rep["tables"][0]["row_count"]["ok"])

    def test_parity_fail_on_null_rate(self):
        fake = _FakeWarehouse(row_count=100, null_rates={"a": 0.0, "b": 0.2})
        rep = compare_baseline.compare(fake, self.NB, self.BASE)
        self.assertEqual(rep["parity"], "fail")

    def test_row_count_tolerance_allows_delta(self):
        base = {"tolerances": {"row_count_tol": 5},
                "tables": {"c.s.t": {"row_count": 100}}}
        fake = _FakeWarehouse(row_count=103)
        rep = compare_baseline.compare(fake, self.NB, base)
        self.assertEqual(rep["parity"], "pass")

    def test_table_without_baseline_is_not_evaluated(self):
        nb = {"name": "n", "outputs": ["c.s.unknown"]}
        fake = _FakeWarehouse(row_count=100)
        rep = compare_baseline.compare(fake, nb, self.BASE)
        self.assertEqual(rep["parity"], "not_evaluated")
        self.assertEqual(rep["tables"][0]["result"], "not_evaluated")


class _FakeRunner:
    """Stand-in for run_notebook.DatabricksRunner; no network."""

    def __init__(self, result_state="SUCCESS", error=None, first_failing_cell=None):
        self.result_state = result_state
        self.error = error
        self.first_failing_cell = first_failing_cell
        self.imported = False

    def import_notebook(self, local_path, workspace_path):
        self.imported = True

    def submit_run(self, workspace_path, notebook_params, compute,
                   run_name="migration-qa"):
        return 42

    def get_run(self, run_id):
        return {"run_id": run_id, "run_page_url": "https://example/run/42",
                "life_cycle_state": "TERMINATED",
                "result_state": self.result_state, "state_message": ""}

    def get_run_output(self, run_id):
        return {"error": self.error,
                "first_failing_cell": self.first_failing_cell}


class RunNotebookTest(unittest.TestCase):
    def _entry(self):
        nb = _write(
            "# Databricks notebook source\n"
            "# MAGIC %md\n# MAGIC t\n"
            "# COMMAND ----------\n"
            "x = 1\n"
            "# COMMAND ----------\n"
            "y = 2\n")
        self.addCleanup(os.remove, nb)
        return {"name": "n", "migrated": nb,
                "workspace_path": "/Workspace/n", "parameters": {},
                "compute": {"type": "serverless"}}, nb

    def test_success_is_full_coverage(self):
        entry, nb = self._entry()
        rep = run_notebook.execution_report(
            _FakeRunner("SUCCESS"), entry, nb, sleep=lambda s: None)
        self.assertTrue(rep["runs"])
        self.assertEqual(rep["state"], "SUCCESS")
        self.assertEqual(rep["executable_cells"], 2)
        self.assertEqual(rep["executed_cells"], 2)
        self.assertEqual(rep["execution_coverage"], 1.0)

    def test_failure_reports_partial_coverage_and_localizes(self):
        # Executable cells are at nb_io indices 1 and 2 (cell 0 is the md
        # header). The run fails at cell 2, so cell 1 ran (executed=1) and the
        # failure is localized to cell 2 -> coverage 1/2 = 0.5.
        entry, nb = self._entry()
        rep = run_notebook.execution_report(
            _FakeRunner("FAILED", error="boom", first_failing_cell=2),
            entry, nb, sleep=lambda s: None)
        self.assertFalse(rep["runs"])
        self.assertEqual(rep["state"], "FAILED")
        self.assertEqual(rep["first_failing_cell"], 2)
        self.assertEqual(rep["executable_cells"], 2)
        self.assertEqual(rep["executed_cells"], 1)
        self.assertEqual(rep["execution_coverage"], 0.5)
        self.assertEqual(rep["error"], "boom")

    def test_failure_without_localization_reports_zero(self):
        entry, nb = self._entry()
        rep = run_notebook.execution_report(
            _FakeRunner("FAILED", error="boom"), entry, nb, sleep=lambda s: None)
        self.assertFalse(rep["runs"])
        self.assertEqual(rep["executed_cells"], 0)
        self.assertIsNone(rep["first_failing_cell"])
        self.assertEqual(rep["error"], "boom")


class QaReportTest(unittest.TestCase):
    def _change(self, cov):
        return {"overall": {"required": 6, "resolved": int(round(cov * 6)),
                            "residual": 6 - int(round(cov * 6)), "coverage": cov},
                "residual_sites": []}

    def _run(self, runs=True, exec_cov=1.0):
        return {"name": "n", "runs": runs, "execution_coverage": exec_cov,
                "executable_cells": 6, "executed_cells": 6 if runs else 0,
                "state": "SUCCESS" if runs else "FAILED",
                "first_failing_cell": None, "error": None}

    def test_clean_pass(self):
        card = qa_report.score(self._change(1.0), self._run(), {"parity": "pass"}, 1.0)
        self.assertTrue(card["pass"])

    def test_change_gate_blocks_pass(self):
        card = qa_report.score(self._change(0.833), self._run(),
                               {"parity": "pass"}, 1.0)
        self.assertFalse(card["pass"])
        self.assertFalse(card["change_ok"])

    def test_relaxed_gate_allows_pass(self):
        card = qa_report.score(self._change(0.833), self._run(),
                               {"parity": "pass"}, 0.8)
        self.assertTrue(card["pass"])

    def test_failed_parity_blocks_pass(self):
        card = qa_report.score(self._change(1.0), self._run(),
                               {"parity": "fail"}, 1.0)
        self.assertFalse(card["pass"])

    def test_parity_not_evaluated_fallback(self):
        card = qa_report.score(self._change(1.0), self._run(), None, 1.0)
        self.assertTrue(card["pass"])  # runs + change gate, parity skipped
        self.assertFalse(card["parity_evaluated"])
        self.assertEqual(card["parity"], "not_evaluated")

    def test_not_runs_blocks_pass(self):
        card = qa_report.score(self._change(1.0), self._run(runs=False),
                               {"parity": "pass"}, 1.0)
        self.assertFalse(card["pass"])

    def test_batch_success_rate(self):
        cards = [
            qa_report.score(self._change(1.0), self._run(), {"parity": "pass"}, 1.0),
            qa_report.score(self._change(0.833), self._run(), {"parity": "pass"}, 1.0),
            qa_report.score(self._change(1.0), self._run(runs=False),
                            {"parity": "pass"}, 1.0),
        ]
        summary = qa_report.batch_summary(cards, 1.0)
        self.assertEqual(summary["total"], 3)
        self.assertEqual(summary["passing"], 1)
        self.assertAlmostEqual(summary["success_rate"], 33.3, places=1)

    def _dump(self, path, obj):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f)

    def test_batch_end_to_end_writes_files(self):
        d = tempfile.mkdtemp()
        for name, cov, runs, parity in (("good", 1.0, True, "pass"),
                                        ("bad", 0.5, False, "fail")):
            self._dump(os.path.join(d, f"{name}.change.json"), self._change(cov))
            run = self._run(runs=runs)
            run["name"] = name
            self._dump(os.path.join(d, f"{name}.run.json"), run)
            self._dump(os.path.join(d, f"{name}.parity.json"),
                       {"name": name, "parity": parity})
        out = os.path.join(d, "batch.md")
        rc = qa_report.main(["--batch", d, "--out", out])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.isfile(out))
        with open(os.path.join(d, "batch.json"), encoding="utf-8") as f:
            summary = json.load(f)
        self.assertEqual(summary["passing"], 1)
        self.assertEqual(summary["total"], 2)


if __name__ == "__main__":
    unittest.main()
