#!/usr/bin/env python3
"""Parity check: do the migrated notebook's output tables match the captured
Synapse baseline within tolerance?

For each fully-qualified table in the notebook's `outputs` (from the run-spec),
this runs up to three checks against the workspace and compares them to the
baseline:
  * row_count   - SELECT COUNT(*); exact match unless `row_count_tol` allows a delta
  * null_rates  - per column, SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END)/COUNT(*),
                  within `null_rate_tol`
  * checksum    - optional order-independent table digest, exact match when the
                  baseline carries one (see references/run-on-databricks.md)

Only fields the baseline actually carries for a table are checked. A table listed
in `outputs` but absent from the baseline is reported `not_evaluated`, never a
failure. Overall parity is `pass` when every evaluated table passes (and at least
one was evaluated), `fail` if any evaluated table fails, else `not_evaluated`.

All SQL execution is isolated behind `WarehouseClient` so the comparison logic is
unit-testable with a mock and no network. Auth is a named CLI/SDK profile only;
tokens are never read from or written to the run-spec or anywhere else.

Usage:
    python compare_baseline.py --baseline BASELINE --run-spec SPEC \
        --notebook NAME [--profile P] [--json PATH]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

from runspec import load_baseline, load_runspec, resolve_notebook

POLL_SECONDS = 2
POLL_TIMEOUT = 60 * 10  # cap a single statement at 10 minutes


class WarehouseClient:
    """The single seam that runs SQL on Databricks. Swapped/mocked for tests.

    Uses `databricks-sdk` SQL execution when available. Auth is a named profile.
    """

    def __init__(self, profile=None, warehouse_id=None):
        self.profile = profile
        self.warehouse_id = warehouse_id
        self._client = None

    def _ws(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient(profile=self.profile)
        return self._client

    def scalar(self, sql: str):
        """Run `sql`, poll to a terminal state, and return the first column of
        the first row (or None).

        A large COUNT / checksum can outlast the inline `wait_timeout`, leaving
        the statement PENDING/RUNNING; without polling the result would silently
        be empty. We wait out the statement to a terminal state (capped) before
        reading the result.
        """
        ws = self._ws()
        kwargs = {"statement": sql, "wait_timeout": "50s"}
        if self.warehouse_id:
            kwargs["warehouse_id"] = self.warehouse_id
        resp = self._await(ws.statement_execution.execute_statement(**kwargs))
        result = getattr(resp, "result", None)
        data = getattr(result, "data_array", None) if result else None
        if not data or not data[0]:
            return None
        return data[0][0]

    def _await(self, resp):
        """Poll a statement response to a terminal state (or the timeout cap)."""
        waited = 0
        while True:
            state = getattr(getattr(resp, "status", None), "state", None)
            state = getattr(state, "value", None) or str(state or "")
            if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED", ""):
                return resp
            if waited >= POLL_TIMEOUT:
                return resp
            time.sleep(POLL_SECONDS)
            waited += POLL_SECONDS
            resp = self._ws().statement_execution.get_statement(resp.statement_id)


def _f(value):
    return None if value is None else float(value)


def _count_sql(table: str) -> str:
    return f"SELECT COUNT(*) FROM {table}"


def _null_rate_sql(table: str, column: str) -> str:
    return (f"SELECT SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) / COUNT(*) "
            f"FROM {table}")


def _checksum_sql(table: str) -> str:
    return (f"SELECT md5(string(sum(crc32(to_json(struct(*)))))) AS checksum "
            f"FROM {table}")


def compare_table(client: WarehouseClient, table: str, base: dict,
                  tol: dict) -> dict:
    """Compare one table to its baseline entry; return the contract per-table dict."""
    row_count_tol = base.get("row_count_tol", tol.get("row_count_tol", 0))
    null_rate_tol = base.get("null_rate_tol", tol.get("null_rate_tol", 0.0))

    entry: dict = {"table": table}
    checks: list[bool] = []

    if "row_count" in base:
        actual = client.scalar(_count_sql(table))
        actual = int(actual) if actual is not None else None
        expected = int(base["row_count"])
        ok = actual is not None and abs(actual - expected) <= row_count_tol
        entry["row_count"] = {"expected": expected, "actual": actual, "ok": ok}
        checks.append(ok)

    if base.get("null_rates"):
        nulls = []
        for column, expected in base["null_rates"].items():
            actual = _f(client.scalar(_null_rate_sql(table, column)))
            expected = float(expected)
            ok = actual is not None and abs(actual - expected) <= null_rate_tol
            nulls.append({"column": column, "expected": expected,
                          "actual": actual, "ok": ok})
            checks.append(ok)
        entry["null_rates"] = nulls

    if base.get("checksum"):
        actual = client.scalar(_checksum_sql(table))
        expected = base["checksum"]
        ok = actual == expected
        entry["checksum"] = {"expected": expected, "actual": actual, "ok": ok}
        checks.append(ok)

    if not checks:
        entry["result"] = "not_evaluated"
    else:
        entry["result"] = "pass" if all(checks) else "fail"
    return entry


def compare(client: WarehouseClient, nb_entry: dict, baseline: dict) -> dict:
    tol = baseline.get("tolerances") or {}
    base_tables = baseline.get("tables") or {}
    outputs = nb_entry.get("outputs") or []

    table_reports = []
    for table in outputs:
        base = base_tables.get(table)
        if base is None:
            table_reports.append({"table": table, "result": "not_evaluated"})
            continue
        table_reports.append(compare_table(client, table, base, tol))

    evaluated = [t for t in table_reports if t["result"] != "not_evaluated"]
    if not evaluated:
        parity = "not_evaluated"
    elif all(t["result"] == "pass" for t in evaluated):
        parity = "pass"
    else:
        parity = "fail"

    return {"name": nb_entry["name"], "parity": parity, "tables": table_reports}


def _print_table(rep: dict) -> None:
    print(f"notebook : {rep['name']}")
    print(f"parity   : {rep['parity']}")
    if not rep["tables"]:
        print("(no output tables declared; nothing to compare)")
        return
    print(f"\n{'TABLE':<40}  {'RESULT':<13}  DETAIL")
    print("-" * 90)
    for t in rep["tables"]:
        detail = ""
        if "row_count" in t:
            rc = t["row_count"]
            detail = f"rows exp={rc['expected']} act={rc['actual']}"
        elif t["result"] == "not_evaluated":
            detail = "no baseline"
        print(f"{t['table']:<40}  {t['result']:<13}  {detail}")
        for nr in t.get("null_rates", []):
            flag = "ok" if nr["ok"] else "MISMATCH"
            print(f"{'':<40}  {'':<13}  null {nr['column']}: "
                  f"exp={nr['expected']} act={nr['actual']} [{flag}]")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", required=True, help="baseline YAML/JSON")
    ap.add_argument("--run-spec", required=True, help="run-spec YAML/JSON")
    ap.add_argument("--notebook", required=True,
                    help="notebook `name` from the run-spec")
    ap.add_argument("--profile", help="Databricks CLI/SDK auth profile")
    ap.add_argument("--warehouse-id", help="SQL warehouse id for the parity queries")
    ap.add_argument("--json", metavar="PATH", help="write the report as JSON")
    args = ap.parse_args(argv)

    for label, p in (("baseline", args.baseline), ("run-spec", args.run_spec)):
        if not os.path.isfile(os.path.expanduser(p)):
            print(f"error: {label} not found: {p}", file=sys.stderr)
            return 2

    spec = load_runspec(args.run_spec)
    nb_entry = resolve_notebook(spec, args.notebook)
    if nb_entry is None:
        print(f"error: notebook '{args.notebook}' not in run-spec", file=sys.stderr)
        return 2
    baseline = load_baseline(args.baseline)

    profile = args.profile or nb_entry.get("workspace_profile")
    try:
        client = WarehouseClient(profile=profile, warehouse_id=args.warehouse_id)
        rep = compare(client, nb_entry, baseline)
    except Exception as exc:
        print(f"error: parity check failed: {exc}", file=sys.stderr)
        return 1

    _print_table(rep)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(rep, fh, indent=2)
        print(f"\nReport written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
