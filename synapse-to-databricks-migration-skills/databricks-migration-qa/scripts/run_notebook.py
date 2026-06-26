#!/usr/bin/env python3
"""Run a migrated notebook on Databricks and measure execution coverage.

Imports the migrated source to its `workspace_path`, submits a one-time notebook
job run (Jobs "runs submit") with the configured compute and `notebook_params`
from the run-spec, polls to a terminal state, pulls the run output, and reports
how much of the notebook actually executed:

    execution_coverage = executed_cells / executable_cells

where `executable_cells` is the count of non-markdown, non-empty cells from the
local `nb_io.py` parser (the same parser every script in this skill uses), and
`executed_cells` is the count of cells up to and including the last one that
produced output. A notebook job run halts at the first uncaught error, so on
failure the report names `first_failing_cell` and its error message.

All Databricks access is isolated behind `DatabricksRunner` so the polling /
coverage logic can be unit-tested or mocked without a network. Auth is by CLI/SDK
profile only; tokens are never read from or written to the run-spec or anywhere
else.

Usage:
    python run_notebook.py --run-spec SPEC --notebook NAME [--profile P] [--json PATH]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

from runspec import executable_cell_indices, load_runspec, resolve_notebook

POLL_SECONDS = 5
POLL_TIMEOUT = 60 * 60  # 1 hour hard cap on a single run


class DatabricksRunner:
    """The single seam that touches Databricks. Swap/mocked for tests.

    Uses `databricks-sdk` when available; otherwise shells out to the
    `databricks` CLI. Either way, auth is a named profile only.
    """

    def __init__(self, profile=None):
        self.profile = profile
        self._client = None

    def _ws(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient(profile=self.profile)
        return self._client

    def import_notebook(self, local_path: str, workspace_path: str) -> None:
        """Upload/overwrite the migrated source at workspace_path.

        The import language is derived from the file extension so non-Python
        notebooks import correctly: `.sql`/`.scala`/`.r` map to their language
        and `.ipynb` uses the Jupyter import format; everything else (the
        Databricks `.py` source export, where per-cell `%sql`/`%scala` magics
        select the language) imports as PYTHON SOURCE.
        """
        from databricks.sdk.service.workspace import ImportFormat, Language
        import base64

        with open(local_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("ascii")
        ws = self._ws()
        parent = workspace_path.rsplit("/", 1)[0]
        if parent:
            ws.workspace.mkdirs(parent)
        ext = os.path.splitext(local_path)[1].lower()
        if ext == ".ipynb":
            ws.workspace.import_(
                path=workspace_path, content=content,
                format=ImportFormat.JUPYTER, overwrite=True)
            return
        language = {".sql": Language.SQL, ".scala": Language.SCALA,
                    ".r": Language.R}.get(ext, Language.PYTHON)
        ws.workspace.import_(
            path=workspace_path, content=content, format=ImportFormat.SOURCE,
            language=language, overwrite=True)

    def submit_run(self, workspace_path, notebook_params, compute,
                   run_name="migration-qa") -> int:
        """Submit a one-time notebook job run; return its run_id."""
        from databricks.sdk.service import jobs

        task = jobs.SubmitTask(
            task_key="qa",
            notebook_task=jobs.NotebookTask(
                notebook_path=workspace_path,
                base_parameters=notebook_params or {}),
            **_compute_kwargs(compute))
        waiter = self._ws().jobs.submit(run_name=run_name, tasks=[task])
        return waiter.run_id

    def get_run(self, run_id: int) -> dict:
        """Return a normalized run-state dict (see _normalize_run)."""
        run = self._ws().jobs.get_run(run_id)
        return _normalize_run(run)

    def get_run_output(self, run_id: int) -> dict:
        """Return {'error': str|None, 'first_failing_cell': int|None} for the
        (single) task in the run.

        `first_failing_cell` is the nb_io index of the cell that raised, used to
        compute partial execution coverage. The public Jobs run-output API does
        not expose per-cell results for a notebook task, so this returns None for
        it on the real path (the run still reports SUCCESS/FAILED and the error
        message); the field is wired through so a richer telemetry source — or a
        test mock — can localize the failure and drive partial coverage.
        """
        run = self._ws().jobs.get_run(run_id)
        tasks = getattr(run, "tasks", None) or []
        if not tasks:
            return {"error": None, "first_failing_cell": None}
        out = self._ws().jobs.get_run_output(tasks[0].run_id)
        return {"error": getattr(out, "error", None),
                "first_failing_cell": None}


def _compute_kwargs(compute: dict) -> dict:
    """Map run-spec compute to SubmitTask kwargs."""
    ctype = (compute or {}).get("type", "serverless")
    if ctype == "existing_cluster":
        return {"existing_cluster_id": compute["existing_cluster_id"]}
    if ctype == "job_cluster":
        # The caller supplies a new_cluster spec under compute["new_cluster"].
        from databricks.sdk.service import compute as c
        return {"new_cluster": c.ClusterSpec(**compute["new_cluster"])}
    # serverless: no cluster fields -> the job runs on serverless compute.
    return {}


def _normalize_run(run) -> dict:
    """Flatten an SDK Run into the fields this script needs.

    Terminal life_cycle_state is TERMINATED; result_state is SUCCESS/FAILED/etc.
    """
    state = getattr(run, "state", None)
    life = getattr(getattr(state, "life_cycle_state", None), "value", None) \
        or str(getattr(state, "life_cycle_state", "") or "")
    result = getattr(getattr(state, "result_state", None), "value", None) \
        or str(getattr(state, "result_state", "") or "")
    return {
        "run_id": getattr(run, "run_id", None),
        "run_page_url": getattr(run, "run_page_url", None),
        "life_cycle_state": life,
        "result_state": result,
        "state_message": getattr(state, "state_message", "") if state else "",
    }


_TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def poll_to_terminal(runner: DatabricksRunner, run_id: int,
                     poll=POLL_SECONDS, timeout=POLL_TIMEOUT, sleep=time.sleep) -> dict:
    waited = 0
    while True:
        r = runner.get_run(run_id)
        if r["life_cycle_state"] in _TERMINAL:
            return r
        if waited >= timeout:
            r["state_message"] = f"timed out after {waited}s; {r.get('state_message', '')}"
            return r
        sleep(poll)
        waited += poll


def execution_report(runner, nb_entry, local_migrated, *, sleep=time.sleep) -> dict:
    """Run nb_entry on the workspace and return the contract JSON object."""
    exec_indices = executable_cell_indices(local_migrated)
    executable = len(exec_indices)

    runner.import_notebook(local_migrated, nb_entry["workspace_path"])
    run_id = runner.submit_run(
        nb_entry["workspace_path"], nb_entry.get("parameters") or {},
        nb_entry.get("compute") or {})

    final = poll_to_terminal(runner, run_id, sleep=sleep)
    output = runner.get_run_output(run_id)
    error = output.get("error")
    first_failing = output.get("first_failing_cell")

    success = final["result_state"] == "SUCCESS" and not error
    if success:
        # Ran end to end: every executable cell produced output.
        executed = executable
        first_failing = None
    elif first_failing is not None:
        # A job run halts at the first uncaught error. The cells before the
        # failing cell ran and produced output; the failing cell and everything
        # after it did not complete. executed_cells = executable cells up to (not
        # including) the first failing cell, so coverage is partial and the
        # failure is localized to first_failing_cell.
        executed = sum(1 for i in exec_indices if i < first_failing)
    else:
        # Failed, but the run output did not localize a cell. We cannot attribute
        # partial progress, so report 0 executed rather than overstate it.
        executed = 0

    return {
        "name": nb_entry["name"],
        "run_id": run_id,
        "run_url": final.get("run_page_url"),
        "state": "SUCCESS" if success else (final["result_state"] or "FAILED"),
        "runs": bool(success),
        "first_failing_cell": first_failing,
        "error": error or (None if success else final.get("state_message") or None),
        "executable_cells": executable,
        "executed_cells": executed,
        "execution_coverage": round(executed / executable, 4) if executable else 1.0,
    }


def _print_table(rep: dict) -> None:
    print(f"notebook : {rep['name']}")
    print(f"run      : {rep['run_id']}  {rep.get('run_url') or ''}")
    print(f"state    : {rep['state']}  (runs={rep['runs']})")
    print(f"coverage : {rep['executed_cells']}/{rep['executable_cells']} cells "
          f"= {rep['execution_coverage'] * 100:.1f}%")
    if rep["first_failing_cell"] is not None:
        print(f"first failing cell: {rep['first_failing_cell']}")
    if rep["error"]:
        print(f"error    : {rep['error']}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-spec", required=True, help="run-spec YAML/JSON")
    ap.add_argument("--notebook", required=True, help="notebook `name` from the run-spec")
    ap.add_argument("--profile", help="Databricks CLI/SDK auth profile")
    ap.add_argument("--json", metavar="PATH", help="write the report as JSON")
    args = ap.parse_args(argv)

    spec = load_runspec(args.run_spec)
    nb_entry = resolve_notebook(spec, args.notebook)
    if nb_entry is None:
        print(f"error: notebook '{args.notebook}' not in run-spec", file=sys.stderr)
        return 2

    local_migrated = os.path.expanduser(nb_entry["migrated"])
    if not os.path.isfile(local_migrated):
        print(f"error: migrated source not found: {local_migrated}", file=sys.stderr)
        return 2

    profile = args.profile or nb_entry.get("workspace_profile")
    try:
        runner = DatabricksRunner(profile=profile)
        rep = execution_report(runner, nb_entry, local_migrated)
    except Exception as exc:
        print(f"error: run failed: {exc}", file=sys.stderr)
        return 1

    _print_table(rep)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(rep, fh, indent=2)
        print(f"\nReport written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
