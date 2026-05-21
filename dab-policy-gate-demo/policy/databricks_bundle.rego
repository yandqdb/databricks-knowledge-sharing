# Policy gate for Databricks Asset Bundles.
# Input: `databricks bundle validate --output json` for a given target.
# Conftest convention: `deny` rules block deploy; `warn` rules surface in CI logs but don't fail.

package main

import rego.v1

# ---------------------------------------------------------------------------
# Config — sourced from policy/config.yaml via `conftest --data`.
# Falls back to `flights_` when no data file is supplied (e.g. local runs).
# ---------------------------------------------------------------------------

default job_prefix := "flights_"

job_prefix := data.naming.job_prefix

# ---------------------------------------------------------------------------
# Hard rules (deny) — these block deploy.
# ---------------------------------------------------------------------------

# Naming convention: every job declared in the bundle must have a `name`
# starting with the configured prefix. Keeps resources discoverable in
# shared workspaces.
deny contains msg if {
	some key, job in input.resources.jobs
	name := object.get(job, "name", key)
	not startswith(name, job_prefix)
	msg := sprintf("job '%s' must have a name starting with '%s' (got '%s')", [key, job_prefix, name])
}

# ---------------------------------------------------------------------------
# Soft rules (warn) — surface in CI logs but don't block.
# ---------------------------------------------------------------------------

# Every job should declare `tags` for cost attribution and ownership.
warn contains msg if {
	some key, job in input.resources.jobs
	not job.tags
	msg := sprintf("job '%s' has no tags — add tags for cost attribution and ownership", [key])
}
