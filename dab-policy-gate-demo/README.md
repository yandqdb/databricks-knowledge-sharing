# DAB Policy-Gate Demo

A worked example of how to gate a Databricks Asset Bundle (DAB) deploy on a policy check, using [Open Policy Agent](https://www.openpolicyagent.org/) / [conftest](https://www.conftest.dev/) to enforce conventions on the bundle before it ever reaches the workspace.

## What this shows

- A DAB with multiple deploy targets (`dev`, `test`, `test_automated`, `staging`, `prod`)
- A Rego policy that runs against `databricks bundle validate --output json`:
  - **deny** rule: every job's `name` must start with a configured prefix (default `flights_`)
  - **warn** rule: every job should declare `tags` for cost attribution
- An external config file (`policy/config.yaml`) that the Rego policy reads via `data.naming.job_prefix`, so the prefix can be changed without editing Rego
- A GitHub Actions workflow that:
  1. Validates the bundle schema (`databricks bundle validate`)
  2. Runs the conftest policy gate against the validated bundle JSON
  3. Dynamically injects a `team: test` tag onto every job via `yq` before deploy (DAB `presets.tags`)
  4. Deploys to `test_automated` only after the gate passes

## Layout

```
dab-policy-gate-demo/
├── .github/workflows/flights-cicd.yml    # CI/CD with policy gate + dynamic tag injection
├── flights/flights-advanced/             # DAB project (jobs, DLT pipelines, src, tests)
│   ├── databricks.yml                    # Bundle definition with multiple targets
│   ├── resources/                        # Per-resource YAMLs (jobs, DLT)
│   ├── src/                              # Python source
│   └── tests/                            # Unit + integration tests
└── policy/
    ├── databricks_bundle.rego            # Conftest policy
    └── config.yaml                       # Data loaded into the policy via `--data`
```

## Running locally

```bash
# 1. Install Databricks CLI: https://docs.databricks.com/dev-tools/cli/databricks-cli.html
# 2. Authenticate: databricks configure
# 3. Validate the bundle
cd flights/flights-advanced
databricks bundle validate --target dev --output json > /tmp/bundle.json

# 4. Run the policy gate
conftest test \
  --policy ../../policy \
  --data ../../policy/config.yaml \
  /tmp/bundle.json
```

To change the required job-name prefix, edit `policy/config.yaml` — no Rego changes needed.

## Adapting for your environment

The workflow assumes:
- A GitHub environment named `test` holding `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` secrets for OAuth M2M auth.
- A workspace with a SQL warehouse named `Shared endpoint` and a cluster policy named `small_job` (referenced via `lookup:` in `databricks.yml`).
- Service principal IDs are placeholders (`00000000-...`) — replace with real ones for `test_automated`, `staging`, `prod`.

To move the workflow into your own repo, copy `.github/workflows/flights-cicd.yml` to the repo root's `.github/workflows/` directory and adjust the `working-directory` path if you place the bundle somewhere other than `flights/flights-advanced/`.
