# Genie Implementation Plan

Self-contained, **interactive HTML implementation plan** for rolling out a Databricks Genie space — a single-file deliverable that can be opened in any browser without a build step or server.

## Contents

| File | Purpose |
|---|---|
| `genie-implementation-plan---*.html` | Standalone HTML page rendering a phased implementation plan with searchable/filterable work items, owner assignments, and progress tracking. |

## What it shows

- A reusable structure for documenting a Genie rollout (phases, work items, owners, status, duration estimates)
- Inline CSS + vanilla JS — no external dependencies, no build pipeline
- Search and filter controls embedded directly in the page

## Using it

Open the `.html` file in any modern browser:

```bash
open genie-implementation-plan---*.html  # macOS
```

To adapt for a different rollout, edit the data section in the HTML (the work items / phases array) and resave. No build step required.
