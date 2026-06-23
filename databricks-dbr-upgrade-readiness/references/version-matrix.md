# DBR Version Matrix

The DBR number is a label. The real upgrade surface is the set of component versions it bundles. Resolve **both** endpoints of the jump here, then read [behavior-changes.md](behavior-changes.md) for the changes that fall between them.

> **Always confirm against the official release notes.** Preinstalled library versions and even some defaults shift between maintenance releases (e.g. `15.4.1` vs `15.4.5`). The headline versions below are for planning the jump, not for pinning. Source of truth: https://docs.databricks.com/aws/en/release-notes/runtime/

## LTS headline versions

| DBR LTS | Apache Spark | Python | Scala | Notes |
|---------|--------------|--------|-------|-------|
| 9.1 LTS | 3.1 | 3.8 | 2.12 | End of support. Migrate off. |
| 10.4 LTS | 3.2 | 3.8 | 2.12 | End of support. |
| 11.3 LTS | 3.3 | 3.9 | 2.12 | Log4j 2 era; many configs renamed. |
| 12.2 LTS | 3.3 | 3.9 | 2.12 | |
| 13.3 LTS | 3.4 | 3.10 | 2.12 | Spark 3.4 — pandas API on Spark changes. |
| 14.3 LTS | 3.5 | 3.10 | 2.12 | Spark 3.5. |
| 15.4 LTS | 3.5 | 3.11 | 2.12 | Python 3.11; preinstalled libs bumped. |
| 16.4 LTS | 3.5.x | 3.12 | 2.12 | Python 3.12 — `distutils` removed from stdlib. |

Non-LTS runtimes exist between these (e.g. 13.0, 14.0, 15.0/15.1). They carry the same Spark/Python as the nearest LTS in most cases, but **target an LTS for production** unless the user has a specific reason.

## Why these columns matter for the jump

- **Spark minor version** is the biggest single source of behavior change. Each minor (3.1 → 3.2 → 3.3 → 3.4 → 3.5) has its own Spark SQL migration guide section. A jump that crosses 2+ minors should be staged (see [upgrade-workflow.md](upgrade-workflow.md)).
- **Python interpreter** jumps remove stdlib modules and change syntax/deprecation behavior. The 3.11 → 3.12 jump removes `distutils`; older jumps remove `imp`, `asyncio.coroutine`, and `collections` ABC aliases.
- **Scala has stayed 2.12 across classic DBR.** A classic→classic DBR upgrade therefore usually needs **no Scala JAR recompile** — this is the key difference from serverless (Scala 2.13.16). Still re-test JARs against the new Spark minor; binary compatibility within 2.12 is not guaranteed across Spark versions.

## Headline preinstalled-library jumps

These are the libraries that most often break code on upgrade. Exact versions vary by maintenance release — **diff the two release notes pages** to get the precise before/after. The pattern to watch:

| Library | Breaking jump to watch for | Common breakage |
|---------|----------------------------|-----------------|
| pandas | 1.x → 2.x (lands in the newer DBR generations) | `DataFrame.append` removed (use `concat`), `.iteritems()` removed (use `.items()`), stricter `inplace`, changed default `dtype` inference, `pd.np` removed. |
| numpy | 1.x → 2.x | `np.float`/`np.int`/`np.bool` aliases removed; some C-API dependent wheels need rebuild. |
| pyarrow | major bumps | Arrow-based pandas conversion behavior; timestamp unit handling. |
| MLflow | 1.x → 2.x → 3.x | Model registry / flavor API changes; signature requirements. |
| Delta Lake | protocol/table-feature bumps | Writing from new DBR can raise a table's min reader/writer version. See the Delta protocol section in [upgrade-workflow.md](upgrade-workflow.md). |

## How to resolve an exact jump

1. Read the source `spark_version` from the cluster/job spec (e.g. `10.4.x-scala2.12` → DBR 10.4 LTS).
2. Pick the target (default: latest LTS).
3. Open both release notes pages and record: Spark version, Python version, and the "behavior changes" / "breaking changes" / "library upgrades" sections.
4. Feed the source and target DBR major versions to `scripts/scan_dbr_readiness.py` so it only flags changes that fall **within** the jump (a finding that was introduced before the source version is already absorbed and is not a risk).
