# Behavior-Change Detection Catalog

The full pattern catalog for DBR upgrade readiness. Each row: what to **detect**, the **category** (1 = bridgeable by config, 2 = code change, 3 = hard blocker), and the **fix / bridge**.

**Only flag a change that falls between the source and target DBR.** A change introduced in Spark 3.2 is irrelevant if the source is already on 3.3 — it is absorbed. Use the "Introduced in" column against the jump from [version-matrix.md](version-matrix.md).

---

## A. Spark SQL semantics

| ID | Detect | Introduced in | Cat | Severity | Fix / Bridge |
|----|--------|---------------|-----|----------|--------------|
| `ansi-cast` | `cast(...)` / `CAST(... AS ...)` of strings to numeric/date, integer division, inserts into typed columns | ANSI default flip | 2 | Blocker | Hand off to **`databricks-ansi-remediation`** (try_cast or session flag). Bridge: `spark.sql.ansi.enabled=false`. |
| `ansi-divide-zero` | `x / 0`, `mod(x,0)` in SQL or column expressions | ANSI default flip | 2 | Blocker | Guard denominators or use `try_divide`. Bridge: `spark.sql.ansi.enabled=false`. |
| `time-parser-legacy` | `to_date`/`to_timestamp`/`from_unixtime`/`unix_timestamp` with non-ISO patterns; week-year `YYYY`, day-of-week `u`/`e`, `Z` zone letters | Spark 3.0 | 1 | Blocker | Fix patterns to the Java 8 `DateTimeFormatter` spec, or bridge `spark.sql.legacy.timeParserPolicy=LEGACY`. |
| `decimal-arith` | Arithmetic on `DECIMAL` columns expecting old precision/scale, or overflow returning `NULL` | Spark 3.x | 1/2 | Warning | New behavior may raise on overflow under ANSI. Bridge: `spark.sql.legacy.allowNegativeScaleOfDecimal` / ANSI flag; or widen the decimal type. |
| `negative-scale-decimal` | `DECIMAL(p, -s)` literals/casts | Spark 3.0 | 1 | Warning | Bridge `spark.sql.legacy.allowNegativeScaleOfDecimal=true`, or remove negative scale. |
| `interval-literal` | `INTERVAL` literals and arithmetic; `CalendarInterval` vs ANSI day-time/year-month intervals | Spark 3.2 | 2 | Warning | New interval types split year-month from day-time. Rewrite literals to typed intervals. |
| `elt-array-oob` | `elt()`, `element_at()` with out-of-range index | ANSI | 1 | Warning | Returns error under ANSI instead of NULL. Guard index or use try_* variants. |
| `map-key-dup` | Building maps with duplicate keys (`map()`, `map_from_arrays`) | Spark 3.0 | 2 | Warning | Now raises instead of last-wins. Dedupe keys. |
| `string-to-bool` | Implicit string→boolean comparisons | Spark 3.x / ANSI | 1 | Info | Tighter coercion. Cast explicitly. |

## B. Datetime & calendar

| ID | Detect | Introduced in | Cat | Severity | Fix / Bridge |
|----|--------|---------------|-----|----------|--------------|
| `julian-gregorian` | Reading old Parquet/Avro written pre-Spark-3.0 with dates before 1582; `rebaseMode` | Spark 3.0 | 1 | Warning | Set `spark.sql.parquet.datetimeRebaseModeInRead`/`Write` and `avro.*` rebase modes to `LEGACY` or `CORRECTED` deliberately. |
| `spark-tz` | Code relying on JVM default timezone for timestamp parsing | Spark 3.x | 2 | Info | Set `spark.sql.session.timeZone` explicitly. |

## C. Python interpreter & stdlib

| ID | Detect | Introduced in | Cat | Severity | Fix / Bridge |
|----|--------|---------------|-----|----------|--------------|
| `py-distutils` | `import distutils`, `from distutils...` | Python 3.12 | 2 | Blocker | `distutils` removed. Use `setuptools`, `packaging`, or `sysconfig`. |
| `py-imp` | `import imp` | Python 3.4 dep / 3.12 removal | 2 | Blocker | Use `importlib`. |
| `py-asyncio-coroutine` | `@asyncio.coroutine`, `yield from` coroutine style | Python 3.11 | 2 | Warning | Use `async def` / `await`. |
| `py-collections-abc` | `collections.Mapping`/`Iterable`/`Sequence` (not `collections.abc.*`) | Python 3.10 | 2 | Warning | Import from `collections.abc`. |
| `py-fstring-debug` | n/a — newer syntax is fine going forward | — | — | — | Going to a newer interpreter rarely breaks syntax; the risk is *removed* stdlib and library wheels. |

## D. Preinstalled libraries

| ID | Detect | Jump | Cat | Severity | Fix |
|----|--------|------|-----|----------|-----|
| `pandas-append` | `df.append(` (pandas) | pandas 1.x → 2.x | 2 | Blocker | Removed. Use `pd.concat([df, other])`. |
| `pandas-iteritems` | `.iteritems()` | pandas 1.x → 2.x | 2 | Blocker | Use `.items()`. |
| `pandas-pd-np` | `pd.np.` | pandas 1.x → 2.x | 2 | Blocker | Import `numpy` directly. |
| `pandas-inplace` | chained `inplace=True` on a slice | pandas 2.x | 2 | Warning | Reassign instead of `inplace`. |
| `numpy-aliases` | `np.float`, `np.int`, `np.bool`, `np.object` | numpy 1.24+/2.x | 2 | Blocker | Use builtin `float`/`int`/`bool` or `np.float64` etc. |
| `mlflow-api` | `mlflow.<flavor>.log_model(...)` without signature; deprecated registry calls | MLflow 1→2→3 | 2 | Warning | Add `signature=`; update registry API. See `databricks-serverless-migration` MLflow-on-UC notes if also moving to UC. |
| `pinned-lib-incompat` | `%pip install <pkg>==<ver>` or `requirements.txt` pin that has no wheel for target Python/Spark | any | 3 | Blocker | Find a compatible version or replace the dependency. Hard blocker if none exists. |

## E. Platform / cluster config

| ID | Detect | Jump | Cat | Severity | Fix |
|----|--------|------|-----|----------|-----|
| `init-script-dbfs` | `init_scripts` pointing at `dbfs:/...`; legacy global / cluster-named init scripts | Disabled on newer DBR | 2 | Blocker | Relocate init scripts to workspace files, UC volumes, or cloud storage; update the job/cluster spec to the new path. |
| `log4j1-config` | `log4j.properties` (v1 syntax), custom v1 appenders in init scripts | Log4j 2 era (DBR 11+) | 2 | Warning | Migrate to `log4j2.xml` / Log4j 2 config. |
| `spark-conf-removed` | `spark.conf.set(...)` / `spark_conf` for a config removed or renamed in the target Spark | per Spark minor | 1/2 | Warning | Look up the config in the target Spark migration guide; remove or rename. Many are now auto-tuned. |
| `legacy-instance-type` | `node_type_id` for an instance type retired in the target DBR | cloud-specific | 2 | Warning | Pick a current instance type. |
| `dbfs-mount` | `dbutils.fs.mount(...)` / `/mnt/` paths | UC era | 2 | Info | Works on classic, but plan UC external locations if also doing UC. Not a hard DBR blocker. |

## F. Delta Lake & table protocol

| ID | Detect | Jump | Cat | Severity | Fix |
|----|--------|------|-----|----------|-----|
| `delta-protocol-upgrade` | Writes to existing Delta tables shared with readers still on the old DBR | newer DBR may bump protocol | 2 | Blocker (coordination) | Writing with new table features (deletion vectors, column mapping, etc.) can raise `minReaderVersion`/`minWriterVersion`, breaking old-DBR readers. Coordinate: upgrade readers first, or disable the new feature on the table. See [upgrade-workflow.md](upgrade-workflow.md). |
| `delta-default-flip` | Tables relying on a Delta default (auto-optimize, deletion vectors) that changed default | per DBR | 1 | Info | Set the table property explicitly to lock behavior. |

## G. Language / API

| ID | Detect | Jump | Cat | Severity | Fix |
|----|--------|------|-----|----------|-----|
| `sqlcontext` | `sqlContext.` , `SQLContext(` | deprecated | 2 | Warning | Use `spark.sql(...)` / `SparkSession`. |
| `removed-functions` | Spark functions/options removed in the target minor (check migration guide) | per Spark minor | 2 | Warning | Replace per the Spark SQL migration guide for the target version. |
| `scala-jar-spark-api` | Scala JAR (`spark_jar_task`, `build.sbt`/`pom.xml`) calling Spark internals | new Spark minor | 2 | Warning | Scala stays 2.12 on classic so usually no recompile, but Spark internal APIs may have moved. Re-test; rebuild against the target Spark if it calls non-stable APIs. |
| `r-workload` | `%r` cells / SparkR / sparklyr | per DBR | 1/3 | Warning | R is supported on classic DBR; confirm the package versions for the target DBR. Becomes a blocker only if a needed CRAN package has no compatible build. |

---

## Using this catalog

1. Filter rows to the jump: keep a row only if its "Introduced in" / "Jump" falls strictly **after** the source and **at or before** the target.
2. For each surviving row, scan the source for the detect pattern (`scripts/scan_dbr_readiness.py` automates the regex-detectable ones).
3. Confirm each hit by reading the surrounding code — many are benign (an ISO-8601 date format, a cast that is already safe, a numpy alias already removed).
4. Record category + severity + fix in the readiness report.
5. For `ansi-*` and HMS rows, hand off to the sibling skill rather than rewriting inline.
