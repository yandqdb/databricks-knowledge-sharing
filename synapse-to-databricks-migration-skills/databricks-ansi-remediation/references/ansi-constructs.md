# ANSI-affected constructs — catalog, behavior, and remediation

Reference for the `databricks-ansi-remediation` skill. Synapse Spark ran with
`spark.sql.ansi.enabled=false`; DBR runs ANSI **on** by default. The table below
is the authoritative list of what the detector flags, how behavior changes, and
which remediation applies.

## The core behavior change

| | ANSI off (Synapse source) | ANSI on (DBR target) |
|---|---|---|
| Invalid `CAST` (e.g. `'abc'` → int) | returns `NULL` | **raises** `CAST_INVALID_INPUT` |
| Numeric overflow on cast/arithmetic | wraps / returns NULL | **raises** `ARITHMETIC_OVERFLOW` |
| Divide by zero | returns `NULL` | **raises** `DIVIDE_BY_ZERO` |
| `to_date`/`to_timestamp` bad format | returns `NULL` | **raises** parse error |
| Insert out-of-range into typed column | stores NULL/truncates | **raises** |

## Construct catalog (what the detector flags)

| Construct | Confidence | What it is | Why it breaks under ANSI on |
|---|---|---|---|
| `EXPLICIT_CAST` | high | `cast(x as t)` / `CAST(x AS t)` | invalid value raises instead of → NULL |
| `DATE_PARSE` | high | `to_date`, `to_timestamp`, `unix_timestamp`, `to_unix_timestamp` | malformed string raises instead of → NULL |
| `INT_DIVISION` | medium | `a div b` | divide-by-zero raises |
| `INSERT_TYPED` | medium | `INSERT INTO/OVERWRITE` a typed table | out-of-range / wrong-type value raises |
| `IMPLICIT_NUMERIC` | low | quoted number in arithmetic (`'1.05' *`) | implicit string→number coercion may raise |

The `IMPLICIT_NUMERIC` heuristic is deliberately conservative and noisy — it is
a prompt to eyeball the line, not a guarantee of breakage. Implicit coercions in
comparisons (`where col = '123'`) are not reliably detectable by pattern and are
covered by the session-flag path, not by site rewriting.

## The two remediations

### 1. `try_cast` (site rewrite)
Rewrites explicit `cast(...)` → `try_cast(...)`. A `try_cast` returns `NULL` on a
bad value, which matches the **intent** of the original ANSI-off code.

Use when: NULL-on-bad-value is the desired/acceptable semantics and you want the
code to stay ANSI-clean going forward.

**Caveats — why this is not a drop-in identity:**
- `try_cast` only covers explicit casts. `DATE_PARSE`, `INT_DIVISION`,
  `INSERT_TYPED`, and implicit coercions are **not** remediated by this mode.
- `try_cast` is not bit-for-bit identical to ANSI-off `CAST` in every edge case
  (e.g. some overflow / rounding / string-trim corners differ).
- It changes the source text, so downstream readers see a different notebook.

### 2. Session flag (notebook-scope parity)
Injects `spark.conf.set("spark.sql.ansi.enabled", "false")` as a top cell,
restoring exact Synapse behavior for the **entire** notebook and **all** the
constructs above at once.

Use when: bit-for-bit parity matters, the notebook is being migrated as-is, or
there are many mixed constructs and per-site rewriting is risky. **This is the
default recommendation where exact parity is required.**

Trade-off: it pins the notebook to legacy semantics rather than making it
ANSI-native, so it is a migration aid, not an end state. Track these notebooks as
tech debt to revisit.

## Decision guidance

```
Many mixed constructs, or parity must be exact?      -> session flag
Only explicit casts, NULL-on-bad-value is fine?      -> try_cast
A few casts + date parsing + inserts together?       -> session flag (one safe step)
Want the notebook ANSI-native long term?             -> try_cast + manual fix of
                                                        date-parse/insert sites,
                                                        validate, then drop the flag
```

Whichever path: the engineer reviews the per-cell diff and approves. The skill
never writes without `--write`.

## Validating the fix

Confirm behavior with the post-conversion validation skill (or manually):
1. Run the affected cells **before** the fix on DBR → they should raise.
2. Apply the chosen remediation.
3. Run again → they should succeed; row counts / null rates should match the
   Synapse baseline (the session-flag path should match exactly; the try_cast
   path should match wherever inputs were valid, with NULLs where they weren't).

## Sources
- Spark ANSI compliance: https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
- Databricks ANSI mode: https://docs.databricks.com/en/sql/language-manual/sql-ref-ansi-compliance.html
- `try_cast`: https://docs.databricks.com/en/sql/language-manual/functions/try_cast.html
