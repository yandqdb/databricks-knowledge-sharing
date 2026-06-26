#!/usr/bin/env python3
"""Score the migration QA of one notebook, or roll a batch up into a success rate.

Reads the JSON emitted by the other three scripts and applies the pass criteria
from references/metrics-and-method.md section 4:

    pass = runs AND (parity == pass) AND (change_coverage >= change_gate)

When parity is `not_evaluated` (no baseline), pass falls back to
`runs AND change_coverage >= change_gate` and the scorecard flags parity as not
evaluated, so a run is never reported as a clean success without saying so. The
change gate defaults to 1.0 (100% change coverage required) and is stated in the
output whenever it is relaxed.

Single notebook:
    python qa_report.py --change CHG.json --run RUN.json [--parity PAR.json] \
        [--change-gate 1.0] --out report.md [--html report.html]

Batch (a directory of <name>.change.json / <name>.run.json / <name>.parity.json):
    python qa_report.py --batch DIR [--change-gate 1.0] --out batch.md [--html batch.html]

Markdown is always written. HTML is optional. A machine-readable `*.json` summary
is always written next to the Markdown.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys

DEFAULT_GATE = 1.0


def _load(path):
    if not path:
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _pct(x):
    return "n/a" if x is None else f"{x * 100:.1f}%"


def score(change, run, parity, gate: float) -> dict:
    """Apply the pass criteria; return a flat scorecard dict for one notebook."""
    coverage = change["overall"]["coverage"] if change else None
    runs = bool(run["runs"]) if run else False
    exec_cov = run.get("execution_coverage") if run else None
    parity_state = (parity or {}).get("parity", "not_evaluated")

    change_ok = coverage is not None and coverage >= gate
    parity_evaluated = parity_state != "not_evaluated"
    if parity_evaluated:
        passed = runs and parity_state == "pass" and change_ok
    else:
        passed = runs and change_ok

    name = (run or {}).get("name") or (parity or {}).get("name")
    if not name and change:
        name = os.path.basename(change.get("migrated") or change.get("original") or "")
    return {
        "name": name or "(unnamed)",
        "change_coverage": coverage,
        "execution_coverage": exec_cov,
        "executable_cells": (run or {}).get("executable_cells"),
        "executed_cells": (run or {}).get("executed_cells"),
        "runs": runs,
        "state": (run or {}).get("state"),
        "first_failing_cell": (run or {}).get("first_failing_cell"),
        "error": (run or {}).get("error"),
        "parity": parity_state,
        "parity_evaluated": parity_evaluated,
        "change_ok": change_ok,
        "pass": passed,
    }


def _mean(values):
    vals = [v for v in values if v is not None]
    return round(sum(vals) / len(vals), 4) if vals else None


def batch_summary(cards: list, gate: float) -> dict:
    total = len(cards)
    passes = sum(1 for c in cards if c["pass"])
    return {
        "change_gate": gate,
        "total": total,
        "passing": passes,
        "success_rate": round(100.0 * passes / total, 1) if total else 0.0,
        "avg_change_coverage": _mean(c["change_coverage"] for c in cards),
        "avg_execution_coverage": _mean(c["execution_coverage"] for c in cards),
        "notebooks": cards,
    }


# ---- rendering -------------------------------------------------------------

def _verdict(card):
    return "PASS" if card["pass"] else "FAIL"


def render_single_md(card: dict, gate: float, change: dict) -> str:
    L = [f"# Migration QA — {card['name']}", ""]
    L.append(f"**Result: {_verdict(card)}**  (change gate {_pct(gate)})")
    L.append("")
    L.append("| metric | value |")
    L.append("| --- | --- |")
    L.append(f"| change coverage | {_pct(card['change_coverage'])} "
             f"{'✓' if card['change_ok'] else '✗'} |")
    ex = card["execution_coverage"]
    cells = ""
    if card["executable_cells"] is not None:
        cells = f" ({card['executed_cells']}/{card['executable_cells']} cells)"
    L.append(f"| execution coverage | {_pct(ex)}{cells} |")
    L.append(f"| runs | {'yes' if card['runs'] else 'no'} |")
    parity = card["parity"] + (" (not evaluated)" if not card["parity_evaluated"] else "")
    L.append(f"| parity | {parity} |")
    L.append("")
    if not card["parity_evaluated"]:
        L.append("> Parity was not evaluated (no baseline for the output tables). "
                 "Pass falls back to runs + change gate.")
        L.append("")
    if card["first_failing_cell"] is not None:
        L.append(f"First failing cell: **{card['first_failing_cell']}**")
    if card["error"]:
        L.append(f"Error: `{card['error']}`")
        L.append("")
    residual = (change or {}).get("residual_sites") or []
    if residual:
        L.append(f"## Residual sites to hand-fix ({len(residual)})")
        L.append("")
        L.append("| category | cell | line | snippet |")
        L.append("| --- | --- | --- | --- |")
        for s in residual:
            snip = str(s.get("snippet", "")).replace("|", "\\|")
            L.append(f"| {s['category']} | {s['cell_index']} | "
                     f"{s['line_in_cell']} | `{snip}` |")
        L.append("")
    return "\n".join(L) + "\n"


def render_batch_md(summary: dict) -> str:
    L = ["# Migration QA — batch rollup", ""]
    L.append(f"**Success rate: {summary['success_rate']:.1f}%** "
             f"({summary['passing']}/{summary['total']} notebooks pass, "
             f"change gate {_pct(summary['change_gate'])})")
    L.append("")
    L.append(f"- average change coverage: {_pct(summary['avg_change_coverage'])}")
    L.append(f"- average execution coverage: {_pct(summary['avg_execution_coverage'])}")
    L.append("")
    L.append("| notebook | change | execution | runs | parity | result |")
    L.append("| --- | --- | --- | --- | --- | --- |")
    # failures first, then by name
    for c in sorted(summary["notebooks"], key=lambda c: (c["pass"], c["name"])):
        parity = c["parity"] + ("*" if not c["parity_evaluated"] else "")
        L.append(f"| {c['name']} | {_pct(c['change_coverage'])} | "
                 f"{_pct(c['execution_coverage'])} | "
                 f"{'yes' if c['runs'] else 'no'} | {parity} | {_verdict(c)} |")
    L.append("")
    if any(not c["parity_evaluated"] for c in summary["notebooks"]):
        L.append("\\* parity not evaluated (no baseline); result is runs + change gate only.")
        L.append("")
    return "\n".join(L) + "\n"


def render_html(title: str, body_md: str) -> str:
    """Minimal self-contained HTML: the Markdown verbatim in a <pre> block."""
    esc = (body_md.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{title}</title>"
        "<style>body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;"
        "margin:2rem;color:#1b1b1b} pre{white-space:pre-wrap;font:14px/1.5 "
        "ui-monospace,SFMono-Regular,Menlo,monospace}</style></head><body>"
        f"<pre>{esc}</pre></body></html>\n")


def _summary_path(out: str) -> str:
    if out.lower().endswith(".md"):
        return out[:-3] + ".json"
    return out + ".json"


def _write(out, html, md_text, summary_obj, html_title):
    with open(out, "w", encoding="utf-8") as f:
        f.write(md_text)
    sp = _summary_path(out)
    with open(sp, "w", encoding="utf-8") as f:
        json.dump(summary_obj, f, indent=2)
    written = [out, sp]
    if html:
        with open(html, "w", encoding="utf-8") as f:
            f.write(render_html(html_title, md_text))
        written.append(html)
    return written


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--change", help="change_coverage.py JSON (single mode)")
    ap.add_argument("--run", help="run_notebook.py JSON (single mode)")
    ap.add_argument("--parity", help="compare_baseline.py JSON (single mode, optional)")
    ap.add_argument("--batch", help="directory of per-notebook json triples")
    ap.add_argument("--change-gate", type=float, default=DEFAULT_GATE,
                    help="minimum change coverage to pass (default 1.0)")
    ap.add_argument("--out", required=True, help="Markdown report path")
    ap.add_argument("--html", help="optional HTML report path")
    args = ap.parse_args(argv)

    gate = args.change_gate

    if args.batch:
        d = os.path.expanduser(args.batch)
        if not os.path.isdir(d):
            print(f"error: batch dir not found: {d}", file=sys.stderr)
            return 2
        names = sorted({os.path.basename(p)[:-len(".change.json")]
                        for p in glob.glob(os.path.join(d, "*.change.json"))} |
                       {os.path.basename(p)[:-len(".run.json")]
                        for p in glob.glob(os.path.join(d, "*.run.json"))} |
                       {os.path.basename(p)[:-len(".parity.json")]
                        for p in glob.glob(os.path.join(d, "*.parity.json"))})
        if not names:
            print(f"error: no <name>.change/run/parity.json files in {d}",
                  file=sys.stderr)
            return 2
        cards = []
        for name in names:
            def _p(suffix):
                fp = os.path.join(d, f"{name}.{suffix}.json")
                return fp if os.path.isfile(fp) else None
            change = _load(_p("change"))
            run = _load(_p("run"))
            parity = _load(_p("parity"))
            card = score(change, run, parity, gate)
            card["name"] = name
            cards.append(card)
        summary = batch_summary(cards, gate)
        md = render_batch_md(summary)
        print(md, end="")
        written = _write(args.out, args.html, md, summary, "Migration QA batch")
        print("wrote " + ", ".join(written))
        return 0

    if not args.change or not args.run:
        print("error: single mode needs --change and --run (or use --batch)",
              file=sys.stderr)
        return 2
    change = _load(args.change)
    run = _load(args.run)
    parity = _load(args.parity)
    card = score(change, run, parity, gate)
    summary = {"change_gate": gate, "notebook": card}
    md = render_single_md(card, gate, change)
    print(md, end="")
    written = _write(args.out, args.html, md, summary, f"Migration QA {card['name']}")
    print("wrote " + ", ".join(written))
    return 0


if __name__ == "__main__":
    sys.exit(main())
