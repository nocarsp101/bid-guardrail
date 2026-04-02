# backend/app/export_report.py
"""
Export renderers for the operator report.

Pure functions — take an operator report dict (from build_operator_report)
and produce export strings (HTML, CSV). No validation logic, no side effects.
"""
from __future__ import annotations

import csv
import io
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# CSV export — one row per finding, flat and spreadsheet-ready
# ---------------------------------------------------------------------------

_CSV_COLUMNS = [
    "severity",
    "category",
    "item_ref",
    "message",
    "quote_unit_price",
    "bid_unit_price",
    "quote_qty",
    "bid_qty",
    "quote_unit",
    "bid_unit",
    "delta",
    "row_index",
]


def render_csv(report: Dict[str, Any]) -> str:
    """
    Render all findings as a flat CSV.

    Each raw finding becomes one row. Metadata fields are flattened into columns.
    The first rows are a header comment block with run_summary, mapping, and next_action.
    """
    buf = io.StringIO()
    writer = csv.writer(buf)

    # --- header comment block (prefixed with #) ---
    rs = report.get("run_summary", {})
    mp = report.get("mapping_provenance", {})
    na = report.get("next_action", {})
    counts = report.get("counts", {})

    writer.writerow(["# Bid Guardrail Validation Report"])
    writer.writerow([f"# Run ID: {rs.get('run_id', '')}"])
    writer.writerow([f"# Status: {rs.get('overall_status', '')} — {rs.get('status_description', '')}"])
    writer.writerow([f"# Mapping: {mp.get('description', '')}"])
    writer.writerow([f"# Next action: [{na.get('action', '')}] {na.get('description', '')}"])
    if counts.get("matched") is not None:
        writer.writerow([
            f"# Counts: matched={counts.get('matched', 0)}"
            f" unmatched={counts.get('unmatched', 0)}"
            f" price_violations={counts.get('price_violations', 0)}"
            f" qty_mismatches={counts.get('quantity_mismatches', 0)}"
        ])
    writer.writerow([])

    # --- data ---
    writer.writerow(_CSV_COLUMNS)

    detail = report.get("detail", {})
    findings = detail.get("findings", [])

    for f in findings:
        meta = f.get("meta", {})
        # Determine category from type
        ftype = f.get("type", "")
        if "unmatched" in ftype:
            cat = "unmatched"
        elif "price" in ftype:
            cat = "price"
        elif "unit_mismatch" in ftype:
            cat = "unit_mismatch"
        elif "quantity" in ftype:
            cat = "qty_mismatch"
        elif "missing" in ftype:
            cat = "missing_data"
        else:
            cat = ftype

        writer.writerow([
            f.get("severity", ""),
            cat,
            f.get("item_ref", ""),
            f.get("message", ""),
            meta.get("quote_unit_price", ""),
            meta.get("bid_unit_price", ""),
            meta.get("quote_qty", ""),
            meta.get("bid_qty", ""),
            meta.get("quote_unit", ""),
            meta.get("bid_unit", ""),
            meta.get("delta", ""),
            f.get("row_index", ""),
        ])

    return buf.getvalue()


# ---------------------------------------------------------------------------
# HTML export — self-contained, printable, no external dependencies
# ---------------------------------------------------------------------------

_STATUS_COLORS = {
    "PASS": "#16a34a",
    "WARN": "#ca8a04",
    "FAIL": "#dc2626",
}


def render_html(report: Dict[str, Any]) -> str:
    """Render a self-contained HTML report from the operator report dict."""
    rs = report.get("run_summary", {})
    mp = report.get("mapping_provenance", {})
    counts = report.get("counts", {})
    kf = report.get("key_findings", [])
    na = report.get("next_action", {})
    detail = report.get("detail", {})
    qs = detail.get("quote_summary") or {}

    status = rs.get("overall_status", "UNKNOWN")
    color = _STATUS_COLORS.get(status, "#333")

    sections: List[str] = []

    # --- Status banner ---
    sections.append(f"""
    <div style="background:{color};color:#fff;padding:16px 20px;border-radius:8px;margin-bottom:18px">
      <h2 style="margin:0">{_esc(status)}</h2>
      <div style="margin-top:4px;opacity:.9">{_esc(rs.get('status_description', ''))}</div>
      <div style="margin-top:8px;font-size:13px;opacity:.8">
        Run: <b>{_esc(rs.get('run_id', ''))}</b> &middot;
        Type: <b>{_esc(rs.get('doc_type', ''))}</b> &middot;
        Findings: {rs.get('total_findings', 0)}
        (FAIL: {rs.get('fail_count', 0)}, WARN: {rs.get('warn_count', 0)})
      </div>
    </div>""")

    # --- Mapping provenance ---
    sections.append(f"""
    <div style="background:#f0f4ff;padding:12px 16px;border-radius:8px;margin-bottom:14px;border-left:4px solid #3b82f6">
      <b>Mapping:</b> {_esc(mp.get('description', ''))}
      {_mapping_detail(mp)}
    </div>""")

    # --- Counts ---
    if counts.get("matched") is not None:
        sections.append(_counts_table(counts))

    # --- Next action ---
    sections.append(f"""
    <div style="background:#fffbeb;padding:12px 16px;border-radius:8px;margin-bottom:14px;border-left:4px solid #f59e0b">
      <b>Next action:</b> <code>{_esc(na.get('action', ''))}</code><br>
      {_esc(na.get('description', ''))}
    </div>""")

    # --- Key findings ---
    if kf:
        sections.append("<h3>Key Findings</h3>")
        for group in kf:
            sev = group.get("severity", "")
            gc = _STATUS_COLORS.get(sev, "#333")
            sections.append(f"""
    <div style="margin-bottom:12px;padding:10px 14px;border-left:4px solid {gc};background:#fafafa;border-radius:4px">
      <b style="color:{gc}">{_esc(sev)}</b> &mdash; {_esc(group.get('summary', ''))}
    </div>""")

    # --- Comparisons table ---
    comparisons = qs.get("comparisons", [])
    if comparisons:
        sections.append(_comparisons_table(comparisons))

    body = "\n".join(sections)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Validation Report — {_esc(rs.get('run_id', ''))}</title>
<style>
  body {{ font-family: system-ui, Arial, sans-serif; max-width: 960px; margin: 30px auto; padding: 0 16px; color: #1a1a1a; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 16px; font-size: 13px; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; }}
  th {{ background: #f5f5f5; font-weight: 600; }}
  tr:nth-child(even) {{ background: #fafafa; }}
  code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 4px; font-size: 12px; }}
  h3 {{ margin-top: 24px; }}
</style>
</head>
<body>
<h1>Bid Guardrail &mdash; Validation Report</h1>
{body}
<hr style="margin-top:30px;border:none;border-top:1px solid #ddd">
<div style="font-size:12px;opacity:.6;margin-top:8px">
  Generated from run {_esc(rs.get('run_id', ''))}. Full structured detail available via /validate/export/json.
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def _esc(val: Any) -> str:
    """Minimal HTML escaping."""
    s = str(val) if val is not None else ""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _mapping_detail(mp: dict) -> str:
    parts = []
    if mp.get("mapping_name_used"):
        parts.append(f"Name: <code>{_esc(mp['mapping_name_used'])}</code>")
    if mp.get("mapping_source"):
        parts.append(f"Source: <code>{_esc(mp['mapping_source'])}</code>")
    if not parts:
        return ""
    return f"<div style='font-size:13px;margin-top:4px;opacity:.8'>{' &middot; '.join(parts)}</div>"


def _counts_table(counts: dict) -> str:
    rows = "".join(
        f"<tr><td><b>{_esc(label)}</b></td><td>{_esc(counts.get(key, ''))}</td></tr>"
        for key, label in [
            ("bid_items_in_file", "Bid items in file"),
            ("quote_lines_in_file", "Quote lines in file"),
            ("matched", "Matched"),
            ("unmatched", "Unmatched"),
            ("price_violations", "Price violations"),
            ("unit_mismatches", "Unit mismatches"),
            ("quantity_mismatches", "Quantity mismatches"),
            ("totals_mismatch", "Totals mismatch"),
        ]
        if counts.get(key) is not None
    )
    return f"""
    <h3>Counts</h3>
    <table style="width:auto"><tbody>{rows}</tbody></table>"""


def _comparisons_table(comparisons: list) -> str:
    header = "<tr>" + "".join(
        f"<th>{_esc(h)}</th>"
        for h in ["Item", "Match Method", "Quote Qty", "Bid Qty",
                   "Quote Unit Price", "Bid Unit Price", "Quote Unit", "Bid Unit"]
    ) + "</tr>"

    rows = []
    for c in comparisons:
        rows.append("<tr>" + "".join(f"<td>{_esc(v)}</td>" for v in [
            c.get("match_key_used", c.get("quote_item", "")),
            c.get("match_method", ""),
            c.get("quote_qty", ""),
            c.get("bid_qty", ""),
            c.get("quote_unit_price", ""),
            c.get("bid_unit_price", ""),
            c.get("quote_unit", ""),
            c.get("bid_unit", ""),
        ]) + "</tr>")

    return f"""
    <h3>Line-by-Line Comparisons</h3>
    <table>{header}{''.join(rows)}</table>"""
