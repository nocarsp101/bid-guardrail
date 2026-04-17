"""
C100 — Rendered report delivery path.

Deterministic delivery / output paths for render-ready reports.
Preserves state labels, revision metadata, and source refs. No new
inference is introduced in rendering — these functions only format
existing canonical payloads into operator-facing output forms.
"""
from __future__ import annotations
import json
import os
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional

REPORT_DELIVERY_VERSION = "report_delivery/v1"

# Closed output-format vocabulary -----------------------------------------
FORMAT_JSON = "json"
FORMAT_TEXT = "text"
FORMAT_MARKDOWN = "markdown"
FORMAT_STRUCTURED = "structured"

_ALL_FORMATS = frozenset({FORMAT_JSON, FORMAT_TEXT,
                           FORMAT_MARKDOWN, FORMAT_STRUCTURED})


def list_formats() -> List[str]:
    return sorted(_ALL_FORMATS)


def deliver_report(report: Dict[str, Any],
                    output_format: str = FORMAT_JSON) -> Dict[str, Any]:
    """Deterministic delivery envelope for a render-ready report."""
    if output_format not in _ALL_FORMATS:
        return _fail_envelope(report, f"unknown_format:{output_format}")
    body: Any
    if output_format == FORMAT_JSON:
        body = json.dumps(report, sort_keys=True, separators=(",", ":"),
                           default=repr)
        content_type = "application/json"
    elif output_format == FORMAT_TEXT:
        body = _render_text(report)
        content_type = "text/plain"
    elif output_format == FORMAT_MARKDOWN:
        body = _render_markdown(report)
        content_type = "text/markdown"
    else:  # structured
        body = _render_structured(report)
        content_type = "application/json"

    return {
        "report_delivery_version": REPORT_DELIVERY_VERSION,
        "delivery_status": "ok",
        "report_kind": report.get("report_kind"),
        "identity": deepcopy(report.get("identity") or {}),
        "state_labels": deepcopy(report.get("state_labels") or {}),
        "source_refs": deepcopy(report.get("source_refs") or []),
        "format": output_format,
        "content_type": content_type,
        "body": body,
        "byte_length": len(body) if isinstance(body, str) else None,
    }


def deliver_reports_batch(reports: Iterable[Dict[str, Any]],
                           output_format: str = FORMAT_JSON) -> Dict[str, Any]:
    items = [deliver_report(r, output_format=output_format) for r in reports]
    return {
        "report_delivery_version": REPORT_DELIVERY_VERSION,
        "delivery_status": "ok" if all(i.get("delivery_status") == "ok"
                                         for i in items) else "partial",
        "format": output_format,
        "report_count": len(items),
        "deliveries": items,
    }


def write_delivery_to_file(delivery: Dict[str, Any], path: str) -> Dict[str, Any]:
    """Persist a delivered report to disk. Deterministic naming."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    body = delivery.get("body")
    mode = "w"
    if isinstance(body, (dict, list)):
        data = json.dumps(body, sort_keys=True, separators=(",", ":"),
                           default=repr)
    elif body is None:
        data = ""
    else:
        data = str(body)
    with open(path, mode, encoding="utf-8") as f:
        f.write(data)
    return {
        "report_delivery_version": REPORT_DELIVERY_VERSION,
        "delivery_status": "ok",
        "path": path,
        "bytes_written": len(data),
    }


def deliver_all_for_bid(repository: Any, bid_id: str,
                         output_format: str = FORMAT_JSON) -> Dict[str, Any]:
    """Deliver every bid-level report for a given bid_id."""
    from .render_reports import (
        build_bid_readiness_report, build_authority_action_report,
        build_final_carry_report,
    )
    reports = [
        build_bid_readiness_report(repository, bid_id),
        build_authority_action_report(repository, bid_id),
        build_final_carry_report(repository, bid_id),
    ]
    return deliver_reports_batch(reports, output_format=output_format)


# ----------------------------------------------------------------------
# Renderers — no inference, format passthrough only
# ----------------------------------------------------------------------


def _render_text(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"{report.get('title') or report.get('report_kind') or 'Report'}")
    lines.append("=" * max(len(lines[-1]), 8))
    identity = report.get("identity") or {}
    if identity:
        lines.append("Identity:")
        for k, v in sorted(identity.items()):
            lines.append(f"  {k}: {v}")
    state_labels = report.get("state_labels") or {}
    if state_labels:
        lines.append("State:")
        for k, v in sorted(state_labels.items()):
            lines.append(f"  {k}: {v}")
    for section in report.get("sections") or []:
        lines.append("")
        lines.append(f"## {section.get('title') or section.get('section_id')}")
        body = section.get("body")
        lines.append(_text_body(body, indent="  "))
    refs = report.get("source_refs") or []
    if refs:
        lines.append("")
        lines.append("Source Refs:")
        for r in refs:
            lines.append(f"  - {r.get('artifact_type')}/"
                         f"{r.get('record_id')} rev="
                         f"{r.get('revision_sequence')}")
    return "\n".join(lines)


def _render_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# {report.get('title') or report.get('report_kind')}")
    identity = report.get("identity") or {}
    if identity:
        lines.append("")
        lines.append("**Identity**")
        for k, v in sorted(identity.items()):
            lines.append(f"- {k}: `{v}`")
    state = report.get("state_labels") or {}
    if state:
        lines.append("")
        lines.append("**State Labels**")
        for k, v in sorted(state.items()):
            lines.append(f"- {k}: `{v}`")
    for s in report.get("sections") or []:
        lines.append("")
        lines.append(f"## {s.get('title') or s.get('section_id')}")
        lines.append("")
        lines.append("```")
        lines.append(_text_body(s.get("body"), indent=""))
        lines.append("```")
    refs = report.get("source_refs") or []
    if refs:
        lines.append("")
        lines.append("## Source Refs")
        for r in refs:
            lines.append(f"- `{r.get('artifact_type')}` "
                         f"`{r.get('record_id')}` "
                         f"rev=`{r.get('revision_sequence')}`")
    return "\n".join(lines)


def _render_structured(report: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": report.get("title"),
        "report_kind": report.get("report_kind"),
        "identity": deepcopy(report.get("identity") or {}),
        "state_labels": deepcopy(report.get("state_labels") or {}),
        "sections": [
            {"section_id": s.get("section_id"),
              "title": s.get("title"),
              "body": deepcopy(s.get("body"))}
            for s in (report.get("sections") or [])
        ],
        "source_refs": deepcopy(report.get("source_refs") or []),
    }


def _text_body(body: Any, indent: str = "") -> str:
    if body is None:
        return f"{indent}(empty)"
    if isinstance(body, dict):
        return "\n".join(f"{indent}{k}: {v}"
                          for k, v in sorted(body.items(),
                                              key=lambda x: str(x[0])))
    if isinstance(body, list):
        return "\n".join(f"{indent}- {item!r}" for item in body)
    return f"{indent}{body!r}"


def _fail_envelope(report: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return {
        "report_delivery_version": REPORT_DELIVERY_VERSION,
        "delivery_status": "error",
        "report_kind": report.get("report_kind"),
        "reason": reason,
    }
