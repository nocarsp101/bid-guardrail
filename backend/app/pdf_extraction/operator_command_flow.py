"""
C106 — Operator command flow layer.

Deterministic app-facing workflow command handlers. Dispatches to
existing operator workflow actions (C101), report delivery /
download (C100/C105), and the product demo (C103). Append-only
revision behavior is preserved — this layer never mutates historical
records in place and emits a full command receipt for traceability.
"""
from __future__ import annotations
import hashlib
import json
from copy import deepcopy
from threading import RLock
from typing import Any, Dict, List, Optional

COMMAND_FLOW_VERSION = "operator_command_flow/v1"

# Closed command vocabulary ------------------------------------------------
CMD_ACKNOWLEDGE_REVIEW = "acknowledge_review"
CMD_CLARIFICATION_ADVANCE = "clarification_advance"
CMD_CARRY_ADVANCE = "carry_advance"
CMD_ACKNOWLEDGE_ITEM = "acknowledge_item"
CMD_CAPTURE_NOTE = "capture_note"
CMD_DELIVER_REPORT = "deliver_report"
CMD_DOWNLOAD_REPORT = "download_report"
CMD_RUN_PRODUCT_DEMO = "run_product_demo"

_ALL_COMMANDS = frozenset({
    CMD_ACKNOWLEDGE_REVIEW, CMD_CLARIFICATION_ADVANCE,
    CMD_CARRY_ADVANCE, CMD_ACKNOWLEDGE_ITEM,
    CMD_CAPTURE_NOTE, CMD_DELIVER_REPORT,
    CMD_DOWNLOAD_REPORT, CMD_RUN_PRODUCT_DEMO,
})

# Status vocabulary --------------------------------------------------------
STATUS_OK = "ok"
STATUS_UNKNOWN_COMMAND = "unknown_command"
STATUS_MISSING_FIELD = "missing_field"
STATUS_UPSTREAM_FAILURE = "upstream_failure"


class CommandReceiptLog:
    """Append-only receipt log for operator commands.

    Keeps an ordered, deep-copied history of every command attempt and
    its outcome. Used for traceability and replay diagnostics.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._receipts: List[Dict[str, Any]] = []

    def append(self, receipt: Dict[str, Any]) -> int:
        with self._lock:
            self._receipts.append(deepcopy(receipt))
            return len(self._receipts) - 1

    def all_receipts(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [deepcopy(r) for r in self._receipts]

    def clear(self) -> None:
        with self._lock:
            self._receipts.clear()

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            by_cmd: Dict[str, int] = {}
            by_status: Dict[str, int] = {}
            for r in self._receipts:
                cmd = r.get("command")
                st = r.get("status")
                by_cmd[cmd] = by_cmd.get(cmd, 0) + 1
                by_status[st] = by_status.get(st, 0) + 1
            return {
                "command_flow_version": COMMAND_FLOW_VERSION,
                "receipt_count": len(self._receipts),
                "by_command": dict(sorted(by_cmd.items())),
                "by_status": dict(sorted(by_status.items())),
            }


_DEFAULT_LOG = CommandReceiptLog()


def get_default_receipt_log() -> CommandReceiptLog:
    return _DEFAULT_LOG


def reset_default_receipt_log() -> None:
    global _DEFAULT_LOG
    _DEFAULT_LOG = CommandReceiptLog()


def list_commands() -> List[str]:
    return sorted(_ALL_COMMANDS)


def execute_command(
    repository: Any,
    command: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    issued_by: Optional[str] = None,
    issued_at: Optional[str] = None,
    log: Optional[CommandReceiptLog] = None,
) -> Dict[str, Any]:
    """Deterministic dispatch for a single operator command."""
    log = log or _DEFAULT_LOG
    payload = payload or {}
    command_id = _command_id(command, payload, issued_by, issued_at)

    if command not in _ALL_COMMANDS:
        receipt = _receipt(command_id, command, payload,
                            STATUS_UNKNOWN_COMMAND,
                            issued_by=issued_by, issued_at=issued_at,
                            result={"reason": "unknown_command"})
        log.append(receipt)
        return receipt

    try:
        result = _dispatch(repository, command, payload,
                            issued_by=issued_by, issued_at=issued_at)
    except Exception as exc:  # fail-closed; surface as upstream failure
        receipt = _receipt(command_id, command, payload,
                            STATUS_UPSTREAM_FAILURE,
                            issued_by=issued_by, issued_at=issued_at,
                            result={"error": repr(exc)})
        log.append(receipt)
        return receipt

    status = _infer_status(command, result)
    receipt = _receipt(command_id, command, payload, status,
                        issued_by=issued_by, issued_at=issued_at,
                        result=result)
    log.append(receipt)
    return receipt


# ----------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------


def _dispatch(repository: Any, command: str, payload: Dict[str, Any],
               issued_by: Optional[str],
               issued_at: Optional[str]) -> Dict[str, Any]:
    if command == CMD_ACKNOWLEDGE_REVIEW:
        from .operator_workflow_actions import acknowledge_review
        return acknowledge_review(
            repository, bid_id=payload.get("bid_id"),
            acknowledged_by=issued_by or payload.get("acknowledged_by"),
            acknowledged_at=issued_at or payload.get("acknowledged_at"),
            note=payload.get("note"))

    if command == CMD_CLARIFICATION_ADVANCE:
        from .operator_workflow_actions import advance_clarification
        return advance_clarification(
            repository, job_id=payload.get("job_id"),
            clarification_id=payload.get("clarification_id"),
            next_state=payload.get("next_state"),
            advanced_by=issued_by or payload.get("advanced_by"),
            advanced_at=issued_at or payload.get("advanced_at"),
            note=payload.get("note"))

    if command == CMD_CARRY_ADVANCE:
        from .operator_workflow_actions import advance_carry_decision
        return advance_carry_decision(
            repository, bid_id=payload.get("bid_id"),
            next_state=payload.get("next_state"),
            advanced_by=issued_by or payload.get("advanced_by"),
            advanced_at=issued_at or payload.get("advanced_at"),
            note=payload.get("note"))

    if command == CMD_ACKNOWLEDGE_ITEM:
        from .operator_workflow_actions import acknowledge_item
        return acknowledge_item(
            repository, bid_id=payload.get("bid_id"),
            item_id=payload.get("item_id"),
            acknowledged_by=issued_by or payload.get("acknowledged_by"),
            acknowledged_at=issued_at or payload.get("acknowledged_at"),
            note=payload.get("note"))

    if command == CMD_CAPTURE_NOTE:
        return _capture_note(repository, payload,
                              issued_by=issued_by, issued_at=issued_at)

    if command == CMD_DELIVER_REPORT:
        from .report_delivery import deliver_report
        return deliver_report(payload.get("report") or {},
                                output_format=payload.get("format", "json"))

    if command == CMD_DOWNLOAD_REPORT:
        from .report_download_flow import build_downloadable
        return build_downloadable(
            repository,
            payload.get("report_kind"),
            bid_id=payload.get("bid_id"),
            job_id=payload.get("job_id"),
            revision_sequence=payload.get("revision_sequence"),
            output_format=payload.get("format", "json"))

    if command == CMD_RUN_PRODUCT_DEMO:
        from .product_demo_flow import run_product_demo
        return run_product_demo(
            scenario_id=payload.get("scenario_id", "proceed_with_caveats"),
            repository=repository)

    return {"status": STATUS_UNKNOWN_COMMAND}


def _capture_note(repository: Any, payload: Dict[str, Any],
                   *, issued_by: Optional[str],
                   issued_at: Optional[str]) -> Dict[str, Any]:
    """Append an operator note to a canonical readiness snapshot."""
    bid_id = payload.get("bid_id")
    note = payload.get("note")
    if not bid_id or not note:
        return {"status": STATUS_MISSING_FIELD,
                "missing": [k for k, v in (("bid_id", bid_id),
                                             ("note", note)) if not v]}

    prior = repository.latest("bid_readiness_snapshot", bid_id=bid_id)
    if prior is None:
        return {"status": "record_not_found",
                "artifact_type": "bid_readiness_snapshot",
                "bid_id": bid_id}

    base = deepcopy((prior.get("envelope") or {}).get("artifact") or {})
    notes = list(base.get("operator_notes") or [])
    notes.append({
        "note": note,
        "authored_by": issued_by or payload.get("authored_by"),
        "authored_at": issued_at or payload.get("authored_at"),
        "tag": payload.get("tag"),
    })
    base["operator_notes"] = notes
    new_rec = repository.save("bid_readiness_snapshot", base,
                                metadata={"created_by": issued_by
                                            or "operator",
                                            "created_at": issued_at})
    return {
        "operator_command_flow_version": COMMAND_FLOW_VERSION,
        "status": STATUS_OK,
        "command": CMD_CAPTURE_NOTE,
        "bid_id": bid_id,
        "prior_record_id": prior.get("record_id"),
        "new_record_id": new_rec.get("record_id"),
        "revision_sequence": new_rec.get("revision_sequence"),
    }


def _infer_status(command: str, result: Dict[str, Any]) -> str:
    if not isinstance(result, dict):
        return STATUS_UPSTREAM_FAILURE
    if command == CMD_DELIVER_REPORT:
        return STATUS_OK if result.get("delivery_status") == "ok" \
            else STATUS_UPSTREAM_FAILURE
    if command == CMD_DOWNLOAD_REPORT:
        return STATUS_OK if result.get("download_status") == "ok" \
            else STATUS_UPSTREAM_FAILURE
    if command == CMD_RUN_PRODUCT_DEMO:
        return STATUS_OK if result.get("all_stages_ok") is True \
            else STATUS_UPSTREAM_FAILURE
    status = result.get("status")
    return status if status == STATUS_OK else (status or STATUS_UPSTREAM_FAILURE)


def _receipt(command_id: str, command: str, payload: Dict[str, Any],
              status: str, *, issued_by: Optional[str],
              issued_at: Optional[str],
              result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "operator_command_flow_version": COMMAND_FLOW_VERSION,
        "command_id": command_id,
        "command": command,
        "status": status,
        "payload": deepcopy(payload),
        "result": deepcopy(result),
        "issued_by": issued_by,
        "issued_at": issued_at,
    }


def _command_id(command: str, payload: Dict[str, Any],
                 issued_by: Optional[str],
                 issued_at: Optional[str]) -> str:
    material = json.dumps({
        "command": command,
        "payload": payload,
        "issued_by": issued_by,
        "issued_at": issued_at,
    }, sort_keys=True, separators=(",", ":"), default=repr)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
