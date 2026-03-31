from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field

Severity = Literal["INFO", "PASS", "WARN", "FAIL"]


class Finding(BaseModel):
    type: str
    severity: Severity
    message: str
    pages: List[int] = Field(default_factory=list)  # for PDFs
    row_index: Optional[int] = None                 # for bid items
    item_ref: Optional[str] = None                  # item id/description helper
    meta: Dict[str, Any] = Field(default_factory=dict)


class OverrideInfo(BaseModel):
    override: bool = True
    override_reason: str
    override_actor: str
    override_timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )


class AuditEvent(BaseModel):
    event_type: str = "validation_run"  # e.g. validation_run, quote_upload, quote_comparison, override_action
    run_id: str
    timestamp_utc: str
    actor: str
    input_files: List[Dict[str, Any]]
    context: Dict[str, Any] = Field(default_factory=dict)  # doc_type etc
    checks_executed: List[str]
    findings: List[Finding]
    overall_status: str
    override: Optional[OverrideInfo] = None

    @classmethod
    def build(
        cls,
        run_id: str,
        actor: str,
        input_files: List[Dict[str, Any]],
        context: Dict[str, Any],
        checks_executed: List[str],
        findings: List[Finding],
        overall_status: str,
        override: Optional[OverrideInfo],
        event_type: str = "validation_run",
    ) -> "AuditEvent":
        return cls(
            event_type=event_type,
            run_id=run_id,
            timestamp_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            actor=actor,
            input_files=input_files,
            context=context,
            checks_executed=checks_executed,
            findings=findings,
            overall_status=overall_status,
            override=override,
        )
