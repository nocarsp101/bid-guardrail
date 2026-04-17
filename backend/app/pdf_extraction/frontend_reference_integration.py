"""
C98 — Frontend / API reference integration.

Reference integration that consumes the existing API/control-room
endpoints for package overview, bid readiness, authority action,
timeline, and exports. Canonical payloads only. This module never
recomputes business truth — it just traces the call shape a frontend
would use so UI layers can bind against the exact endpoints that are
already served.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

FRONTEND_INTEGRATION_VERSION = "frontend_reference_integration/v1"


class ControlRoomReferenceClient:
    """Direct-call reference client over the orchestration/view layer.

    Uses the same functions the FastAPI endpoints use, so frontend code
    and the HTTP API observe the same canonical payloads.
    """

    def __init__(self, repository: Any) -> None:
        self._repo = repository

    # --- control-room views ------------------------------------------------
    def package_overview(self, bid_id: str) -> Dict[str, Any]:
        from .control_room_assembly import assemble_package_overview_payload
        return assemble_package_overview_payload(self._repo, bid_id)

    def bid_readiness(self, bid_id: str) -> Dict[str, Any]:
        from .control_room_assembly import assemble_bid_readiness_payload
        return assemble_bid_readiness_payload(self._repo, bid_id)

    def authority_action(self, bid_id: Optional[str] = None) -> Dict[str, Any]:
        from .control_room_assembly import assemble_authority_action_payload
        return assemble_authority_action_payload(self._repo, bid_id)

    def quote_case(self, job_id: str) -> Dict[str, Any]:
        from .control_room_assembly import assemble_quote_case_payload
        return assemble_quote_case_payload(self._repo, job_id)

    def timeline(self, bid_id: Optional[str] = None,
                  job_id: Optional[str] = None,
                  artifact_kinds: Optional[List[str]] = None) -> Dict[str, Any]:
        from .control_room_assembly import assemble_timeline_payload
        return assemble_timeline_payload(self._repo, bid_id=bid_id,
                                          job_id=job_id,
                                          artifact_kinds=artifact_kinds)

    # --- exports -----------------------------------------------------------
    def export_bid_readiness(self, bid_id: str,
                              revision_sequence: Optional[int] = None) -> Dict[str, Any]:
        from .export_orchestration import generate_bid_readiness_export
        return generate_bid_readiness_export(self._repo, bid_id, revision_sequence)

    def export_authority_action(self, bid_id: str,
                                 revision_sequence: Optional[int] = None) -> Dict[str, Any]:
        from .export_orchestration import generate_authority_action_export
        return generate_authority_action_export(self._repo, bid_id, revision_sequence)

    def export_final_carry(self, bid_id: str,
                            revision_sequence: Optional[int] = None) -> Dict[str, Any]:
        from .export_orchestration import generate_final_carry_export
        return generate_final_carry_export(self._repo, bid_id, revision_sequence)

    # --- reports -----------------------------------------------------------
    def report_bid_readiness(self, bid_id: str) -> Dict[str, Any]:
        from .render_reports import build_bid_readiness_report
        return build_bid_readiness_report(self._repo, bid_id)

    def report_authority_action(self, bid_id: str) -> Dict[str, Any]:
        from .render_reports import build_authority_action_report
        return build_authority_action_report(self._repo, bid_id)

    def report_final_carry(self, bid_id: str) -> Dict[str, Any]:
        from .render_reports import build_final_carry_report
        return build_final_carry_report(self._repo, bid_id)

    def report_estimator_review(self, job_id: str) -> Dict[str, Any]:
        from .render_reports import build_estimator_review_report
        return build_estimator_review_report(self._repo, job_id)

    # --- integration bundles ----------------------------------------------
    def bid_overview_bundle(self, bid_id: str) -> Dict[str, Any]:
        """Single canonical bundle a frontend bid page would bind to."""
        return {
            "frontend_integration_version": FRONTEND_INTEGRATION_VERSION,
            "bid_id": bid_id,
            "package_overview": self.package_overview(bid_id),
            "bid_readiness": self.bid_readiness(bid_id),
            "authority_action": self.authority_action(bid_id),
            "timeline": self.timeline(bid_id=bid_id),
            "ui_integration_pack": _ui_pack(),
        }

    def quote_case_bundle(self, job_id: str) -> Dict[str, Any]:
        return {
            "frontend_integration_version": FRONTEND_INTEGRATION_VERSION,
            "job_id": job_id,
            "quote_case": self.quote_case(job_id),
            "timeline": self.timeline(job_id=job_id,
                                       artifact_kinds=["quote_dossier"]),
            "ui_integration_pack": _ui_pack(),
        }


def build_integration_manifest() -> Dict[str, Any]:
    """Return a stable manifest of the endpoints a UI should bind to.

    Complements the C90 UI integration pack by grouping endpoints into
    frontend-facing lanes (overview, actions, reports, history).
    """
    return {
        "frontend_integration_version": FRONTEND_INTEGRATION_VERSION,
        "lanes": [
            {
                "lane_id": "bid_overview",
                "title": "Bid Overview Lane",
                "endpoints": [
                    "/control-room/package-overview/{bid_id}",
                    "/control-room/bid-readiness/{bid_id}",
                    "/control-room/authority-action",
                    "/control-room/timeline",
                ],
                "export_endpoints": [
                    "/exports/bid-readiness/{bid_id}",
                    "/exports/authority-action/{bid_id}",
                    "/exports/final-carry/{bid_id}",
                ],
                "report_endpoints": [
                    "/api/reports/bid-readiness",
                    "/api/reports/authority-action",
                    "/api/reports/final-carry",
                ],
            },
            {
                "lane_id": "quote_case",
                "title": "Quote Case Lane",
                "endpoints": [
                    "/control-room/quote-case/{job_id}",
                    "/control-room/timeline",
                ],
                "export_endpoints": [
                    "/exports/sub-clarification/{job_id}",
                    "/exports/estimator-review/{job_id}",
                ],
                "report_endpoints": [
                    "/api/reports/estimator-review",
                ],
            },
            {
                "lane_id": "operations",
                "title": "Operations & Admin Lane",
                "endpoints": [
                    "/api/diagnostics",
                    "/api/smoke-harness",
                    "/api/acceptance",
                    "/api/authorization/summary",
                    "/api/idempotency/summary",
                ],
                "export_endpoints": [],
                "report_endpoints": [],
            },
        ],
    }


def _ui_pack() -> Dict[str, Any]:
    from .ui_integration_pack import get_ui_integration_pack
    return get_ui_integration_pack()
