"""
Phase B — /validate endpoint integration tests.
Tests the FastAPI endpoint with the Adel canonical bid data.
"""
from __future__ import annotations

import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app


STRUCTURED_DIR = Path(__file__).parent / "fixtures" / "adel_ipsi" / "structured"


@pytest.fixture
def client(tmp_path, monkeypatch):
    """
    Create a TestClient with a temporary data directory.
    Patches BID_GUARDRAIL_DATA_DIR so RunStorage uses tmp_path.
    """
    import app.main as main_module
    # Re-initialize storage with tmp_path
    from app.storage.local_fs import RunStorage
    from app.audit.writer import AuditWriter

    monkeypatch.setattr(main_module, "storage", RunStorage(str(tmp_path)))
    monkeypatch.setattr(main_module, "audit", AuditWriter(str(tmp_path)))

    return TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"


class TestValidateEndpointBidOnly:
    """Test /validate with bid XLSX + a minimal PDF (no quote)."""

    def test_validate_bid_xlsx_no_pdf_requires_pdf(self, client):
        """PRIME_BID doc_type requires a PDF upload."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        with open(bid_path, "rb") as f:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "PRIME_BID"},
                files={"bid_items": ("bid_items.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            )
        assert resp.status_code == 400
        assert "pdf" in resp.json().get("detail", "").lower()

    def test_validate_bid_xlsx_with_dummy_pdf(self, client):
        """
        Submit bid XLSX with a minimal valid PDF.
        Should process bid validation successfully.
        """
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"

        # Create a minimal valid PDF in memory
        pdf_bytes = _minimal_pdf()

        with open(bid_path, "rb") as bf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "PRIME_BID"},
                files={
                    "pdf": ("test.pdf", io.BytesIO(pdf_bytes), "application/pdf"),
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "run_id" in data
        assert "findings" in data
        assert "bid_summary" in data
        assert data["doc_type"] == "PRIME_BID"

    def test_validate_bid_summary_has_mobilization(self, client):
        """Bid summary should report mobilization found."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        pdf_bytes = _minimal_pdf()

        with open(bid_path, "rb") as bf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "PRIME_BID"},
                files={
                    "pdf": ("test.pdf", io.BytesIO(pdf_bytes), "application/pdf"),
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        data = resp.json()
        bid_summary = data.get("bid_summary", {})
        assert bid_summary.get("mobilization_found") is True


class TestValidateEndpointQuoteMode:
    """Test /validate in QUOTE mode."""

    def test_quote_mode_requires_quote_lines(self, client):
        """QUOTE doc_type requires quote_lines upload."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        with open(bid_path, "rb") as bf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 400
        assert "quote_lines" in resp.json().get("detail", "").lower()

    def test_quote_mode_ipsi_ingests_but_reconciliation_fails(self, client):
        """
        Phase C-1: IPSI quote XLSX now ingests successfully.
        Endpoint returns 200 and proceeds to reconciliation.
        Reconciliation FAILs because quote line numbers (520, etc.)
        don't match bid DOT item numbers (2524-...).
        """
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"

        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["doc_type"] == "QUOTE"
        assert "quote_summary" in data
        # All quote lines should be unmatched (line numbers vs DOT items)
        findings = data.get("findings", [])
        unmatched = [f for f in findings if f.get("type") == "quote_line_unmatched"]
        assert len(unmatched) >= 14, (
            f"Expected >=14 unmatched findings (line-number gap), got {len(unmatched)}"
        )


def _minimal_pdf() -> bytes:
    """Generate a minimal valid PDF (1 page, no content)."""
    import fitz
    doc = fitz.open()
    doc.new_page(width=612, height=792)
    pdf_bytes = doc.tobytes()
    doc.close()
    return pdf_bytes
