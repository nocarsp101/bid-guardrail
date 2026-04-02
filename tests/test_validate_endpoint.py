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
    from app.storage.local_fs import RunStorage
    from app.audit.writer import AuditWriter
    from app.storage.mapping_store import MappingStore

    monkeypatch.setattr(main_module, "storage", RunStorage(str(tmp_path)))
    monkeypatch.setattr(main_module, "audit", AuditWriter(str(tmp_path)))
    monkeypatch.setattr(main_module, "mapping_store", MappingStore(str(tmp_path)))

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
        # Phase C-5: TOTAL row filtered at ingest, so exactly 14 data rows remain
        assert len(unmatched) == 14, (
            f"Expected 14 unmatched findings (line-number gap), got {len(unmatched)}"
        )
        # No spurious missing_unit_price from TOTAL row
        missing_up = [f for f in findings if f.get("type") == "quote_line_missing_unit_price"]
        assert len(missing_up) == 0


class TestValidateEndpointContractHardening:
    """C6B: Endpoint contract, mapping toggle, and parameter hardening."""

    def test_mapping_without_quote_lines_rejected(self, client):
        """line_to_item_mapping without quote_lines is fail-closed 400."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        mapping_json = b'{"520": "2524-6765010"}'
        pdf_bytes = _minimal_pdf()

        with open(bid_path, "rb") as bf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "PRIME_BID"},
                files={
                    "pdf": ("test.pdf", io.BytesIO(pdf_bytes), "application/pdf"),
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(mapping_json), "application/json"),
                },
            )
        assert resp.status_code == 400
        assert "line_to_item_mapping" in resp.json()["detail"]

    def test_mapping_invalid_json_rejected(self, client):
        """Invalid JSON in mapping file -> 400."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"

        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(b"NOT JSON"), "application/json"),
                },
            )
        assert resp.status_code == 400
        assert "json" in resp.json()["detail"].lower()

    def test_mapping_non_dict_json_rejected(self, client):
        """JSON array in mapping file -> 400."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"

        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(b'[1, 2, 3]'), "application/json"),
                },
            )
        assert resp.status_code == 400
        assert "json object" in resp.json()["detail"].lower()

    def test_quote_mode_with_mapping_applies_mapping(self, client):
        """QUOTE mode with valid mapping -> line_mapping_applied=True, matches produced."""
        import json as _json

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"

        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        mapping_bytes = _json.dumps(full_mapping).encode()

        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(mapping_bytes), "application/json"),
                },
            )
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()

        # Mapping flag in ingestion metadata
        ingestion = data["quote_summary"]["ingestion"]
        assert ingestion["line_mapping_applied"] is True

        # With correct mapping, all 14 quote lines should match bid items
        unmatched = [f for f in data["findings"] if f["type"] == "quote_line_unmatched"]
        assert len(unmatched) == 0, f"With mapping, expected 0 unmatched, got {len(unmatched)}"

    def test_quote_mode_without_mapping_no_mapping_applied(self, client):
        """QUOTE mode without mapping -> line_mapping_applied=False."""
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
        assert resp.status_code == 200
        data = resp.json()
        ingestion = data["quote_summary"]["ingestion"]
        assert ingestion["line_mapping_applied"] is False

    def test_output_contract_top_level_keys(self, client):
        """Response contains all required top-level keys."""
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
        assert resp.status_code == 200
        data = resp.json()
        required_keys = {"run_id", "doc_type", "overall_status", "findings", "bid_summary", "quote_summary", "audit_log"}
        assert required_keys.issubset(data.keys()), f"Missing keys: {required_keys - data.keys()}"

    def test_quote_summary_ingestion_keys(self, client):
        """quote_summary.ingestion has expected keys including line_mapping_applied."""
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
        data = resp.json()
        ingestion = data["quote_summary"]["ingestion"]
        expected_keys = {
            "rows_raw_total", "mapping_missing", "mapping_ambiguous",
            "mapping_used", "alias_dictionary", "normalization", "line_mapping_applied",
            "mapping_source", "mapping_name_used",
        }
        assert expected_keys.issubset(ingestion.keys()), f"Missing: {expected_keys - ingestion.keys()}"

    def test_bid_only_mode_has_no_quote_summary(self, client):
        """PRIME_BID without quote_lines -> quote_summary is None."""
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
        assert resp.status_code == 200
        data = resp.json()
        assert data["quote_summary"] is None


class TestMappingEndpoints:
    """C7: /mapping/save, /mapping/list, /mapping/{name} CRUD tests."""

    def test_save_mapping_valid(self, client):
        """POST /mapping/save with valid JSON succeeds."""
        mapping_json = b'{"520": "2524-6765010", "530": "2524-6765020"}'
        resp = client.post(
            "/mapping/save",
            data={"name": "test-mapping", "actor": "test-harness"},
            files={"mapping": ("mapping.json", io.BytesIO(mapping_json), "application/json")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test-mapping"
        assert data["entry_count"] == 2
        assert data["saved_by"] == "test-harness"

    def test_save_mapping_bad_name_rejected(self, client):
        """Names with spaces or special chars are rejected."""
        resp = client.post(
            "/mapping/save",
            data={"name": "bad name!", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(b'{}'), "application/json")},
        )
        assert resp.status_code == 400
        assert "alphanumeric" in resp.json()["detail"]

    def test_save_mapping_bad_json_rejected(self, client):
        resp = client.post(
            "/mapping/save",
            data={"name": "test", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(b"NOT JSON"), "application/json")},
        )
        assert resp.status_code == 400

    def test_save_mapping_non_dict_rejected(self, client):
        resp = client.post(
            "/mapping/save",
            data={"name": "test", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(b"[1,2]"), "application/json")},
        )
        assert resp.status_code == 400
        assert "json object" in resp.json()["detail"].lower()

    def test_list_mappings_empty(self, client):
        resp = client.get("/mapping/list")
        assert resp.status_code == 200
        assert resp.json()["mappings"] == []

    def test_list_mappings_after_save(self, client):
        client.post(
            "/mapping/save",
            data={"name": "alpha", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(b'{"1": "A"}'), "application/json")},
        )
        resp = client.get("/mapping/list")
        names = [m["name"] for m in resp.json()["mappings"]]
        assert "alpha" in names

    def test_get_mapping_found(self, client):
        client.post(
            "/mapping/save",
            data={"name": "alpha", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(b'{"1": "A"}'), "application/json")},
        )
        resp = client.get("/mapping/alpha")
        assert resp.status_code == 200
        assert resp.json()["mapping"] == {"1": "A"}

    def test_get_mapping_not_found(self, client):
        resp = client.get("/mapping/nonexistent")
        assert resp.status_code == 404


class TestMappingAutoLoad:
    """C7: /validate auto-loads saved mapping via mapping_name."""

    def _save_full_mapping(self, client):
        """Helper: save the Adel/IPSI full mapping to the store."""
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        mapping_bytes = _json.dumps(full_mapping).encode()
        resp = client.post(
            "/mapping/save",
            data={"name": "adel-ipsi", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(mapping_bytes), "application/json")},
        )
        assert resp.status_code == 200

    def test_validate_with_mapping_name_auto_loads(self, client):
        """QUOTE mode + mapping_name -> mapping auto-loaded, all 14 lines match."""
        self._save_full_mapping(client)

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE", "mapping_name": "adel-ipsi"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["quote_summary"]["ingestion"]["line_mapping_applied"] is True
        unmatched = [f for f in data["findings"] if f["type"] == "quote_line_unmatched"]
        assert len(unmatched) == 0

    def test_validate_mapping_name_not_found(self, client):
        """mapping_name referencing non-existent mapping -> 404."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE", "mapping_name": "nonexistent"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 404

    def test_validate_mapping_name_without_quote_lines_rejected(self, client):
        """mapping_name without quote_lines -> 400."""
        self._save_full_mapping(client)

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        pdf_bytes = _minimal_pdf()
        with open(bid_path, "rb") as bf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "PRIME_BID", "mapping_name": "adel-ipsi"},
                files={
                    "pdf": ("test.pdf", io.BytesIO(pdf_bytes), "application/pdf"),
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 400
        assert "mapping_name" in resp.json()["detail"]

    def test_file_upload_takes_precedence_over_mapping_name(self, client):
        """When both file and mapping_name provided, file upload wins."""
        import json as _json

        # Save a mapping with ONE entry
        client.post(
            "/mapping/save",
            data={"name": "partial", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(b'{"520": "WRONG-ITEM"}'), "application/json")},
        )

        # Upload the CORRECT full mapping as file
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        mapping_bytes = _json.dumps(full_mapping).encode()

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE", "mapping_name": "partial"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(mapping_bytes), "application/json"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        # File upload (correct full mapping) used -> all matched
        unmatched = [f for f in data["findings"] if f["type"] == "quote_line_unmatched"]
        assert len(unmatched) == 0


class TestUnmatchedEnrichment:
    """C7: Pipeline enrichment when quote lines are unmatched."""

    def test_unmatched_shows_available_bid_items(self, client):
        """When lines are unmatched (no mapping), quote_summary includes available_bid_items."""
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
        data = resp.json()
        qs = data["quote_summary"]
        assert "available_bid_items" in qs
        assert len(qs["available_bid_items"]) > 0
        # Bid items should be DOT-style numbers like 2524-6765010
        assert any("-" in item for item in qs["available_bid_items"])
        assert "mapping_hint" in qs

    def test_unmatched_enrichment_hint_message(self, client):
        """mapping_hint should mention the unmatched count."""
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
        data = resp.json()
        hint = data["quote_summary"]["mapping_hint"]
        assert "14" in hint  # 14 unmatched lines
        assert "mapping" in hint.lower()

    def test_no_enrichment_when_all_matched(self, client):
        """When all lines match (with mapping), no available_bid_items in summary."""
        import json as _json

        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        mapping_bytes = _json.dumps(full_mapping).encode()

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test-harness", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(mapping_bytes), "application/json"),
                },
            )
        data = resp.json()
        qs = data["quote_summary"]
        assert "available_bid_items" not in qs
        assert "mapping_hint" not in qs


class TestMappingSaveWithContext:
    """C7B: /mapping/save with project/vendor metadata."""

    def test_save_with_project_and_vendor(self, client):
        resp = client.post(
            "/mapping/save",
            data={"name": "adel-ipsi", "actor": "test", "project": "adel", "vendor": "ipsi"},
            files={"mapping": ("m.json", io.BytesIO(b'{"520": "2524-6765010"}'), "application/json")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["project"] == "adel"
        assert data["vendor"] == "ipsi"

    def test_save_without_context_has_null_fields(self, client):
        resp = client.post(
            "/mapping/save",
            data={"name": "bare", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(b'{"1": "A"}'), "application/json")},
        )
        data = resp.json()
        assert data["project"] is None
        assert data["vendor"] is None

    def test_list_records_include_context(self, client):
        client.post(
            "/mapping/save",
            data={"name": "ctx-map", "actor": "test", "project": "proj-x", "vendor": "vendor-y"},
            files={"mapping": ("m.json", io.BytesIO(b'{"1": "A"}'), "application/json")},
        )
        resp = client.get("/mapping/list")
        records = resp.json()["mappings"]
        assert len(records) == 1
        assert records[0]["name"] == "ctx-map"
        assert records[0]["project"] == "proj-x"
        assert records[0]["vendor"] == "vendor-y"

    def test_get_mapping_includes_context(self, client):
        client.post(
            "/mapping/save",
            data={"name": "ctx-map", "actor": "test", "project": "proj-x", "vendor": "vendor-y"},
            files={"mapping": ("m.json", io.BytesIO(b'{"1": "A"}'), "application/json")},
        )
        resp = client.get("/mapping/ctx-map")
        data = resp.json()
        assert data["project"] == "proj-x"
        assert data["vendor"] == "vendor-y"


class TestAutoSelection:
    """C7B: /validate auto-selects mapping by project/vendor context."""

    def _save_mapping(self, client, name, project=None, vendor=None):
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        data = {"name": name, "actor": "test"}
        if project:
            data["project"] = project
        if vendor:
            data["vendor"] = vendor
        resp = client.post(
            "/mapping/save",
            data=data,
            files={"mapping": ("m.json", io.BytesIO(_json.dumps(full_mapping).encode()), "application/json")},
        )
        assert resp.status_code == 200

    def test_auto_select_unique_match(self, client):
        """Single mapping matches project+vendor -> auto-selected, all lines match."""
        self._save_mapping(client, "adel-ipsi", project="adel", vendor="ipsi")

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE", "project": "adel", "vendor": "ipsi"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        ing = data["quote_summary"]["ingestion"]
        assert ing["line_mapping_applied"] is True
        assert ing["mapping_source"] == "auto_selected"
        assert ing["mapping_name_used"] == "adel-ipsi"
        unmatched = [f for f in data["findings"] if f["type"] == "quote_line_unmatched"]
        assert len(unmatched) == 0

    def test_auto_select_by_project_only(self, client):
        """Match on project alone when only one mapping has that project."""
        self._save_mapping(client, "adel-ipsi", project="adel", vendor="ipsi")

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE", "project": "adel"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        ing = resp.json()["quote_summary"]["ingestion"]
        assert ing["mapping_source"] == "auto_selected"
        assert ing["mapping_name_used"] == "adel-ipsi"

    def test_auto_select_case_insensitive(self, client):
        """Auto-selection is case-insensitive."""
        self._save_mapping(client, "adel-ipsi", project="Adel", vendor="IPSI")

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE", "project": "adel", "vendor": "ipsi"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        assert resp.json()["quote_summary"]["ingestion"]["mapping_source"] == "auto_selected"

    def test_auto_select_no_match_no_mapping(self, client):
        """No saved mapping matches -> no mapping applied (not an error)."""
        self._save_mapping(client, "other-map", project="other-project", vendor="other-vendor")

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE", "project": "adel", "vendor": "ipsi"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        ing = resp.json()["quote_summary"]["ingestion"]
        assert ing["line_mapping_applied"] is False
        assert ing["mapping_source"] is None
        assert ing["mapping_name_used"] is None

    def test_auto_select_ambiguous_fails_409(self, client):
        """Two mappings match same project -> 409 Conflict."""
        self._save_mapping(client, "adel-ipsi-v1", project="adel", vendor="ipsi")
        self._save_mapping(client, "adel-ipsi-v2", project="adel", vendor="ipsi")

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE", "project": "adel", "vendor": "ipsi"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert "adel-ipsi-v1" in detail
        assert "adel-ipsi-v2" in detail

    def test_ambiguous_resolved_by_vendor(self, client):
        """Two mappings share project but differ by vendor -> vendor disambiguates."""
        self._save_mapping(client, "adel-ipsi", project="adel", vendor="ipsi")
        self._save_mapping(client, "adel-summit", project="adel", vendor="summit")

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE", "project": "adel", "vendor": "ipsi"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        ing = resp.json()["quote_summary"]["ingestion"]
        assert ing["mapping_source"] == "auto_selected"
        assert ing["mapping_name_used"] == "adel-ipsi"


class TestMappingSourceMetadata:
    """C7B: mapping_source and mapping_name_used in response metadata."""

    def test_file_upload_source(self, client):
        """File upload -> mapping_source='file_upload', mapping_name_used=None."""
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        mapping_bytes = _json.dumps(full_mapping).encode()

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(mapping_bytes), "application/json"),
                },
            )
        ing = resp.json()["quote_summary"]["ingestion"]
        assert ing["mapping_source"] == "file_upload"
        assert ing["mapping_name_used"] is None

    def test_named_source(self, client):
        """mapping_name -> mapping_source='named', mapping_name_used set."""
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        client.post(
            "/mapping/save",
            data={"name": "my-map", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(_json.dumps(full_mapping).encode()), "application/json")},
        )

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE", "mapping_name": "my-map"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        ing = resp.json()["quote_summary"]["ingestion"]
        assert ing["mapping_source"] == "named"
        assert ing["mapping_name_used"] == "my-map"

    def test_no_mapping_source_null(self, client):
        """No mapping at all -> mapping_source=None, mapping_name_used=None."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        ing = resp.json()["quote_summary"]["ingestion"]
        assert ing["mapping_source"] is None
        assert ing["mapping_name_used"] is None

    def test_file_upload_takes_precedence_over_auto_select(self, client):
        """File upload wins even when project/vendor would match a saved mapping."""
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]

        # Save a mapping with project/vendor
        client.post(
            "/mapping/save",
            data={"name": "saved-one", "actor": "test", "project": "adel", "vendor": "ipsi"},
            files={"mapping": ("m.json", io.BytesIO(_json.dumps(full_mapping).encode()), "application/json")},
        )

        mapping_bytes = _json.dumps(full_mapping).encode()
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={"actor": "test", "doc_type": "QUOTE", "project": "adel", "vendor": "ipsi"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(mapping_bytes), "application/json"),
                },
            )
        ing = resp.json()["quote_summary"]["ingestion"]
        assert ing["mapping_source"] == "file_upload"
        assert ing["mapping_name_used"] is None

    def test_mapping_name_takes_precedence_over_auto_select(self, client):
        """mapping_name wins over project/vendor auto-selection."""
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        mapping_bytes = _json.dumps(full_mapping).encode()

        # Save two mappings — one with context, one without
        client.post(
            "/mapping/save",
            data={"name": "with-ctx", "actor": "test", "project": "adel", "vendor": "ipsi"},
            files={"mapping": ("m.json", io.BytesIO(mapping_bytes), "application/json")},
        )
        client.post(
            "/mapping/save",
            data={"name": "explicit-pick", "actor": "test"},
            files={"mapping": ("m.json", io.BytesIO(mapping_bytes), "application/json")},
        )

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate",
                data={
                    "actor": "test", "doc_type": "QUOTE",
                    "mapping_name": "explicit-pick",
                    "project": "adel", "vendor": "ipsi",
                },
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        ing = resp.json()["quote_summary"]["ingestion"]
        assert ing["mapping_source"] == "named"
        assert ing["mapping_name_used"] == "explicit-pick"


class TestOperatorReport:
    """C7C: /validate/report operator workflow layer."""

    def test_report_structure_keys(self, client):
        """Report has all required top-level sections."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        required = {"run_summary", "mapping_provenance", "counts", "key_findings", "next_action", "detail"}
        assert required.issubset(data.keys()), f"Missing: {required - data.keys()}"

    def test_detail_preserves_raw_response(self, client):
        """detail section contains the full raw /validate output."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        detail = resp.json()["detail"]
        assert "run_id" in detail
        assert "findings" in detail
        assert "bid_summary" in detail
        assert "quote_summary" in detail

    def test_unmatched_run_summary(self, client):
        """14 unmatched lines -> FAIL status, description mentions unmatched count."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        rs = resp.json()["run_summary"]
        assert rs["overall_status"] == "FAIL"
        assert "14" in rs["status_description"]
        assert "unmatched" in rs["status_description"].lower()
        assert rs["fail_count"] == 14

    def test_unmatched_next_action_create_mapping(self, client):
        """No mapping applied + unmatched -> next_action='create_mapping'."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        na = resp.json()["next_action"]
        assert na["action"] == "create_mapping"
        assert "/mapping/save" in na["description"]

    def test_unmatched_key_findings_categorized(self, client):
        """Unmatched lines appear as grouped key_finding with items list."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        kf = resp.json()["key_findings"]
        unmatched_group = [g for g in kf if g["category"] == "unmatched_lines"]
        assert len(unmatched_group) == 1
        assert unmatched_group[0]["count"] == 14
        assert unmatched_group[0]["severity"] == "FAIL"
        assert len(unmatched_group[0]["items"]) == 14

    def test_unmatched_counts(self, client):
        """Counts reflect 0 matched, 14 unmatched."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        c = resp.json()["counts"]
        assert c["matched"] == 0
        assert c["unmatched"] == 14
        assert c["bid_items_in_file"] is not None

    def test_mapping_provenance_no_mapping(self, client):
        """No mapping -> provenance says so."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        mp = resp.json()["mapping_provenance"]
        assert mp["mapping_applied"] is False
        assert mp["mapping_source"] is None
        assert "no" in mp["description"].lower()

    def test_with_mapping_report_shows_pass_and_warnings(self, client):
        """With correct mapping -> all matched, next_action reflects warnings (qty mismatch)."""
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        mapping_bytes = _json.dumps(full_mapping).encode()

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(mapping_bytes), "application/json"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()

        # Run summary
        rs = data["run_summary"]
        assert rs["overall_status"] == "WARN"
        assert rs["fail_count"] == 0

        # Mapping provenance
        mp = data["mapping_provenance"]
        assert mp["mapping_applied"] is True
        assert mp["mapping_source"] == "file_upload"

        # Counts: all 14 matched
        c = data["counts"]
        assert c["matched"] == 14
        assert c["unmatched"] == 0

        # No unmatched key_findings
        kf_unmatched = [g for g in data["key_findings"] if g["category"] == "unmatched_lines"]
        assert len(kf_unmatched) == 0

        # Qty mismatches should appear
        kf_qty = [g for g in data["key_findings"] if g["category"] == "quantity_mismatches"]
        assert len(kf_qty) == 1
        assert kf_qty[0]["severity"] == "WARN"

        # Next action: review_warnings (no FAILs, but WARNs exist)
        assert data["next_action"]["action"] == "review_warnings"

    def test_mapping_provenance_auto_selected(self, client):
        """Auto-selected mapping -> provenance reflects it."""
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        client.post(
            "/mapping/save",
            data={"name": "adel-ipsi", "actor": "test", "project": "adel", "vendor": "ipsi"},
            files={"mapping": ("m.json", io.BytesIO(_json.dumps(full_mapping).encode()), "application/json")},
        )

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "QUOTE", "project": "adel", "vendor": "ipsi"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        mp = resp.json()["mapping_provenance"]
        assert mp["mapping_source"] == "auto_selected"
        assert mp["mapping_name_used"] == "adel-ipsi"
        assert "auto-selected" in mp["description"].lower()

    def test_prime_bid_report(self, client):
        """PRIME_BID mode -> report works, no quote-specific sections populated."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        pdf_bytes = _minimal_pdf()
        with open(bid_path, "rb") as bf:
            resp = client.post(
                "/validate/report",
                data={"actor": "test", "doc_type": "PRIME_BID"},
                files={
                    "pdf": ("test.pdf", io.BytesIO(pdf_bytes), "application/pdf"),
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "run_summary" in data
        # No quote counts
        assert "matched" not in data["counts"]
        # Mapping provenance reflects no quote
        assert "PRIME_BID" in data["mapping_provenance"]["description"]


class TestExportEndpoints:
    """C7D: /validate/export/html, /validate/export/csv, /validate/export/json."""

    # -- HTML --

    def test_html_export_content_type(self, client):
        """HTML export returns text/html with attachment header."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/html",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "attachment" in resp.headers.get("content-disposition", "")

    def test_html_export_contains_status(self, client):
        """HTML contains the overall status and description."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/html",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        html = resp.text
        assert "FAIL" in html
        assert "unmatched" in html.lower()

    def test_html_export_contains_mapping_provenance(self, client):
        """HTML mentions mapping status."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/html",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        html = resp.text
        assert "Mapping" in html
        assert "No line-to-item mapping" in html

    def test_html_export_contains_next_action(self, client):
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/html",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert "create_mapping" in resp.text

    def test_html_export_with_mapping_has_comparisons(self, client):
        """With mapping applied, HTML contains the comparisons table."""
        import json as _json
        mapping_path = STRUCTURED_DIR / "line_to_item_mapping.json"
        with open(mapping_path, "r", encoding="utf-8") as f:
            full_mapping = _json.load(f)["full_mapping"]
        mapping_bytes = _json.dumps(full_mapping).encode()

        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/html",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "line_to_item_mapping": ("mapping.json", io.BytesIO(mapping_bytes), "application/json"),
                },
            )
        html = resp.text
        assert "Comparisons" in html
        assert "2524-6765010" in html  # first mapped DOT item

    # -- CSV --

    def test_csv_export_content_type(self, client):
        """CSV export returns text/csv with attachment header."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/csv",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert "attachment" in resp.headers.get("content-disposition", "")

    def test_csv_export_has_header_and_data(self, client):
        """CSV has column header row and finding data rows."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/csv",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        lines = [l.strip() for l in resp.text.strip().splitlines() if l.strip()]
        data_lines = [l for l in lines if not l.startswith("#")]
        # Should have header + 14 finding rows
        assert len(data_lines) >= 15, f"Expected header + data rows, got {len(data_lines)}: {data_lines[:3]}"
        assert "severity" in data_lines[0]

    def test_csv_export_contains_mapping_provenance(self, client):
        """CSV header comments include mapping info."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/csv",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        text = resp.text
        assert "Mapping" in text
        assert "create_mapping" in text

    # -- JSON --

    def test_json_export_content_type_and_download(self, client):
        """JSON export returns application/json with attachment header."""
        bid_path = STRUCTURED_DIR / "bid_items.xlsx"
        quote_path = STRUCTURED_DIR / "quote_lines.xlsx"
        with open(bid_path, "rb") as bf, open(quote_path, "rb") as qf:
            resp = client.post(
                "/validate/export/json",
                data={"actor": "test", "doc_type": "QUOTE"},
                files={
                    "bid_items": ("bid_items.xlsx", bf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    "quote_lines": ("quote_lines.xlsx", qf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                },
            )
        assert resp.status_code == 200
        assert "application/json" in resp.headers["content-type"]
        assert "attachment" in resp.headers.get("content-disposition", "")
        # Should be parseable and have report structure
        data = resp.json()
        assert "run_summary" in data
        assert "detail" in data


def _minimal_pdf() -> bytes:
    """Generate a minimal valid PDF (1 page, no content)."""
    import fitz
    doc = fitz.open()
    doc.new_page(width=612, height=792)
    pdf_bytes = doc.tobytes()
    doc.close()
    return pdf_bytes
