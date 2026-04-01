"""
Phase B Canonical Test Harness — Adel / IPSI
Shared fixtures and paths for deterministic testing.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "adel_ipsi"
STRUCTURED_DIR = FIXTURES_DIR / "structured"
RAW_REF_DIR = FIXTURES_DIR / "raw_reference"


@pytest.fixture
def structured_dir() -> Path:
    return STRUCTURED_DIR


@pytest.fixture
def bid_xlsx_path() -> Path:
    return STRUCTURED_DIR / "bid_items.xlsx"


@pytest.fixture
def quote_xlsx_path() -> Path:
    return STRUCTURED_DIR / "quote_lines.xlsx"


@pytest.fixture
def bid_csv_path() -> Path:
    return STRUCTURED_DIR / "bid_structural.csv"


@pytest.fixture
def bid_truth() -> list:
    with open(STRUCTURED_DIR / "bid_truth.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def quote_truth() -> list:
    with open(STRUCTURED_DIR / "quote_truth.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def line_to_item_mapping() -> dict:
    with open(STRUCTURED_DIR / "line_to_item_mapping.json", "r", encoding="utf-8") as f:
        return json.load(f)
