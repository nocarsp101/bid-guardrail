from __future__ import annotations

import os
import json
from app.audit.models import AuditEvent


class AuditWriter:
    def __init__(self, data_dir: str):
        self.audit_dir = os.path.join(data_dir, "audit")
        os.makedirs(self.audit_dir, exist_ok=True)
        self.log_path = os.path.join(self.audit_dir, "audit_log.jsonl")

    def append_event(self, event: AuditEvent) -> None:
        # Append-only JSON Lines
        line = json.dumps(event.model_dump(), ensure_ascii=False)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
