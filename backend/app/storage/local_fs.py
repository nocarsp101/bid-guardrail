from __future__ import annotations

import os
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, List

from fastapi import UploadFile


@dataclass(frozen=True)
class RunInfo:
    run_id: str
    run_dir: str
    actor: str
    created_utc: str


class RunStorage:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.runs_dir = os.path.join(self.data_dir, "runs")
        os.makedirs(self.runs_dir, exist_ok=True)

    def create_run(self, actor: str) -> RunInfo:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_actor = "".join(c for c in actor if c.isalnum() or c in ("-", "_", ".", "@"))[:60]
        run_id = f"run_{ts}_{safe_actor}"
        run_dir = os.path.join(self.runs_dir, run_id)
        os.makedirs(run_dir, exist_ok=False)

        meta = {
            "run_id": run_id,
            "actor": actor,
            "created_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        with open(os.path.join(run_dir, "run_meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

        return RunInfo(run_id=run_id, run_dir=run_dir, actor=actor, created_utc=meta["created_utc"])

    async def save_upload(self, run_id: str, upload: UploadFile) -> str:
        run_dir = os.path.join(self.runs_dir, run_id)
        if not os.path.isdir(run_dir):
            raise RuntimeError(f"Run not found: {run_id}")

        uploads_dir = os.path.join(run_dir, "uploads")
        os.makedirs(uploads_dir, exist_ok=True)

        # keep filename safe-ish
        filename = os.path.basename(upload.filename).replace("..", ".")
        dest = os.path.join(uploads_dir, filename)

        # stream to disk
        with open(dest, "wb") as f:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)

        return dest

    def describe_input_files(self, run_id: str) -> List[Dict[str, Any]]:
        run_dir = os.path.join(self.runs_dir, run_id)
        uploads_dir = os.path.join(run_dir, "uploads")
        out: List[Dict[str, Any]] = []
        if not os.path.isdir(uploads_dir):
            return out

        for name in sorted(os.listdir(uploads_dir)):
            path = os.path.join(uploads_dir, name)
            if os.path.isfile(path):
                out.append({
                    "file_name": name,
                    "size_bytes": os.path.getsize(path),
                })
        return out

    def audit_log_path(self) -> str:
        # Single shared audit log file (append-only)
        return os.path.join(self.data_dir, "audit", "audit_log.jsonl")
