import React, { useState } from "react";
import { Panel, Button, useAsync, IdentityBar, Json } from "./common.jsx";
import { Toast, InFlightOverlay } from "./states.jsx";
import {
  buildDownloadable,
  buildDownloadBundle,
  listDownloadReportKinds,
} from "../api.js";

// C119 — Operator-facing report/download UX.
// Deterministic: choose report kind + format, optional revision_sequence,
// trigger browser download or preview, show stable success/failure feedback.

const BID_KINDS = [
  "bid_readiness_report",
  "authority_action_report",
  "final_carry_report",
];

const FORMATS = ["json", "markdown", "text", "structured"];

export default function DownloadsPanel({ bidId, jobId }) {
  const kinds = useAsync(() => listDownloadReportKinds(), []);
  const [format, setFormat] = useState("json");
  const [reportKind, setReportKind] = useState("bid_readiness_report");
  const [revision, setRevision] = useState("");
  const [recent, setRecent] = useState([]);
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState(null);
  const [preview, setPreview] = useState(null);

  const availableKinds =
    (kinds?.data?.report_kinds || [])
      .filter((k) => {
        if (k === "estimator_review_report") return !!jobId;
        return !!bidId;
      })
      .sort() || BID_KINDS;

  async function doDownload({ kind, asBundle = false } = {}) {
    setBusy(true);
    const startedAt = new Date().toISOString();
    try {
      const res = asBundle
        ? await buildDownloadBundle({ bidId, jobId, format })
        : await buildDownloadable({
            reportKind: kind || reportKind,
            bidId,
            jobId: kind === "estimator_review_report" ? jobId : jobId,
            revisionSequence:
              revision === "" ? null : Number(revision),
            format,
          });
      const status = res?.download_status || "error";
      const ok = status === "ok" || status === "partial";
      setRecent((xs) =>
        [
          {
            at: startedAt,
            summary: asBundle
              ? `bundle (${format})`
              : `${kind || reportKind} (${format})`,
            status,
            filename: res?.filename,
            byte_length: res?.byte_length,
            content_hash: res?.content_hash,
            revision: res?.revision_metadata?.revision_sequence,
            downloads: res?.downloads,
            body: res?.body,
          },
          ...xs,
        ].slice(0, 12)
      );
      setToast({
        kind: ok ? "success" : "error",
        text: asBundle
          ? `Bundle ${status} • ${res?.download_count ?? 0} reports`
          : `${kind || reportKind} (${format}) → ${status}`,
      });
      if (!asBundle && ok) {
        triggerBrowserDownload(res);
      }
      return res;
    } catch (err) {
      setRecent((xs) =>
        [
          {
            at: startedAt,
            summary: asBundle ? `bundle (${format})` : `${kind || reportKind} (${format})`,
            status: "error",
            error: err.message || String(err),
          },
          ...xs,
        ].slice(0, 12)
      );
      setToast({ kind: "error", text: err.message || String(err) });
    } finally {
      setBusy(false);
    }
  }

  function triggerBrowserDownload(download) {
    try {
      if (!download || !download.filename) return;
      const body = download.body;
      let blobBody;
      let mime = download.content_type || "application/octet-stream";
      if (typeof body === "string") {
        blobBody = body;
      } else if (body === null || body === undefined) {
        blobBody = "";
      } else {
        blobBody = JSON.stringify(body, null, 2);
        mime = "application/json";
      }
      const blob = new Blob([blobBody], { type: mime });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = download.filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (e) {
      // Browser download is best-effort; the API response itself is
      // the source of truth.
    }
  }

  return (
    <Panel title="Reports & Downloads (C119)" subtitle="Tied to canonical revisions">
      {toast && (
        <Toast kind={toast.kind} onClose={() => setToast(null)}>
          {toast.text}
        </Toast>
      )}
      <IdentityBar entries={[["bid_id", bidId], ["job_id", jobId]]} />
      <div style={{ position: "relative" }}>
        <InFlightOverlay active={busy} label="Generating report…" />
        <div
          style={{
            display: "flex",
            gap: 8,
            flexWrap: "wrap",
            alignItems: "center",
          }}
        >
          <label style={{ fontSize: 13 }}>
            kind:&nbsp;
            <select
              value={reportKind}
              onChange={(e) => setReportKind(e.target.value)}
              style={{
                padding: 6,
                borderRadius: 6,
                border: "1px solid #ccc",
              }}
            >
              {availableKinds.map((k) => (
                <option key={k} value={k}>
                  {k}
                </option>
              ))}
            </select>
          </label>
          <label style={{ fontSize: 13 }}>
            format:&nbsp;
            <select
              value={format}
              onChange={(e) => setFormat(e.target.value)}
              style={{
                padding: 6,
                borderRadius: 6,
                border: "1px solid #ccc",
              }}
            >
              {FORMATS.map((f) => (
                <option key={f} value={f}>
                  {f}
                </option>
              ))}
            </select>
          </label>
          <label style={{ fontSize: 13 }}>
            rev (optional, "latest" if blank):&nbsp;
            <input
              value={revision}
              onChange={(e) => setRevision(e.target.value)}
              placeholder=""
              style={{
                padding: 6,
                borderRadius: 6,
                border: "1px solid #ccc",
                width: 80,
              }}
            />
          </label>
          <Button
            disabled={busy || (!bidId && !jobId)}
            onClick={() => doDownload()}
          >
            Build & download
          </Button>
          <Button
            kind="secondary"
            disabled={busy || !bidId}
            onClick={() => doDownload({ asBundle: true })}
          >
            Build bid bundle
          </Button>
          <Button
            kind="secondary"
            onClick={() => setPreview(recent[0])}
            disabled={!recent.length}
          >
            Preview last
          </Button>
        </div>

        <div style={{ marginTop: 10 }}>
          <div
            style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}
          >
            Quick actions
          </div>
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
            {BID_KINDS.map((k) => (
              <Button
                key={k}
                kind="secondary"
                disabled={busy || !bidId}
                onClick={() => {
                  setReportKind(k);
                  doDownload({ kind: k });
                }}
              >
                {k}
              </Button>
            ))}
          </div>
        </div>

        {recent.length > 0 && (
          <div style={{ marginTop: 12 }}>
            <div
              style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}
            >
              Download receipts
            </div>
            <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12 }}>
              {recent.map((r, i) => (
                <li key={i}>
                  <span style={{ opacity: 0.6 }}>{r.at}</span>{" "}
                  <code>{r.summary}</code> → <b>{r.status}</b>
                  {r.filename && (
                    <span>
                      {" "}
                      · <code>{r.filename}</code>
                    </span>
                  )}
                  {r.byte_length && <span> ({r.byte_length} bytes)</span>}
                  {r.revision !== null && r.revision !== undefined && (
                    <span> · rev={r.revision}</span>
                  )}
                  {r.error && (
                    <span style={{ color: "crimson" }}> {r.error}</span>
                  )}
                </li>
              ))}
            </ul>
          </div>
        )}

        {preview && (
          <div style={{ marginTop: 12 }}>
            <div
              style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}
            >
              Preview — {preview.summary}
            </div>
            {preview.downloads ? (
              <Json data={preview.downloads} />
            ) : typeof preview.body === "string" ? (
              <pre
                style={{
                  background: "#f7f7f7",
                  padding: 10,
                  borderRadius: 8,
                  overflowX: "auto",
                  fontSize: 12,
                  maxHeight: 320,
                }}
              >
                {preview.body}
              </pre>
            ) : (
              <Json data={preview.body || preview} />
            )}
          </div>
        )}
      </div>
    </Panel>
  );
}
