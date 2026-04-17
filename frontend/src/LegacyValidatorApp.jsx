import React, { useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export default function App() {
  const [actor, setActor] = useState("zain.rajput");
  const [docType, setDocType] = useState("PRIME_BID");

  const [pdf, setPdf] = useState(null);
  const [bidItems, setBidItems] = useState(null);

  // NEW: quote lines upload (Milestone-2)
  const [quoteLines, setQuoteLines] = useState(null);

  const [override, setOverride] = useState(false);
  const [overrideReason, setOverrideReason] = useState("");

  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");

  const canSubmit = useMemo(() => {
    const hasActor = actor.trim().length > 0;
    const pdfOk = docType === "QUOTE" ? true : !!pdf;   // PDF required only for PRIME_BID
    return hasActor && pdfOk;
  }, [actor, pdf, docType]);

  async function onSubmit(e) {
    e.preventDefault();
    setError("");
    setResult(null);

    if (!canSubmit) return;

    if (docType === "PRIME_BID" && !pdf) {
      setError("Bid Package PDF is required for PRIME_BID.");
      return;
    }

    if (override && !overrideReason.trim()) {
      setError("Override reason is required if override is enabled.");
      return;
    }

    // Milestone-2 requires bid_items if quote_lines is provided
    if (quoteLines && !bidItems) {
      setError("Quote Lines requires Bid Items (CSV/XLSX) for deterministic mapping.");
      return;
    }

    const fd = new FormData();
    fd.append("actor", actor.trim());
    fd.append("doc_type", docType);
    if (pdf) fd.append("pdf", pdf);

    if (bidItems) fd.append("bid_items", bidItems);

    // NEW: send quote_lines
    if (quoteLines) fd.append("quote_lines", quoteLines);

    fd.append("override", String(override));
    if (override) {
      fd.append("override_reason", overrideReason.trim());
      fd.append("override_actor", actor.trim());
    }

    try {
      setLoading(true);
      const res = await fetch(`${API_BASE}/validate`, { method: "POST", body: fd });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.detail || "Validation failed");
      setResult(json);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  function severityStyle(sev) {
    if (sev === "FAIL") return { color: "crimson", fontWeight: 800 };
    if (sev === "WARN") return { color: "#b45309", fontWeight: 800 };
    if (sev === "INFO") return { color: "#0f766e", fontWeight: 800 };
    return { fontWeight: 800 };
  }

  return (
    <div style={{ maxWidth: 980, margin: "30px auto", fontFamily: "system-ui, Arial" }}>
      <h2>Bid Guardrail MVP (Week-2)</h2>
      <p style={{ marginTop: -8, opacity: 0.75 }}>
        Upload PDF + Bid Items + (optional) Quote Lines → deterministic checks → PASS/WARN/FAIL → append-only audit log.
      </p>

      <form onSubmit={onSubmit} style={{ border: "1px solid #ddd", padding: 16, borderRadius: 10 }}>
        <div style={{ display: "grid", gap: 12 }}>
          <label>
            <div style={{ fontWeight: 600 }}>Actor (user)</div>
            <input
              value={actor}
              onChange={(e) => setActor(e.target.value)}
              placeholder="e.g. zain.rajput"
              style={{ width: "100%", padding: 10, borderRadius: 8, border: "1px solid #ccc" }}
            />
          </label>

          <label>
            <div style={{ fontWeight: 600 }}>Document Type Context</div>
            <select
              value={docType}
              onChange={(e) => setDocType(e.target.value)}
              style={{ width: "100%", padding: 10, borderRadius: 8, border: "1px solid #ccc" }}
            >
              <option value="PRIME_BID">PRIME BID</option>
              <option value="QUOTE">QUOTE</option>
            </select>
            <div style={{ fontSize: 12, opacity: 0.75, marginTop: 4 }}>
              This can severity-adjust PDF integrity findings (INFO/WARN/FAIL) without changing detection logic.
            </div>
          </label>

          <label>
            <div style={{ fontWeight: 600 }}>
              Bid Package PDF ({docType === "QUOTE" ? "optional" : "required"})
            </div>
            <input
              type="file"
              accept="application/pdf"
              onChange={(e) => setPdf(e.target.files?.[0] || null)}
            />
            {pdf && <div style={{ marginTop: 6, fontSize: 13, opacity: 0.8 }}>Selected: {pdf.name}</div>}
          </label>

          <label>
            <div style={{ fontWeight: 600 }}>Bid Items (CSV/XLSX)</div>
            <input
              type="file"
              accept=".csv,.xlsx,.xlsm,.xltx,.xltm"
              onChange={(e) => setBidItems(e.target.files?.[0] || null)}
            />
            {bidItems && (
              <div style={{ marginTop: 6, fontSize: 13, opacity: 0.8 }}>
                Selected: {bidItems.name}
              </div>
            )}
          </label>

          {/* NEW: Quote Lines upload */}
          <label>
            <div style={{ fontWeight: 600 }}>Quote Lines (CSV/XLSX) — Milestone-2</div>
            <input
              type="file"
              accept=".csv,.xlsx,.xlsm,.xltx,.xltm"
              onChange={(e) => setQuoteLines(e.target.files?.[0] || null)}
            />
            {quoteLines && (
              <div style={{ marginTop: 6, fontSize: 13, opacity: 0.8 }}>
                Selected: {quoteLines.name}
              </div>
            )}
            <div style={{ fontSize: 12, opacity: 0.75, marginTop: 4 }}>
              Requires Bid Items for deterministic mapping (quote.item must match bid.item exactly; units must match).
            </div>
          </label>

          <label style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <input
              type="checkbox"
              checked={override}
              onChange={(e) => setOverride(e.target.checked)}
            />
            <span><b>Override</b> (logged to audit trail)</span>
          </label>

          {override && (
            <label>
              <div style={{ fontWeight: 600 }}>Override reason</div>
              <textarea
                value={overrideReason}
                onChange={(e) => setOverrideReason(e.target.value)}
                placeholder="Explain why this FAIL/WARN is being overridden..."
                rows={3}
                style={{ width: "100%", padding: 10, borderRadius: 8, border: "1px solid #ccc" }}
              />
            </label>
          )}

          <button
            disabled={!canSubmit || loading}
            style={{
              padding: 12,
              borderRadius: 10,
              border: "1px solid #222",
              background: loading ? "#eee" : "#222",
              color: loading ? "#222" : "#fff",
              cursor: loading ? "not-allowed" : "pointer",
              fontWeight: 700
            }}
          >
            {loading ? "Validating..." : "Run Validation"}
          </button>

          {error && <div style={{ color: "crimson", fontWeight: 600 }}>{error}</div>}
        </div>
      </form>

      {result && (
        <div style={{ marginTop: 18, border: "1px solid #ddd", padding: 16, borderRadius: 10 }}>
          <h3 style={{ marginTop: 0 }}>
            Overall: <span style={severityStyle(result.overall_status)}>{result.overall_status}</span>
          </h3>

          <div style={{ fontSize: 13, opacity: 0.8 }}>
            Run ID: <b>{result.run_id}</b> • Doc Type: <b>{result.doc_type}</b>
          </div>

          {result.bid_summary && (
            <>
              <h4>Bid Summary</h4>
              <pre style={{ background: "#f7f7f7", padding: 10, borderRadius: 8, overflowX: "auto" }}>
                {JSON.stringify(result.bid_summary, null, 2)}
              </pre>
            </>
          )}

          {result.quote_summary && (
            <>
              <h4>Quote Summary</h4>
              <pre style={{ background: "#f7f7f7", padding: 10, borderRadius: 8, overflowX: "auto" }}>
                {JSON.stringify(result.quote_summary, null, 2)}
              </pre>
            </>
          )}

          <h4>Findings</h4>
          {result.findings?.length ? (
            <ul>
              {result.findings.map((f, idx) => (
                <li key={idx} style={{ marginBottom: 10 }}>
                  <div>
                    <span style={severityStyle(f.severity)}>{f.severity}</span>{" "}
                    — <code>{f.type}</code>
                    {Number.isInteger(f.row_index) ? (
                      <span style={{ opacity: 0.75 }}> (row {f.row_index})</span>
                    ) : null}
                  </div>
                  <div style={{ opacity: 0.85 }}>{f.message}</div>
                  {f.item_ref ? <div style={{ fontSize: 13, opacity: 0.8 }}>Item: {f.item_ref}</div> : null}
                  {f.pages?.length ? (
                    <div style={{ fontSize: 13, opacity: 0.8 }}>Pages: {f.pages.join(", ")}</div>
                  ) : null}
                </li>
              ))}
            </ul>
          ) : (
            <div>No findings.</div>
          )}

          <h4>Audit Log</h4>
          <div style={{ fontSize: 13, opacity: 0.8 }}>
            Append-only file: <code>{result.audit_log}</code>
          </div>
        </div>
      )}
    </div>
  );
}
