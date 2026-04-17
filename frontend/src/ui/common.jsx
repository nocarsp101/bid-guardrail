import React from "react";

// ----- shared UI primitives. No business logic lives here. ---------------

export function Panel({ title, subtitle, right, children, style }) {
  return (
    <div
      style={{
        border: "1px solid #ddd",
        borderRadius: 10,
        padding: 14,
        marginBottom: 14,
        background: "#fff",
        ...style,
      }}
    >
      {(title || right) && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: 6,
          }}
        >
          <div>
            {title && (
              <div style={{ fontWeight: 700, fontSize: 16 }}>{title}</div>
            )}
            {subtitle && (
              <div style={{ fontSize: 13, opacity: 0.7 }}>{subtitle}</div>
            )}
          </div>
          {right}
        </div>
      )}
      {children}
    </div>
  );
}

export function KeyValueGrid({ entries, columns = 2 }) {
  const rows = entries || [];
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))`,
        gap: 6,
        fontSize: 13,
      }}
    >
      {rows.map(([k, v], i) => (
        <div key={i} style={{ display: "flex", gap: 6 }}>
          <div style={{ opacity: 0.7 }}>{k}:</div>
          <div style={{ fontWeight: 600, wordBreak: "break-word" }}>
            {formatValue(v)}
          </div>
        </div>
      ))}
    </div>
  );
}

export function StateBadge({ label, value }) {
  const color = severityColor(value);
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 999,
        background: color.bg,
        color: color.fg,
        fontSize: 12,
        fontWeight: 700,
        marginRight: 6,
      }}
    >
      {label}: {formatValue(value)}
    </span>
  );
}

export function severityColor(value) {
  const v = String(value || "").toUpperCase();
  if (["BLOCKED", "HIGH_RISK", "FAIL", "CRITICAL"].includes(v))
    return { bg: "#fee2e2", fg: "#991b1b" };
  if (
    ["CONDITIONAL", "MEDIUM", "WARN", "ACTION_REQUIRED", "AT_RISK"].includes(v)
  )
    return { bg: "#fef3c7", fg: "#92400e" };
  if (
    ["READY", "SAFE", "LOW", "PASS", "APPROVED", "ALIGNED", "CLEAR"].includes(v)
  )
    return { bg: "#dcfce7", fg: "#166534" };
  return { bg: "#e5e7eb", fg: "#374151" };
}

export function formatValue(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export function Json({ data }) {
  return (
    <pre
      style={{
        background: "#f7f7f7",
        padding: 10,
        borderRadius: 8,
        overflowX: "auto",
        fontSize: 12,
        maxHeight: 260,
      }}
    >
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}

export function Button({ children, onClick, disabled, kind = "primary" }) {
  const palette = {
    primary: { bg: "#111827", fg: "#fff" },
    secondary: { bg: "#e5e7eb", fg: "#111827" },
    danger: { bg: "#b91c1c", fg: "#fff" },
  };
  const c = palette[kind] || palette.primary;
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: "8px 12px",
        borderRadius: 8,
        border: "1px solid #111827",
        background: disabled ? "#e5e7eb" : c.bg,
        color: disabled ? "#6b7280" : c.fg,
        cursor: disabled ? "not-allowed" : "pointer",
        fontWeight: 600,
        fontSize: 13,
      }}
    >
      {children}
    </button>
  );
}

export function TabBar({ tabs, active, onSelect }) {
  return (
    <div
      style={{
        display: "flex",
        gap: 6,
        borderBottom: "1px solid #ddd",
        marginBottom: 12,
      }}
    >
      {tabs.map((t) => (
        <button
          key={t.id}
          onClick={() => onSelect(t.id)}
          style={{
            padding: "8px 12px",
            background: "transparent",
            border: "none",
            borderBottom:
              active === t.id ? "2px solid #111827" : "2px solid transparent",
            fontWeight: active === t.id ? 700 : 500,
            cursor: "pointer",
            color: "#111827",
          }}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}

export function SourceRefsList({ refs }) {
  if (!refs || !refs.length) {
    return <div style={{ fontSize: 12, opacity: 0.6 }}>No source refs.</div>;
  }
  return (
    <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12 }}>
      {refs.map((r, i) => (
        <li key={i}>
          <code>{r.artifact_type}</code>{" "}
          <span style={{ opacity: 0.7 }}>
            record_id=<b>{r.record_id}</b> rev=
            <b>{String(r.revision_sequence ?? "—")}</b>
          </span>
        </li>
      ))}
    </ul>
  );
}

export function StateLabelBadges({ labels }) {
  const entries = Object.entries(labels || {}).filter(
    ([, v]) => v !== null && v !== undefined
  );
  if (!entries.length) return null;
  return (
    <div style={{ marginTop: 6 }}>
      {entries.map(([k, v]) => (
        <StateBadge key={k} label={k} value={v} />
      ))}
    </div>
  );
}

export function useAsync(fn, deps, options) {
  const refreshPulse = options?.refreshPulse ?? 0;
  const [state, setState] = React.useState({
    loading: true,
    data: null,
    error: null,
    version: 0,
    lastFetchedAt: null,
  });
  const cancelRef = React.useRef(0);
  const refresh = React.useCallback(() => {
    const myGen = ++cancelRef.current;
    setState((s) => ({ ...s, loading: true, error: null }));
    fn()
      .then((data) => {
        if (myGen !== cancelRef.current) return;
        setState((s) => ({
          loading: false,
          data,
          error: null,
          version: s.version + 1,
          lastFetchedAt: new Date().toISOString(),
        }));
      })
      .catch((err) => {
        if (myGen !== cancelRef.current) return;
        setState((s) => ({
          loading: false,
          data: null,
          error: err.message || String(err),
          version: s.version + 1,
          lastFetchedAt: new Date().toISOString(),
        }));
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps || []);
  React.useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refresh, refreshPulse]);
  return { ...state, refresh };
}

export function IdentityBar({ entries }) {
  const filtered = (entries || []).filter(
    ([, v]) => v !== null && v !== undefined && v !== ""
  );
  if (!filtered.length) return null;
  return (
    <div
      style={{
        display: "flex",
        gap: 8,
        flexWrap: "wrap",
        fontSize: 12,
        background: "#f9fafb",
        padding: "6px 8px",
        borderRadius: 6,
        marginBottom: 8,
      }}
    >
      {filtered.map(([k, v]) => (
        <span key={k}>
          <span style={{ opacity: 0.6 }}>{k}:</span>{" "}
          <code style={{ fontWeight: 600 }}>{String(v)}</code>
        </span>
      ))}
    </div>
  );
}
