import React from "react";
import { Panel, Button } from "./common.jsx";

// C117 — Shared loading / error / empty state primitives.

export function LoadingState({ title = "Loading…", lines = 3 }) {
  return (
    <Panel title={title}>
      {Array.from({ length: lines }).map((_, i) => (
        <div
          key={i}
          style={{
            height: 10,
            background: "#eef2f7",
            borderRadius: 6,
            marginBottom: 8,
            width: `${80 - i * 10}%`,
          }}
        />
      ))}
    </Panel>
  );
}

export function ErrorState({ title = "Something went wrong", error, onRetry }) {
  return (
    <Panel title={title} subtitle="fail-closed">
      <div
        style={{
          padding: 12,
          background: "#fef2f2",
          color: "#991b1b",
          borderRadius: 8,
          fontSize: 13,
          marginBottom: 10,
          wordBreak: "break-word",
        }}
      >
        {String(error || "Unknown error.")}
      </div>
      {onRetry && (
        <Button onClick={onRetry} kind="secondary">
          Retry
        </Button>
      )}
    </Panel>
  );
}

export function EmptyState({
  title = "No data",
  message = "No records available yet.",
  hint,
  action,
}) {
  return (
    <Panel title={title}>
      <div style={{ fontSize: 13, opacity: 0.75 }}>{message}</div>
      {hint && (
        <div style={{ fontSize: 12, opacity: 0.6, marginTop: 4 }}>{hint}</div>
      )}
      {action && <div style={{ marginTop: 8 }}>{action}</div>}
    </Panel>
  );
}

export function FailClosedBanner({ message }) {
  return (
    <div
      style={{
        border: "1px solid #fecaca",
        background: "#fef2f2",
        color: "#991b1b",
        padding: 10,
        borderRadius: 8,
        fontSize: 13,
        marginBottom: 10,
      }}
    >
      <b>Fail-closed:</b> {message}
    </div>
  );
}

export function Toast({ kind = "info", children, onClose }) {
  const palette = {
    info: { bg: "#eff6ff", fg: "#1d4ed8", border: "#bfdbfe" },
    success: { bg: "#ecfdf5", fg: "#065f46", border: "#a7f3d0" },
    warn: { bg: "#fffbeb", fg: "#92400e", border: "#fde68a" },
    error: { bg: "#fef2f2", fg: "#991b1b", border: "#fecaca" },
  };
  const c = palette[kind] || palette.info;
  return (
    <div
      style={{
        border: `1px solid ${c.border}`,
        background: c.bg,
        color: c.fg,
        padding: "8px 10px",
        borderRadius: 8,
        fontSize: 13,
        marginBottom: 8,
        display: "flex",
        justifyContent: "space-between",
        alignItems: "flex-start",
        gap: 8,
      }}
    >
      <div style={{ flex: 1 }}>{children}</div>
      {onClose && (
        <button
          onClick={onClose}
          style={{
            border: "none",
            background: "transparent",
            color: c.fg,
            cursor: "pointer",
            fontWeight: 700,
          }}
        >
          ×
        </button>
      )}
    </div>
  );
}

export function ConfirmDialog({ open, title, message, onConfirm, onCancel, kind = "primary" }) {
  if (!open) return null;
  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(15, 23, 42, 0.45)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        style={{
          background: "#fff",
          borderRadius: 10,
          padding: 18,
          width: 420,
          boxShadow: "0 10px 25px rgba(0,0,0,0.25)",
        }}
      >
        <div style={{ fontWeight: 700, fontSize: 16, marginBottom: 6 }}>
          {title || "Confirm action"}
        </div>
        <div style={{ fontSize: 13, marginBottom: 14 }}>{message}</div>
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 6 }}>
          <Button onClick={onCancel} kind="secondary">
            Cancel
          </Button>
          <Button onClick={onConfirm} kind={kind}>
            Confirm
          </Button>
        </div>
      </div>
    </div>
  );
}

export function InFlightOverlay({ active, label = "Working…" }) {
  if (!active) return null;
  return (
    <div
      style={{
        position: "absolute",
        inset: 0,
        background: "rgba(255,255,255,0.6)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 13,
        color: "#111827",
        fontWeight: 600,
        borderRadius: 10,
      }}
    >
      {label}
    </div>
  );
}

export function TraceRow({ label, value }) {
  if (value === null || value === undefined) return null;
  return (
    <div style={{ fontSize: 12, opacity: 0.75 }}>
      <code>{label}</code>:{" "}
      <span style={{ fontWeight: 600 }}>{String(value)}</span>
    </div>
  );
}
