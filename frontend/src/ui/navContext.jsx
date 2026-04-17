import React from "react";

// C116 — Shared navigation + identity context.
// Centralizes selected bid/job/artifact + nav history + refresh pulses so
// every screen observes the same source of truth.

const NavContext = React.createContext(null);

const LS_KEY = "bid_guardrail_nav_v2";

function readPersisted() {
  try {
    const raw = localStorage.getItem(LS_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    return null;
  }
}

function writePersisted(state) {
  try {
    localStorage.setItem(LS_KEY, JSON.stringify(state));
  } catch (e) {
    // ignore
  }
}

export function NavProvider({ children, initial }) {
  const persisted = readPersisted();
  const baseState = {
    active: "demo",
    bidId: "",
    jobId: "",
    artifactType: "bid_readiness_snapshot",
    operator: "zain.rajput",
    beforeRev: "",
    afterRev: "",
    runtimeMode: "dev",
    ...(initial || {}),
    ...(persisted || {}),
  };
  const [state, setState] = React.useState(baseState);
  const [history, setHistory] = React.useState([]);
  const [refreshPulse, setRefreshPulse] = React.useState(0);

  React.useEffect(() => {
    writePersisted({
      active: state.active,
      bidId: state.bidId,
      jobId: state.jobId,
      artifactType: state.artifactType,
      operator: state.operator,
      runtimeMode: state.runtimeMode,
    });
  }, [state.active, state.bidId, state.jobId, state.artifactType,
       state.operator, state.runtimeMode]);

  const navigateTo = React.useCallback(
    (target, options = {}) => {
      if (!target) return;
      setHistory((h) => {
        if (options.replace) return h;
        return [
          ...h,
          {
            active: state.active,
            bidId: state.bidId,
            jobId: state.jobId,
            artifactType: state.artifactType,
          },
        ].slice(-20);
      });
      setState((s) => ({
        ...s,
        ...("bidId" in target ? { bidId: target.bidId || "" } : {}),
        ...("jobId" in target ? { jobId: target.jobId || "" } : {}),
        ...(target.artifactType ? { artifactType: target.artifactType } : {}),
        ...(target.screen ? { active: target.screen } : {}),
      }));
    },
    [state.active, state.bidId, state.jobId, state.artifactType]
  );

  const goBack = React.useCallback(() => {
    setHistory((h) => {
      if (!h.length) return h;
      const prev = h[h.length - 1];
      setState((s) => ({ ...s, ...prev }));
      return h.slice(0, -1);
    });
  }, []);

  const setField = React.useCallback((field, value) => {
    setState((s) => ({ ...s, [field]: value }));
  }, []);

  const triggerRefresh = React.useCallback(() => {
    setRefreshPulse((p) => p + 1);
  }, []);

  const value = React.useMemo(
    () => ({
      ...state,
      setField,
      navigateTo,
      goBack,
      historyDepth: history.length,
      refreshPulse,
      triggerRefresh,
    }),
    [state, setField, navigateTo, goBack, history.length, refreshPulse,
     triggerRefresh]
  );
  return <NavContext.Provider value={value}>{children}</NavContext.Provider>;
}

export function useNav() {
  const ctx = React.useContext(NavContext);
  if (!ctx) throw new Error("useNav must be used inside <NavProvider>");
  return ctx;
}

export function Breadcrumbs({ trail }) {
  const items = trail || [];
  if (!items.length) return null;
  return (
    <div
      style={{
        fontSize: 12,
        opacity: 0.75,
        marginBottom: 8,
        display: "flex",
        gap: 4,
        flexWrap: "wrap",
      }}
    >
      {items.map((seg, i) => (
        <span key={i}>
          {i > 0 && <span style={{ opacity: 0.5 }}> › </span>}
          {seg.onClick ? (
            <button
              onClick={seg.onClick}
              style={{
                background: "none",
                border: "none",
                color: "#1d4ed8",
                cursor: "pointer",
                textDecoration: "underline",
                padding: 0,
                fontSize: 12,
              }}
            >
              {seg.label}
            </button>
          ) : (
            <span style={{ fontWeight: 600 }}>{seg.label}</span>
          )}
        </span>
      ))}
    </div>
  );
}
