import React, { useState } from "react";
import { Panel, Button, Json, useAsync } from "./common.jsx";
import { Toast, ConfirmDialog, InFlightOverlay, LoadingState, ErrorState } from "./states.jsx";
import { Breadcrumbs, useNav } from "./navContext.jsx";
import {
  listDemoScenarios,
  runDemoScenario,
  runProductDemo,
  runUiDemo,
  getRepositorySummary,
  resetRepository,
  getArtifactsByBid,
  getConfigSummary,
} from "../api.js";

// C115 + C116/C118/C120 — Demo Harness launcher + runtime panel.
export default function DemoHarnessScreen() {
  const nav = useNav();
  const scenarios = useAsync(() => listDemoScenarios(), []);
  const repo = useAsync(() => getRepositorySummary(), [], {
    refreshPulse: nav.refreshPulse,
  });
  const config = useAsync(() => getConfigSummary(), []);

  const [busy, setBusy] = useState(false);
  const [lastRun, setLastRun] = useState(null);
  const [artifacts, setArtifacts] = useState(null);
  const [selectedScenario, setSelectedScenario] = useState(
    "proceed_with_caveats"
  );
  const [toast, setToast] = useState(null);
  const [confirm, setConfirm] = useState(null);

  async function loadArtifactsForBid(bidId) {
    try {
      const arts = await getArtifactsByBid(bidId);
      setArtifacts({ bidId, records: arts.records || [] });
    } catch (err) {
      setArtifacts({ error: err.message || String(err) });
    }
  }

  async function run(fn, label) {
    try {
      setBusy(true);
      const res = await fn();
      setLastRun({ label, at: new Date().toISOString(), res });
      repo.refresh();
      nav.triggerRefresh();
      if (res?.bid_id) {
        await loadArtifactsForBid(res.bid_id);
      } else if (res?.scenario_results?.length) {
        const bid = res.scenario_results[0]?.bid_id;
        if (bid) await loadArtifactsForBid(bid);
      }
      setToast({ kind: "success", text: `${label} → ok` });
      return res;
    } catch (err) {
      setLastRun({
        label,
        at: new Date().toISOString(),
        error: err.message || String(err),
      });
      setToast({ kind: "error", text: `${label}: ${err.message || err}` });
    } finally {
      setBusy(false);
    }
  }

  function askReset() {
    setConfirm({
      title: "Reset canonical repository",
      message:
        "This wipes all canonical records from the running backend. It is intended for dev/demo only. Continue?",
      kind: "danger",
      onConfirm: async () => {
        setConfirm(null);
        await run(() => resetRepository(), "reset canonical repository");
        setArtifacts(null);
      },
      onCancel: () => setConfirm(null),
    });
  }

  const runtimeMode =
    config?.data?.environment ||
    (config?.data?.config && config.data.config.environment) ||
    "dev";
  const validationOk =
    config?.data?.validation?.ok ??
    (config?.data?.validation && config.data.validation.ok);

  return (
    <div>
      <Breadcrumbs trail={[{ label: "Demo Harness" }]} />
      {toast && (
        <Toast kind={toast.kind} onClose={() => setToast(null)}>
          {toast.text}
        </Toast>
      )}
      <Panel
        title="Runtime"
        subtitle={`environment=${runtimeMode}`}
        right={
          validationOk === false ? (
            <span
              style={{
                background: "#fef2f2",
                color: "#991b1b",
                fontSize: 12,
                padding: "2px 8px",
                borderRadius: 999,
                fontWeight: 700,
              }}
            >
              CONFIG INVALID
            </span>
          ) : validationOk ? (
            <span
              style={{
                background: "#ecfdf5",
                color: "#065f46",
                fontSize: 12,
                padding: "2px 8px",
                borderRadius: 999,
                fontWeight: 700,
              }}
            >
              CONFIG OK
            </span>
          ) : null
        }
      >
        {config.loading && <LoadingState lines={1} />}
        {config.error && <ErrorState error={config.error} onRetry={config.refresh} />}
        {config.data && <Json data={config.data} />}
      </Panel>

      <Panel
        title="Demo Harness"
        subtitle="Seeded scenarios exercise backend canonical flows."
        right={
          <div style={{ display: "flex", gap: 6 }}>
            <Button onClick={scenarios.refresh} kind="secondary">
              Refresh scenarios
            </Button>
            <Button onClick={repo.refresh} kind="secondary">
              Refresh repo
            </Button>
            <Button kind="danger" disabled={busy} onClick={askReset}>
              Reset repository
            </Button>
          </div>
        }
      >
        <div style={{ position: "relative" }}>
          <InFlightOverlay active={busy} label="Running scenario…" />
          <div
            style={{
              display: "flex",
              gap: 8,
              alignItems: "center",
              flexWrap: "wrap",
            }}
          >
            <label style={{ fontSize: 13 }}>Scenario:</label>
            <select
              value={selectedScenario}
              onChange={(e) => setSelectedScenario(e.target.value)}
              style={{
                padding: 6,
                borderRadius: 6,
                border: "1px solid #ccc",
              }}
            >
              {(scenarios?.data?.scenarios || []).map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
            <Button
              disabled={busy}
              onClick={() =>
                run(
                  () => runDemoScenario(selectedScenario),
                  `runDemoScenario:${selectedScenario}`
                )
              }
            >
              Run scenario e2e
            </Button>
            <Button
              disabled={busy}
              onClick={() =>
                run(
                  () => runProductDemo(selectedScenario),
                  `runProductDemo:${selectedScenario}`
                )
              }
            >
              Run product demo
            </Button>
            <Button
              disabled={busy}
              onClick={() =>
                run(
                  () => runUiDemo(selectedScenario),
                  `runUiDemo:${selectedScenario}`
                )
              }
            >
              Run UI demo
            </Button>
          </div>
        </div>
      </Panel>

      <Panel title="Repository Summary">
        {repo.loading && <LoadingState lines={2} />}
        {repo.error && (
          <ErrorState error={repo.error} onRetry={repo.refresh} />
        )}
        {repo.data && <Json data={repo.data} />}
      </Panel>

      {lastRun && (
        <Panel
          title="Last run"
          subtitle={`${lastRun.label} @ ${lastRun.at}`}
          right={
            lastRun.res?.bid_id ? (
              <Button
                onClick={() =>
                  nav.navigateTo({
                    screen: "package_overview",
                    bidId: lastRun.res.bid_id,
                  })
                }
              >
                Open bid: {lastRun.res.bid_id}
              </Button>
            ) : null
          }
        >
          {lastRun.error ? (
            <div style={{ color: "crimson" }}>{lastRun.error}</div>
          ) : (
            <Json data={lastRun.res} />
          )}
        </Panel>
      )}

      {artifacts && artifacts.bidId && (
        <Panel title={`Artifacts for ${artifacts.bidId}`}>
          <table
            style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}
          >
            <thead>
              <tr style={{ background: "#f3f4f6" }}>
                <th style={cellStyle}>artifact_type</th>
                <th style={cellStyle}>record_id</th>
                <th style={cellStyle}>rev</th>
                <th style={cellStyle}>superseded_by</th>
                <th style={cellStyle}></th>
              </tr>
            </thead>
            <tbody>
              {(artifacts.records || []).map((r, i) => {
                const art = (r.envelope && r.envelope.artifact) || {};
                const jobId = art.job_id;
                return (
                  <tr key={i} style={{ borderBottom: "1px solid #e5e7eb" }}>
                    <td style={cellStyle}>
                      <code>{r.artifact_type}</code>
                    </td>
                    <td style={cellStyle}>
                      <code>{r.record_id}</code>
                    </td>
                    <td style={cellStyle}>{r.revision_sequence}</td>
                    <td style={cellStyle}>
                      {r.superseded_by ? <code>{r.superseded_by}</code> : "—"}
                    </td>
                    <td style={cellStyle}>
                      {r.artifact_type === "quote_dossier" && jobId && (
                        <Button
                          kind="secondary"
                          onClick={() =>
                            nav.navigateTo({
                              screen: "quote_case",
                              jobId,
                              bidId: artifacts.bidId,
                            })
                          }
                        >
                          Open quote case
                        </Button>
                      )}
                      {r.artifact_type === "bid_readiness_snapshot" && (
                        <Button
                          kind="secondary"
                          onClick={() =>
                            nav.navigateTo({
                              screen: "bid_readiness",
                              bidId: artifacts.bidId,
                            })
                          }
                        >
                          Open bid readiness
                        </Button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </Panel>
      )}

      <ConfirmDialog
        open={!!confirm}
        title={confirm?.title}
        message={confirm?.message}
        onConfirm={confirm?.onConfirm}
        onCancel={confirm?.onCancel}
        kind={confirm?.kind}
      />
    </div>
  );
}

const cellStyle = {
  padding: "4px 6px",
  textAlign: "left",
  verticalAlign: "top",
};
