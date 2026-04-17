import React, { useState } from "react";
import {
  Panel,
  KeyValueGrid,
  StateLabelBadges,
  SourceRefsList,
  Button,
  useAsync,
  Json,
  IdentityBar,
} from "./common.jsx";
import {
  LoadingState,
  ErrorState,
  EmptyState,
  Toast,
  ConfirmDialog,
  InFlightOverlay,
  FailClosedBanner,
} from "./states.jsx";
import { Breadcrumbs, useNav } from "./navContext.jsx";
import DownloadsPanel from "./DownloadsPanel.jsx";
import {
  getBidReadinessScreen,
  executeCommand,
  getCommandReceipts,
} from "../api.js";

// C112 + C113 + C116/C117/C118 — Bid Readiness Screen
export default function BidReadinessScreen() {
  const nav = useNav();
  const bidId = nav.bidId;
  const operator = nav.operator;
  const { loading, data, error, refresh, lastFetchedAt } = useAsync(
    () => getBidReadinessScreen(bidId),
    [bidId],
    { refreshPulse: nav.refreshPulse }
  );
  const [busy, setBusy] = useState(false);
  const [actionLog, setActionLog] = useState([]);
  const [note, setNote] = useState("");
  const [toast, setToast] = useState(null);
  const [confirm, setConfirm] = useState(null);

  async function runCommand(summary, body, { destructive = false } = {}) {
    if (destructive) {
      return new Promise((resolve) => {
        setConfirm({
          title: `Confirm: ${summary}`,
          message: `This appends a new revision and is visible to all operators. Proceed?`,
          onConfirm: async () => {
            setConfirm(null);
            resolve(await actuallyRun(summary, body));
          },
          onCancel: () => {
            setConfirm(null);
            resolve({ cancelled: true });
          },
          kind: "danger",
        });
      });
    }
    return actuallyRun(summary, body);
  }

  async function actuallyRun(summary, body) {
    try {
      setBusy(true);
      const issuedAt = new Date().toISOString();
      const receipt = await executeCommand({
        command: body.command,
        payload: body.payload,
        issuedBy: operator,
        issuedAt,
      });
      const status = receipt?.status || receipt?.result?.status || "unknown";
      setActionLog((xs) =>
        [
          {
            at: issuedAt,
            summary,
            status,
            command_id: receipt?.command_id,
            result: receipt?.result,
          },
          ...xs,
        ].slice(0, 15)
      );
      setToast({
        kind:
          status === "ok" ? "success" : status === "invalid_transition"
            ? "warn"
            : "error",
        text: `${summary} → ${status}`,
      });
      // Canonical post-command refresh.
      refresh();
      nav.triggerRefresh();
      return receipt;
    } catch (err) {
      setActionLog((xs) =>
        [
          {
            at: new Date().toISOString(),
            summary,
            status: "error",
            error: err.message || String(err),
          },
          ...xs,
        ].slice(0, 15)
      );
      setToast({ kind: "error", text: `${summary}: ${err.message || err}` });
      return { error: err.message || String(err) };
    } finally {
      setBusy(false);
    }
  }

  const crumbs = [
    { label: "Demo", onClick: () => nav.navigateTo({ screen: "demo" }) },
    bidId
      ? {
          label: `Package ${bidId}`,
          onClick: () =>
            nav.navigateTo({ screen: "package_overview", bidId }),
        }
      : null,
    { label: "Bid Readiness" },
  ].filter(Boolean);

  if (!bidId) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <EmptyState
          title="Bid Readiness"
          message="No bid_id selected."
          action={
            <Button onClick={() => nav.navigateTo({ screen: "demo" })}>
              Open demo harness
            </Button>
          }
        />
      </>
    );
  }
  if (loading) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <LoadingState title={`Bid Readiness — ${bidId}`} />
      </>
    );
  }
  if (error) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <ErrorState
          title="Bid Readiness failed to load"
          error={error}
          onRetry={refresh}
        />
      </>
    );
  }

  const body = data?.body || {};
  const pq = body.priority_queue || {};
  const carry = body.carry_justification || {};
  const top_reasons = body.top_reasons || [];
  const top_items = body.top_unresolved_items || [];
  const top_actions = body.top_priority_queue_actions || [];
  const readinessState = data?.state_labels?.readiness_state
    || data?.state_labels?.overall_readiness;
  const blocked = String(readinessState || "").toUpperCase() === "BLOCKED";

  return (
    <div style={{ position: "relative" }}>
      <Breadcrumbs trail={crumbs} />
      {toast && (
        <Toast kind={toast.kind} onClose={() => setToast(null)}>
          {toast.text}
        </Toast>
      )}
      {blocked && (
        <FailClosedBanner message="Bid readiness is BLOCKED — operator actions should prefer hold/clarify rather than approval." />
      )}

      <Panel
        title={`Bid Readiness — ${bidId}`}
        subtitle={`fetched ${lastFetchedAt || "—"}`}
        right={
          <div style={{ display: "flex", gap: 6 }}>
            {nav.historyDepth > 0 && (
              <Button onClick={nav.goBack} kind="secondary">
                ← Back
              </Button>
            )}
            <Button onClick={refresh} kind="secondary">
              Refresh
            </Button>
            <Button
              onClick={() =>
                nav.navigateTo({ screen: "package_overview", bidId })
              }
              kind="secondary"
            >
              → Package Overview
            </Button>
            <Button
              onClick={() =>
                nav.navigateTo({ screen: "authority_action", bidId })
              }
              kind="secondary"
            >
              → Authority Action
            </Button>
            <Button
              onClick={() => nav.navigateTo({ screen: "timeline", bidId })}
              kind="secondary"
            >
              → Timeline
            </Button>
            <Button
              onClick={() =>
                nav.navigateTo({
                  screen: "revision_inspection",
                  artifactType: "bid_readiness_snapshot",
                  bidId,
                })
              }
              kind="secondary"
            >
              → Diff
            </Button>
          </div>
        }
      >
        <IdentityBar entries={[["bid_id", bidId], ["operator", operator]]} />
        <StateLabelBadges labels={data?.state_labels} />
      </Panel>

      <Panel title="Package Confidence">
        {Object.keys(body.package_confidence || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No confidence sub-structure.
          </div>
        ) : (
          <Json data={body.package_confidence || {}} />
        )}
      </Panel>

      <Panel title="Authority Posture">
        {Object.keys(body.authority_posture || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No authority posture on this snapshot.
          </div>
        ) : (
          <Json data={body.authority_posture || {}} />
        )}
      </Panel>

      <Panel title="Deadline Pressure">
        {Object.keys(body.deadline_pressure || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No deadline pressure data.
          </div>
        ) : (
          <Json data={body.deadline_pressure || {}} />
        )}
      </Panel>

      <Panel title="Top Reasons">
        {top_reasons.length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>No top reasons.</div>
        ) : (
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
            {top_reasons.map((r, i) => (
              <li key={i}>{typeof r === "string" ? r : JSON.stringify(r)}</li>
            ))}
          </ul>
        )}
      </Panel>

      <Panel title="Top Unresolved Items">
        {top_items.length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>No unresolved items.</div>
        ) : (
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
            {top_items.map((r, i) => (
              <li key={i}>
                <code>{r.item_type || r.queue_item_id || "item"}</code>{" "}
                <span style={{ opacity: 0.8 }}>{r.reason || ""}</span>
              </li>
            ))}
          </ul>
        )}
      </Panel>

      <Panel title="Priority Queue Actions">
        {top_actions.length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>No priority actions.</div>
        ) : (
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
            {top_actions.map((r, i) => (
              <li key={i}>
                <code>{r.action_bucket}</code> — {r.item_type || r.reason}
              </li>
            ))}
          </ul>
        )}
      </Panel>

      <Panel title="Carry Justification">
        <KeyValueGrid
          entries={[
            ["carry_decision", carry.carry_decision],
            ["carry_progression_state", carry.carry_progression_state],
            ["decided_by", carry.decided_by],
            ["decided_at", carry.decided_at],
          ]}
        />
      </Panel>

      <Panel
        title="Operator Actions (C113 + C118)"
        subtitle="Every command appends a new revision and refreshes the view."
      >
        <div style={{ position: "relative" }}>
          <InFlightOverlay active={busy} label="Dispatching…" />
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <Button
              disabled={busy}
              onClick={() =>
                runCommand("acknowledge_review", {
                  command: "acknowledge_review",
                  payload: { bid_id: bidId },
                })
              }
            >
              Acknowledge review
            </Button>
            <Button
              disabled={busy}
              onClick={() =>
                runCommand("carry_advance → under_review", {
                  command: "carry_advance",
                  payload: { bid_id: bidId, next_state: "under_review" },
                })
              }
            >
              Carry: under_review
            </Button>
            <Button
              disabled={busy}
              onClick={() =>
                runCommand(
                  "carry_advance → approved",
                  {
                    command: "carry_advance",
                    payload: { bid_id: bidId, next_state: "approved" },
                  },
                  { destructive: true }
                )
              }
            >
              Carry: approved
            </Button>
            <Button
              disabled={busy}
              kind="danger"
              onClick={() =>
                runCommand(
                  "carry_advance → rejected",
                  {
                    command: "carry_advance",
                    payload: { bid_id: bidId, next_state: "rejected" },
                  },
                  { destructive: true }
                )
              }
            >
              Carry: rejected
            </Button>
          </div>

          <div style={{ marginTop: 10 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Capture note</div>
            <div style={{ display: "flex", gap: 6, marginTop: 4 }}>
              <input
                value={note}
                onChange={(e) => setNote(e.target.value)}
                placeholder="Operator note text"
                style={{
                  flex: 1,
                  padding: 8,
                  borderRadius: 6,
                  border: "1px solid #ccc",
                }}
              />
              <Button
                disabled={busy || !note.trim()}
                onClick={async () => {
                  const val = note;
                  await runCommand("capture_note", {
                    command: "capture_note",
                    payload: { bid_id: bidId, note: val, tag: "ui" },
                  });
                  setNote("");
                }}
              >
                Submit note
              </Button>
            </div>
          </div>

          {actionLog.length > 0 && (
            <div style={{ marginTop: 12 }}>
              <div
                style={{
                  fontSize: 13,
                  fontWeight: 600,
                  marginBottom: 6,
                }}
              >
                Recent action receipts
              </div>
              <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12 }}>
                {actionLog.map((l, i) => (
                  <li key={i}>
                    <span style={{ opacity: 0.6 }}>{l.at}</span>{" "}
                    <code>{l.summary}</code> → <b>{l.status}</b>
                    {l.command_id && (
                      <span style={{ opacity: 0.6 }}>
                        {" "}
                        · id=<code>{l.command_id}</code>
                      </span>
                    )}
                    {l.error && (
                      <span style={{ color: "crimson" }}> {l.error}</span>
                    )}
                  </li>
                ))}
              </ul>
              <div style={{ marginTop: 6 }}>
                <Button
                  kind="secondary"
                  onClick={async () => {
                    const all = await getCommandReceipts();
                    setActionLog(
                      (all.receipts || [])
                        .slice(-15)
                        .reverse()
                        .map((r) => ({
                          at: r.issued_at,
                          summary: r.command,
                          status: r.status,
                          command_id: r.command_id,
                          result: r.result,
                        }))
                    );
                  }}
                >
                  Sync server receipts
                </Button>
              </div>
            </div>
          )}
        </div>
      </Panel>

      <DownloadsPanel bidId={bidId} />

      <Panel title="Source Refs">
        <SourceRefsList refs={data?.source_refs} />
      </Panel>

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
