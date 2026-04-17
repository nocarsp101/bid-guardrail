import React, { useState } from "react";
import { Panel, Button, Json, formatValue, IdentityBar } from "./common.jsx";
import { LoadingState, ErrorState, EmptyState } from "./states.jsx";
import { Breadcrumbs, useNav } from "./navContext.jsx";
import { getRevisionInspection } from "../api.js";

// C114 + C116/C117 — Revision diff inspection UI
export default function RevisionInspectionScreen() {
  const nav = useNav();
  const artifactType = nav.artifactType;
  const bidId = nav.bidId;
  const jobId = nav.jobId;

  const [state, setState] = useState({
    loading: true,
    data: null,
    error: null,
  });
  const [beforeRev, setBeforeRev] = useState(nav.beforeRev || "");
  const [afterRev, setAfterRev] = useState(nav.afterRev || "");

  const load = React.useCallback(() => {
    setState({ loading: true, data: null, error: null });
    getRevisionInspection({
      artifact_type: artifactType,
      bid_id: bidId,
      job_id: jobId,
      before_revision_sequence: beforeRev === "" ? null : Number(beforeRev),
      after_revision_sequence: afterRev === "" ? null : Number(afterRev),
    })
      .then((data) => setState({ loading: false, data, error: null }))
      .catch((err) =>
        setState({
          loading: false,
          data: null,
          error: err.message || String(err),
        })
      );
  }, [artifactType, bidId, jobId, beforeRev, afterRev]);

  React.useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [artifactType, bidId, jobId, nav.refreshPulse]);

  const crumbs = [
    { label: "Demo", onClick: () => nav.navigateTo({ screen: "demo" }) },
    bidId
      ? {
          label: `Package ${bidId}`,
          onClick: () =>
            nav.navigateTo({ screen: "package_overview", bidId }),
        }
      : null,
    { label: `Diff — ${artifactType || "(none)"}` },
  ].filter(Boolean);

  if (!artifactType) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <EmptyState
          title="Revision Inspection"
          message="No artifact_type selected."
          hint="Pick an artifact_type in the header."
        />
      </>
    );
  }

  const data = state.data || {};
  const body = data.body || {};
  const diff = body.latest_diff;
  const summary = body.diff_summary;
  const lineage = body.lineage_diffs || [];

  return (
    <div>
      <Breadcrumbs trail={crumbs} />
      <Panel
        title={`Revision Inspection — ${artifactType}`}
        right={
          <div style={{ display: "flex", gap: 6 }}>
            {nav.historyDepth > 0 && (
              <Button onClick={nav.goBack} kind="secondary">
                ← Back
              </Button>
            )}
            <Button onClick={load} kind="secondary">
              Refresh
            </Button>
            {bidId && (
              <Button
                onClick={() =>
                  nav.navigateTo({ screen: "bid_readiness", bidId })
                }
                kind="secondary"
              >
                → Bid Readiness
              </Button>
            )}
            <Button
              onClick={() =>
                nav.navigateTo({ screen: "timeline", bidId, jobId })
              }
              kind="secondary"
            >
              → Timeline
            </Button>
          </div>
        }
      >
        <IdentityBar
          entries={[
            ["artifact_type", artifactType],
            ["bid_id", bidId],
            ["job_id", jobId],
          ]}
        />
        <div
          style={{
            display: "flex",
            gap: 8,
            fontSize: 13,
            alignItems: "center",
          }}
        >
          <label>
            before:
            <input
              value={beforeRev}
              onChange={(e) => setBeforeRev(e.target.value)}
              placeholder="rev"
              style={{
                marginLeft: 4,
                padding: 4,
                width: 60,
                borderRadius: 6,
                border: "1px solid #ccc",
              }}
            />
          </label>
          <label>
            after:
            <input
              value={afterRev}
              onChange={(e) => setAfterRev(e.target.value)}
              placeholder="rev"
              style={{
                marginLeft: 4,
                padding: 4,
                width: 60,
                borderRadius: 6,
                border: "1px solid #ccc",
              }}
            />
          </label>
          <Button onClick={load} kind="secondary">
            Apply
          </Button>
          <Button
            onClick={() => {
              setBeforeRev("");
              setAfterRev("");
              load();
            }}
            kind="secondary"
          >
            Latest vs previous
          </Button>
        </div>
      </Panel>

      {state.loading && <LoadingState title="Diff" />}
      {state.error && (
        <ErrorState title="Diff failed" error={state.error} onRetry={load} />
      )}

      {!state.loading && !state.error && (
        <>
          <Panel title="Summary">
            <div style={{ fontSize: 13 }}>
              history_length=
              <b>{formatValue(data?.state_labels?.history_length)}</b> ·
              before_rev=<b>{formatValue(summary?.before_revision)}</b> ·
              after_rev=<b>{formatValue(summary?.after_revision)}</b> · status=
              <b>{formatValue(diff?.status)}</b>
            </div>
          </Panel>

          <Panel title="Changed Fields">
            {(diff?.changed_fields || []).length === 0 ? (
              <div style={{ fontSize: 13, opacity: 0.6 }}>
                No changed fields.
              </div>
            ) : (
              <ul style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
                {diff.changed_fields.map((c, i) => (
                  <li key={i}>
                    <code>{c.field_path}</code>:{" "}
                    <span style={{ opacity: 0.7 }}>
                      {formatValue(c.before)} →{" "}
                    </span>
                    <b>{formatValue(c.after)}</b>
                  </li>
                ))}
              </ul>
            )}
          </Panel>

          <Panel title="Unchanged Fields">
            {(diff?.unchanged_fields || []).length === 0 ? (
              <div style={{ fontSize: 13, opacity: 0.6 }}>
                No unchanged fields.
              </div>
            ) : (
              <ul style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
                {diff.unchanged_fields.map((c, i) => (
                  <li key={i}>
                    <code>{c.field_path}</code>: <b>{formatValue(c.after)}</b>
                  </li>
                ))}
              </ul>
            )}
          </Panel>

          <Panel title="Lineage Diffs">
            {lineage.length === 0 ? (
              <div style={{ fontSize: 13, opacity: 0.6 }}>
                No lineage diffs.
              </div>
            ) : (
              <Json data={lineage} />
            )}
          </Panel>

          <Panel title="Raw diff payload">
            <Json data={diff || {}} />
          </Panel>
        </>
      )}
    </div>
  );
}
