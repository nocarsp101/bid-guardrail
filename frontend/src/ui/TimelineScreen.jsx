import React from "react";
import {
  Panel,
  Button,
  useAsync,
  Json,
  formatValue,
  IdentityBar,
} from "./common.jsx";
import { LoadingState, ErrorState, EmptyState } from "./states.jsx";
import { Breadcrumbs, useNav } from "./navContext.jsx";
import { getTimelineScreen } from "../api.js";

// C114 + C116/C117 — Timeline viewer
export default function TimelineScreen() {
  const nav = useNav();
  const bidId = nav.bidId;
  const jobId = nav.jobId;
  const { loading, data, error, refresh, lastFetchedAt } = useAsync(
    () => getTimelineScreen({ bidId, jobId }),
    [bidId, jobId],
    { refreshPulse: nav.refreshPulse }
  );

  const crumbs = [
    { label: "Demo", onClick: () => nav.navigateTo({ screen: "demo" }) },
    bidId
      ? {
          label: `Package ${bidId}`,
          onClick: () =>
            nav.navigateTo({ screen: "package_overview", bidId }),
        }
      : null,
    jobId
      ? {
          label: `Quote ${jobId}`,
          onClick: () => nav.navigateTo({ screen: "quote_case", jobId }),
        }
      : null,
    { label: "Timeline" },
  ].filter(Boolean);

  if (!bidId && !jobId) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <EmptyState
          title="Timeline"
          message="Provide a bid_id or job_id in the header to load timelines."
        />
      </>
    );
  }
  if (loading) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <LoadingState title="Timeline" />
      </>
    );
  }
  if (error) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <ErrorState
          title="Timeline failed to load"
          error={error}
          onRetry={refresh}
        />
      </>
    );
  }

  const body = data?.body || {};
  const kinds = body.kind_timelines || [];
  const merged = body.merged_timeline || {};

  return (
    <div>
      <Breadcrumbs trail={crumbs} />
      <Panel
        title="Timeline"
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
            {jobId && (
              <Button
                onClick={() =>
                  nav.navigateTo({ screen: "quote_case", jobId })
                }
                kind="secondary"
              >
                → Quote Case
              </Button>
            )}
          </div>
        }
      >
        <IdentityBar entries={[["bid_id", bidId], ["job_id", jobId]]} />
        <div style={{ fontSize: 13, opacity: 0.7 }}>
          Merged event count:{" "}
          <b>{formatValue((merged.timeline_summary || {}).event_count)}</b>
        </div>
      </Panel>

      {kinds.length === 0 ? (
        <EmptyState
          title="No timelines available"
          message="No canonical revisions stored for this identity yet."
          hint="Seed a scenario or run an operator action to create revisions."
        />
      ) : (
        kinds.map((tl, i) => (
          <Panel
            key={i}
            title={`${tl.artifact_kind || "artifact"} timeline`}
            subtitle={`${(tl.timeline_summary || {}).event_count || 0} event(s)`}
            right={
              tl.artifact_kind && (
                <Button
                  kind="secondary"
                  onClick={() =>
                    nav.navigateTo({
                      screen: "revision_inspection",
                      artifactType: tl.artifact_kind,
                      bidId,
                      jobId,
                    })
                  }
                >
                  Inspect diff
                </Button>
              )
            }
          >
            <table
              style={{
                width: "100%",
                fontSize: 12,
                borderCollapse: "collapse",
              }}
            >
              <thead>
                <tr style={{ background: "#f3f4f6" }}>
                  <th style={cellStyle}>rev</th>
                  <th style={cellStyle}>record_id</th>
                  <th style={cellStyle}>state</th>
                  <th style={cellStyle}>created_at</th>
                  <th style={cellStyle}>created_by</th>
                  <th style={cellStyle}>supersedes</th>
                </tr>
              </thead>
              <tbody>
                {(tl.events || []).map((e, j) => (
                  <tr key={j} style={{ borderBottom: "1px solid #e5e7eb" }}>
                    <td style={cellStyle}>{e.revision_sequence ?? "—"}</td>
                    <td style={cellStyle}>
                      <code>{e.record_id || "—"}</code>
                    </td>
                    <td style={cellStyle}>{formatValue(e.state)}</td>
                    <td style={cellStyle}>{e.created_at || "—"}</td>
                    <td style={cellStyle}>{e.created_by || "—"}</td>
                    <td style={cellStyle}>
                      {e.supersedes ? <code>{e.supersedes}</code> : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>
        ))
      )}

      <Panel title="Merged Timeline (raw)">
        <Json data={merged} />
      </Panel>
    </div>
  );
}

const cellStyle = {
  padding: "4px 6px",
  textAlign: "left",
  verticalAlign: "top",
};
