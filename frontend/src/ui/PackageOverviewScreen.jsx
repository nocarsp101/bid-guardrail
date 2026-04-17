import React from "react";
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
import { LoadingState, ErrorState, EmptyState } from "./states.jsx";
import { Breadcrumbs, useNav } from "./navContext.jsx";
import { getPackageOverviewScreen } from "../api.js";

// C111 + C116/C117 — Package Overview + Vendor Comparison Screen
export default function PackageOverviewScreen() {
  const nav = useNav();
  const bidId = nav.bidId;
  const { loading, data, error, refresh, lastFetchedAt } = useAsync(
    () => getPackageOverviewScreen(bidId),
    [bidId],
    { refreshPulse: nav.refreshPulse }
  );

  const crumbs = [
    { label: "Demo", onClick: () => nav.navigateTo({ screen: "demo" }) },
    { label: `Package ${bidId || "(none)"}` },
  ];

  if (!bidId) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <EmptyState
          title="Package Overview"
          message="No bid_id selected."
          hint="Run a seeded scenario in the Demo Harness, then return here."
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
        <LoadingState title={`Package Overview — ${bidId}`} />
      </>
    );
  }
  if (error) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <ErrorState
          title="Package Overview failed to load"
          error={error}
          onRetry={refresh}
        />
      </>
    );
  }

  const body = data?.body || {};
  const gate = body.package_gate || {};
  const summaries = body.quote_summaries || [];
  const vc = body.vendor_comparison || {};

  return (
    <div>
      <Breadcrumbs trail={crumbs} />
      <Panel
        title={`Package Overview — ${bidId}`}
        subtitle={`${summaries.length} quote(s) • fetched ${lastFetchedAt || "—"}`}
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
                nav.navigateTo({ screen: "authority_action", bidId })
              }
              kind="secondary"
            >
              → Authority Action
            </Button>
            <Button
              onClick={() =>
                nav.navigateTo({ screen: "bid_readiness", bidId })
              }
              kind="secondary"
            >
              → Bid Readiness
            </Button>
            <Button
              onClick={() => nav.navigateTo({ screen: "timeline", bidId })}
              kind="secondary"
            >
              → Timeline
            </Button>
          </div>
        }
      >
        <IdentityBar entries={[["bid_id", bidId]]} />
        <StateLabelBadges labels={data?.state_labels} />
      </Panel>

      <Panel title="Package Summary">
        {Object.keys(body.package_summary || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No package summary fields available.
          </div>
        ) : (
          <KeyValueGrid
            entries={Object.entries(body.package_summary || {}).map(([k, v]) => [
              k,
              v,
            ])}
          />
        )}
      </Panel>

      <Panel title="Package Gate">
        {Object.keys(gate).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No package gate recorded yet.
          </div>
        ) : (
          <KeyValueGrid entries={Object.entries(gate).map(([k, v]) => [k, v])} />
        )}
      </Panel>

      <Panel
        title="Quote Distributions / Summaries"
        subtitle={summaries.length === 0 ? "no quotes linked" : undefined}
      >
        {summaries.length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No quote summaries — seed a scenario or attach dossiers first.
          </div>
        ) : (
          <table
            style={{
              width: "100%",
              fontSize: 13,
              borderCollapse: "collapse",
            }}
          >
            <thead>
              <tr style={{ background: "#f3f4f6" }}>
                <th style={cellStyle}>job_id</th>
                <th style={cellStyle}>vendor</th>
                <th style={cellStyle}>gate</th>
                <th style={cellStyle}>risk</th>
                <th style={cellStyle}>posture</th>
                <th style={cellStyle}></th>
              </tr>
            </thead>
            <tbody>
              {summaries.map((q, i) => (
                <tr key={i} style={{ borderBottom: "1px solid #e5e7eb" }}>
                  <td style={cellStyle}>
                    <code>{q.job_id}</code>
                  </td>
                  <td style={cellStyle}>{q.vendor_name || q.vendor || "—"}</td>
                  <td style={cellStyle}>{q.gate_outcome || "—"}</td>
                  <td style={cellStyle}>{q.risk_level || "—"}</td>
                  <td style={cellStyle}>{q.decision_posture || "—"}</td>
                  <td style={cellStyle}>
                    {q.job_id && (
                      <Button
                        onClick={() =>
                          nav.navigateTo({
                            screen: "quote_case",
                            jobId: q.job_id,
                            bidId,
                          })
                        }
                        kind="secondary"
                      >
                        Open
                      </Button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Panel>

      <Panel title="Vendor Comparison Summary">
        {Object.keys(vc).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No vendor comparison stored.
          </div>
        ) : (
          <Json data={vc} />
        )}
      </Panel>

      <Panel title="Source Refs">
        <SourceRefsList refs={data?.source_refs} />
      </Panel>
    </div>
  );
}

const cellStyle = {
  padding: "6px 8px",
  borderBottom: "1px solid #e5e7eb",
  textAlign: "left",
};
