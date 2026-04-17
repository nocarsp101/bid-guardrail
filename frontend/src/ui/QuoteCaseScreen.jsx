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
import { getQuoteCaseScreen } from "../api.js";

// C110 + C116/C117 — Quote Case Screen.
export default function QuoteCaseScreen() {
  const nav = useNav();
  const jobId = nav.jobId;
  const { loading, data, error, refresh, lastFetchedAt } = useAsync(
    () => getQuoteCaseScreen(jobId),
    [jobId],
    { refreshPulse: nav.refreshPulse }
  );

  const crumbs = [
    { label: "Demo", onClick: () => nav.navigateTo({ screen: "demo" }) },
    nav.bidId
      ? {
          label: `Package ${nav.bidId}`,
          onClick: () =>
            nav.navigateTo({ screen: "package_overview", bidId: nav.bidId }),
        }
      : null,
    { label: `Quote ${jobId || "(none)"}` },
  ].filter(Boolean);

  if (!jobId) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <EmptyState
          title="Quote Case"
          message="No job_id selected. Pick a quote from the package overview or demo harness."
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
        <LoadingState title={`Quote Case — ${jobId}`} />
      </>
    );
  }
  if (error) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <ErrorState
          title="Quote Case failed to load"
          error={error}
          onRetry={refresh}
        />
      </>
    );
  }

  const body = data?.body || {};
  const view = body.view_model || {};
  const record = body.dossier_record_ref || {};
  const present = data?.diagnostics?.dossier_present;
  const bidId = data?.identity?.bid_id || nav.bidId;

  return (
    <div>
      <Breadcrumbs trail={crumbs} />
      <Panel
        title={`Quote Case — ${data?.identity?.vendor_name || "Unknown Vendor"}`}
        subtitle={`last fetched ${lastFetchedAt || "—"}`}
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
                  nav.navigateTo({ screen: "package_overview", bidId })
                }
                kind="secondary"
              >
                → Package Overview
              </Button>
            )}
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
                nav.navigateTo({ screen: "timeline", jobId, bidId })
              }
              kind="secondary"
            >
              → Timeline
            </Button>
            <Button
              onClick={() =>
                nav.navigateTo({
                  screen: "revision_inspection",
                  artifactType: "quote_dossier",
                  jobId,
                })
              }
              kind="secondary"
            >
              → Diff
            </Button>
          </div>
        }
      >
        <IdentityBar
          entries={[
            ["job_id", jobId],
            ["bid_id", bidId],
            ["vendor_name", data?.identity?.vendor_name],
            ["record_id", record.record_id],
            ["revision_sequence", record.revision_sequence],
          ]}
        />
        <StateLabelBadges labels={data?.state_labels} />
        {!present && (
          <div
            style={{
              marginTop: 8,
              padding: 8,
              background: "#fffbeb",
              color: "#92400e",
              borderRadius: 8,
              fontSize: 13,
            }}
          >
            No dossier record found for job_id <code>{jobId}</code>. Visit the
            demo harness to seed a scenario or open a different job.
          </div>
        )}
      </Panel>

      <Panel title="Gate / Risk / Readiness">
        <KeyValueGrid
          entries={[
            ["gate_outcome", data?.state_labels?.gate_outcome],
            ["risk_level", data?.state_labels?.risk_level],
            ["decision_posture", data?.state_labels?.decision_posture],
            ["readiness_status", data?.state_labels?.readiness_status],
          ]}
        />
      </Panel>

      <Panel title="Open Clarifications">
        {Object.keys(body.clarifications || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No clarification counts on this dossier.
          </div>
        ) : (
          <KeyValueGrid
            entries={Object.entries(body.clarifications || {}).map(
              ([k, v]) => [k, v]
            )}
          />
        )}
      </Panel>

      <Panel title="Response History Summary">
        {Object.keys(view.response_history_summary || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No response history recorded.
          </div>
        ) : (
          <KeyValueGrid
            entries={Object.entries(
              view.response_history_summary || {}
            ).map(([k, v]) => [k, v])}
          />
        )}
      </Panel>

      <Panel title="Comparability Posture">
        {Object.keys(view.comparability_posture || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No comparability data.
          </div>
        ) : (
          <KeyValueGrid
            entries={Object.entries(view.comparability_posture || {}).map(
              ([k, v]) => [k, v]
            )}
          />
        )}
      </Panel>

      <Panel title="Scope Gaps">
        {Object.keys(body.scope_gaps || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>No scope gaps.</div>
        ) : (
          <KeyValueGrid
            entries={Object.entries(body.scope_gaps || {}).map(([k, v]) => [
              k,
              v,
            ])}
          />
        )}
      </Panel>

      <Panel title="Evidence Status">
        {Object.keys(view.evidence_status || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No evidence status recorded.
          </div>
        ) : (
          <KeyValueGrid
            entries={Object.entries(view.evidence_status || {}).map(
              ([k, v]) => [k, v]
            )}
          />
        )}
      </Panel>

      <Panel title="Reliance Posture">
        {Object.keys(body.reliance_posture || {}).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No reliance posture on this dossier.
          </div>
        ) : (
          <KeyValueGrid
            entries={Object.entries(body.reliance_posture || {}).map(
              ([k, v]) => [k, v]
            )}
          />
        )}
      </Panel>

      <Panel title="View Model (canonical backend payload)">
        <Json data={view} />
      </Panel>

      <Panel title="Source Refs">
        <SourceRefsList refs={data?.source_refs} />
      </Panel>
    </div>
  );
}
