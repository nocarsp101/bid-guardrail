import React from "react";
import {
  Panel,
  StateLabelBadges,
  SourceRefsList,
  Button,
  useAsync,
  Json,
  IdentityBar,
} from "./common.jsx";
import { LoadingState, ErrorState, EmptyState } from "./states.jsx";
import { Breadcrumbs, useNav } from "./navContext.jsx";
import { getAuthorityActionScreen } from "../api.js";

// C112 + C116/C117 — Authority Action Screen
export default function AuthorityActionScreen() {
  const nav = useNav();
  const bidId = nav.bidId;
  const { loading, data, error, refresh, lastFetchedAt } = useAsync(
    () => getAuthorityActionScreen(bidId),
    [bidId],
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
    { label: "Authority Action" },
  ].filter(Boolean);

  if (loading) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <LoadingState title="Authority Action" />
      </>
    );
  }
  if (error) {
    return (
      <>
        <Breadcrumbs trail={crumbs} />
        <ErrorState
          title="Authority Action failed"
          error={error}
          onRetry={refresh}
        />
      </>
    );
  }

  const body = data?.body || {};
  const actions = body.action_items || body.top_priority_actions || [];
  const implications = body.implication_groups || [];
  const reference = body.authority_reference || {};
  const present = data?.diagnostics?.authority_action_present;

  return (
    <div>
      <Breadcrumbs trail={crumbs} />
      <Panel
        title="Authority Action"
        subtitle={`${bidId ? `bid_id=${bidId}` : "all bids"} • fetched ${
          lastFetchedAt || "—"
        }`}
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
          </div>
        }
      >
        {bidId && <IdentityBar entries={[["bid_id", bidId]]} />}
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
            No authority action packet on file yet.
          </div>
        )}
      </Panel>

      <Panel title="Top Priority Actions">
        {actions.length === 0 ? (
          <EmptyState
            title="No authority-backed actions"
            message="Nothing to act on here."
          />
        ) : (
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
            {actions.map((a, i) => (
              <li key={i} style={{ marginBottom: 6 }}>
                <code>{a.authority_topic_id || a.topic_id || a.id || i}</code>{" "}
                <b>{a.handling_implication || a.action_type || "—"}</b>
                <div style={{ opacity: 0.8 }}>
                  {a.authority_description || a.description || ""}
                </div>
                {a.authority_source_type && (
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    source: {a.authority_source_type} /{" "}
                    {a.authority_posture || "—"}
                  </div>
                )}
              </li>
            ))}
          </ul>
        )}
      </Panel>

      <Panel title="Implication Groups">
        {implications.length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No implication groupings provided.
          </div>
        ) : (
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
            {implications.map((g, i) => (
              <li key={i}>
                <b>{g.handling_implication || g.implication || "—"}</b>
                <span style={{ opacity: 0.7 }}>
                  {" "}
                  count={g.count ?? g.action_count ?? "—"}
                </span>
              </li>
            ))}
          </ul>
        )}
      </Panel>

      <Panel title="Authority Reference (raw)">
        {Object.keys(reference).length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.6 }}>
            No reference artifact.
          </div>
        ) : (
          <Json data={reference} />
        )}
      </Panel>

      <Panel title="Source Refs">
        <SourceRefsList refs={data?.source_refs} />
      </Panel>
    </div>
  );
}
