import React from "react";
import { TabBar, Panel, Button } from "./ui/common.jsx";
import { NavProvider, useNav } from "./ui/navContext.jsx";
import QuoteCaseScreen from "./ui/QuoteCaseScreen.jsx";
import PackageOverviewScreen from "./ui/PackageOverviewScreen.jsx";
import AuthorityActionScreen from "./ui/AuthorityActionScreen.jsx";
import BidReadinessScreen from "./ui/BidReadinessScreen.jsx";
import TimelineScreen from "./ui/TimelineScreen.jsx";
import RevisionInspectionScreen from "./ui/RevisionInspectionScreen.jsx";
import DemoHarnessScreen from "./ui/DemoHarnessScreen.jsx";
import LegacyValidatorApp from "./LegacyValidatorApp.jsx";

const TABS = [
  { id: "demo", label: "Demo Harness" },
  { id: "package_overview", label: "Package Overview" },
  { id: "quote_case", label: "Quote Case" },
  { id: "authority_action", label: "Authority Action" },
  { id: "bid_readiness", label: "Bid Readiness" },
  { id: "timeline", label: "Timeline" },
  { id: "revision_inspection", label: "Diff" },
  { id: "legacy", label: "Legacy Validator" },
];

const ARTIFACT_TYPES = [
  "bid_readiness_snapshot",
  "bid_carry_justification",
  "package_overview",
  "quote_dossier",
  "authority_action_packet",
  "priority_queue",
  "authority_posture",
];

function HeaderIdentityBar() {
  const nav = useNav();
  return (
    <div
      style={{
        display: "flex",
        flexWrap: "wrap",
        gap: 10,
        alignItems: "center",
      }}
    >
      <label style={{ fontSize: 13 }}>
        bid_id:&nbsp;
        <input
          value={nav.bidId}
          onChange={(e) => nav.setField("bidId", e.target.value)}
          placeholder="e.g. seed-caveats"
          style={{
            padding: 6,
            borderRadius: 6,
            border: "1px solid #ccc",
            width: 180,
          }}
        />
      </label>
      <label style={{ fontSize: 13 }}>
        job_id:&nbsp;
        <input
          value={nav.jobId}
          onChange={(e) => nav.setField("jobId", e.target.value)}
          placeholder="e.g. pc-j1"
          style={{
            padding: 6,
            borderRadius: 6,
            border: "1px solid #ccc",
            width: 160,
          }}
        />
      </label>
      <label style={{ fontSize: 13 }}>
        artifact_type:&nbsp;
        <select
          value={nav.artifactType}
          onChange={(e) => nav.setField("artifactType", e.target.value)}
          style={{ padding: 6, borderRadius: 6, border: "1px solid #ccc" }}
        >
          {ARTIFACT_TYPES.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
        </select>
      </label>
      <label style={{ fontSize: 13 }}>
        operator:&nbsp;
        <input
          value={nav.operator}
          onChange={(e) => nav.setField("operator", e.target.value)}
          style={{
            padding: 6,
            borderRadius: 6,
            border: "1px solid #ccc",
            width: 140,
          }}
        />
      </label>
      <Button onClick={nav.triggerRefresh} kind="secondary">
        ↻ Global refresh
      </Button>
    </div>
  );
}

function AppShell() {
  const nav = useNav();
  return (
    <div
      style={{
        maxWidth: 1280,
        margin: "24px auto",
        padding: "0 16px",
        fontFamily: "system-ui, Arial",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 10,
        }}
      >
        <div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>Bid Guardrail</div>
          <div style={{ fontSize: 13, opacity: 0.7 }}>
            Deterministic, append-only. UI consumes canonical backend truth —
            never recomputes it.
          </div>
        </div>
      </div>
      <Panel title="Identity / Operator">
        <HeaderIdentityBar />
      </Panel>

      <TabBar
        tabs={TABS}
        active={nav.active}
        onSelect={(id) => nav.setField("active", id)}
      />

      {nav.active === "demo" && <DemoHarnessScreen />}
      {nav.active === "package_overview" && <PackageOverviewScreen />}
      {nav.active === "quote_case" && <QuoteCaseScreen />}
      {nav.active === "authority_action" && <AuthorityActionScreen />}
      {nav.active === "bid_readiness" && <BidReadinessScreen />}
      {nav.active === "timeline" && <TimelineScreen />}
      {nav.active === "revision_inspection" && <RevisionInspectionScreen />}
      {nav.active === "legacy" && <LegacyValidatorApp />}
    </div>
  );
}

export default function App() {
  return (
    <NavProvider>
      <AppShell />
    </NavProvider>
  );
}
