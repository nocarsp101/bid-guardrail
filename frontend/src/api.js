// Canonical backend API surface. The UI consumes these endpoints and
// never recomputes business truth locally.

export const API_BASE =
  import.meta.env.VITE_API_BASE || "http://localhost:8000";

async function getJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GET ${path} failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function postJson(path, body) {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`POST ${path} failed (${res.status}): ${text}`);
  }
  return res.json();
}

// --- UI screen adapters (C104) -------------------------------------------
export const getQuoteCaseScreen = (jobId) =>
  getJson(`/api/ui/quote-case/${encodeURIComponent(jobId)}`);
export const getPackageOverviewScreen = (bidId) =>
  getJson(`/api/ui/package-overview/${encodeURIComponent(bidId)}`);
export const getAuthorityActionScreen = (bidId) =>
  getJson(
    `/api/ui/authority-action${bidId ? `?bid_id=${encodeURIComponent(bidId)}` : ""}`
  );
export const getBidReadinessScreen = (bidId) =>
  getJson(`/api/ui/bid-readiness/${encodeURIComponent(bidId)}`);
export const getTimelineScreen = ({ bidId, jobId } = {}) => {
  const params = new URLSearchParams();
  if (bidId) params.set("bid_id", bidId);
  if (jobId) params.set("job_id", jobId);
  const qs = params.toString();
  return getJson(`/api/ui/timeline${qs ? `?${qs}` : ""}`);
};
export const getRevisionInspection = (body) =>
  postJson(`/api/ui/revision-inspection`, body);

// --- Frontend manifest + integration pack (C98 / C90) --------------------
export const getFrontendManifest = () => getJson(`/api/frontend/manifest`);
export const getBidOverviewBundle = (bidId) =>
  getJson(`/api/frontend/bid-overview/${encodeURIComponent(bidId)}`);
export const getUiIntegrationPack = () => getJson(`/api/ui-integration-pack`);

// --- Seeded demo scenarios -----------------------------------------------
export const listDemoScenarios = () => getJson(`/demo/scenarios`);
export const runDemoScenario = (scenarioId) =>
  postJson(`/demo/run/${encodeURIComponent(scenarioId)}`);
export const runProductDemo = (scenarioId) =>
  postJson(`/api/demo/product-flow`, { scenario_id: scenarioId });
export const runUiDemo = (scenarioId) =>
  postJson(`/api/demo/ui-flow`, { scenario_id: scenarioId });

// --- Repository snapshots ------------------------------------------------
export const getRepositorySummary = () =>
  getJson(`/canonical/repository/summary`);
export const resetRepository = () => postJson(`/canonical/repository/reset`);
export const getArtifactsByBid = (bidId) =>
  getJson(`/canonical/artifacts/by-bid/${encodeURIComponent(bidId)}`);

// --- Operator commands (C106) --------------------------------------------
export const getCommandVocabulary = () => getJson(`/api/commands/vocabulary`);
export const executeCommand = ({ command, payload, issuedBy, issuedAt }) =>
  postJson(`/api/commands/execute`, {
    command,
    payload: payload || {},
    issued_by: issuedBy,
    issued_at: issuedAt,
  });
export const getCommandReceipts = () => getJson(`/api/commands/receipts`);
export const resetCommandReceipts = () =>
  postJson(`/api/commands/receipts/reset`);

// --- Report downloads (C100 + C105) --------------------------------------
export const listDownloadReportKinds = () =>
  getJson(`/api/download/report-kinds`);
export const buildDownloadable = ({
  reportKind,
  bidId,
  jobId,
  revisionSequence,
  format,
}) =>
  postJson(`/api/download/report`, {
    report_kind: reportKind,
    bid_id: bidId,
    job_id: jobId,
    revision_sequence: revisionSequence,
    format: format || "json",
  });
export const buildDownloadBundle = ({ bidId, jobId, format }) =>
  postJson(`/api/download/bundle`, {
    bid_id: bidId,
    job_id: jobId,
    format: format || "json",
  });

// --- Render reports (direct) ---------------------------------------------
export const getReport = (kind, body) =>
  postJson(`/api/reports/${kind}`, body || {});

// --- Admin / diagnostics / acceptance ------------------------------------
export const getDiagnostics = (runSmoke = false) =>
  getJson(`/api/diagnostics?run_smoke_flag=${runSmoke ? "true" : "false"}`);
export const getAuthorizationSummary = () =>
  getJson(`/api/authorization/summary`);
export const getIdempotencySummary = () => getJson(`/api/idempotency/summary`);
export const getSafetySummary = () => getJson(`/api/safety/summary`);
export const getConfigSummary = () => getJson(`/api/config/summary`);
export const bootstrapStart = (body) => postJson(`/api/bootstrap/start`, body);

// --- Runtime packaging (C120) / walkthrough (C121) -----------------------
export const getRuntimeProfile = (mode) =>
  getJson(`/api/runtime/profile${mode ? `?mode=${encodeURIComponent(mode)}` : ""}`);
export const getRuntimeHandoff = (mode) =>
  getJson(
    `/api/runtime/frontend-handoff${mode ? `?mode=${encodeURIComponent(mode)}` : ""}`
  );
export const getStartupVerification = (mode) =>
  getJson(
    `/api/runtime/startup-verification${mode ? `?mode=${encodeURIComponent(mode)}` : ""}`
  );
export const packageRuntime = (body) => postJson(`/api/runtime/package`, body);
export const listWalkthroughScenarios = () =>
  getJson(`/api/acceptance/walkthrough/scenarios`);
export const runAcceptanceWalkthrough = (scenarioId) =>
  postJson(`/api/acceptance/walkthrough`, { scenario_id: scenarioId });
