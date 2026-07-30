"use strict";

const publicActions = {
  candidate: null,
  retryWindows: [],
  jobs: new Map(),
  pollTimer: null,
};

const byId = (id) => document.getElementById(id);
const TERMINAL_JOB_STATUSES = new Set([
  "succeeded",
  "partial",
  "failed",
  "interrupted",
]);
const SESSION_JOB_KEY = "market-monitor:public-action-jobs:v1";

function showGlobalError(error) {
  const target = byId("actions-global-error");
  target.textContent = error.message || String(error);
  target.hidden = false;
}

function clearGlobalError() {
  byId("actions-global-error").hidden = true;
}

function showStatus(id, message, tone = "") {
  const target = byId(id);
  target.textContent = message;
  target.dataset.tone = tone;
  target.hidden = false;
}

async function request(path, options = {}) {
  const headers = { Accept: "application/json", ...(options.headers || {}) };
  if (options.body) headers["Content-Type"] = "application/json";
  const response = await fetch(path, {
    ...options,
    headers,
    credentials: "same-origin",
  });
  let payload = {};
  try {
    payload = await response.json();
  } catch {
    payload = {};
  }
  if (!response.ok) {
    const error = new Error(payload.error || `Request failed (${response.status})`);
    error.code = payload.error_code || "";
    error.retryable = payload.retryable === true;
    error.status = response.status;
    throw error;
  }
  return payload;
}

function text(id, value) {
  byId(id).textContent = value ?? "—";
}

function renderTokenCandidate(candidate) {
  publicActions.candidate = candidate;
  const identity = candidate.identity || {};
  const discovery = candidate.discovery || {};
  const capabilities = candidate.capabilities || {};
  const pools = discovery.top_pools || [];
  text(
    "public-token-identity",
    `${identity.token_symbol || "Unknown"} · ${identity.token_name || "Unnamed"}`,
  );
  text("public-token-address", identity.contract_address);
  text(
    "public-token-pools",
    `${discovery.usable_pool_count || 0} usable pool${discovery.usable_pool_count === 1 ? "" : "s"}`,
  );
  text(
    "public-token-top-pool",
    pools[0]?.pool_name || pools[0]?.dex || "No pool selected",
  );
  text(
    "public-token-cex",
    candidate.already_configured
      ? "Existing catalog record"
      : capabilities.cex === "available"
        ? "Available"
        : "Manual mapping required",
  );
  text(
    "public-token-cex-note",
    candidate.already_configured
      ? "Open Token Research for its current venue mappings"
      : "Never inferred from a contract",
  );
  byId("public-token-preview").hidden = false;
  byId("public-token-add").disabled = candidate.already_configured === true;
  showStatus(
    "public-token-status",
    candidate.already_configured
      ? "This contract is already configured."
      : "Identity verified. Review it, then start the fixed 30-day collection.",
    candidate.already_configured ? "warning" : "success",
  );
}

function retryWindowReason(window) {
  return (window.reason_codes || []).join(", ") || "missing_unexplained";
}

function appendCell(row, value, className = "") {
  const cell = document.createElement("td");
  cell.textContent = value;
  if (className) cell.className = className;
  row.append(cell);
  return cell;
}

function renderRetryWindows() {
  const body = byId("public-retry-body");
  body.replaceChildren();
  byId("public-retry-count").textContent = `${publicActions.retryWindows.length} windows`;
  if (!publicActions.retryWindows.length) {
    const row = document.createElement("tr");
    const cell = appendCell(row, "No audited retry windows");
    cell.colSpan = 5;
    cell.className = "actions-empty";
    body.append(row);
    return;
  }
  publicActions.retryWindows.forEach((window, index) => {
    const row = document.createElement("tr");
    appendCell(row, window.token_symbol);
    appendCell(row, `${window.start_date} → ${window.end_date}`);
    appendCell(row, (window.market_types || []).map((item) => item.toUpperCase()).join(" + "));
    appendCell(row, retryWindowReason(window), "public-retry-reasons");
    const actionCell = document.createElement("td");
    const button = document.createElement("button");
    button.className = "action-secondary";
    button.type = "button";
    button.dataset.retryIndex = String(index);
    button.textContent = "Retry exact window";
    actionCell.append(button);
    row.append(actionCell);
    body.append(row);
  });
}

async function loadRetryWindows() {
  const button = byId("public-retry-refresh");
  button.disabled = true;
  try {
    const payload = await request("/api/actions/quality/retryable");
    publicActions.retryWindows = Array.isArray(payload.windows) ? payload.windows : [];
    renderRetryWindows();
    showStatus(
      "public-retry-status",
      publicActions.retryWindows.length
        ? "Only the exact windows below can be submitted."
        : "The current audit has no retryable missing-fact windows.",
      publicActions.retryWindows.length ? "" : "success",
    );
  } catch (error) {
    publicActions.retryWindows = [];
    renderRetryWindows();
    showStatus(
      "public-retry-status",
      error.status === 404
        ? "Public recovery is currently unavailable."
        : (error.message || "Could not load the quality queue."),
      "error",
    );
  } finally {
    button.disabled = false;
  }
}

function loadStoredJobIds() {
  try {
    const values = JSON.parse(sessionStorage.getItem(SESSION_JOB_KEY) || "[]");
    return Array.isArray(values)
      ? values
          .filter((value) => (
            typeof value === "string"
            && /^[0-9a-f]{32}$/.test(value)
          ))
          .slice(-10)
      : [];
  } catch {
    return [];
  }
}

function storeJobIds() {
  try {
    sessionStorage.setItem(
      SESSION_JOB_KEY,
      JSON.stringify([...publicActions.jobs.keys()].slice(-10)),
    );
  } catch {
    // Status tracking remains available in-memory when storage is blocked.
  }
}

function jobScope(job) {
  if (job.job_type === "token_onboarding") {
    return `${job.chain || "chain"} · ${job.contract_address || job.token_symbol || "Token"}`;
  }
  return `${job.token_symbol || "Token"} · ${job.start_date || "?"} → ${job.end_date || "?"}`;
}

function renderJobs() {
  const panel = byId("public-job-panel");
  const target = byId("public-jobs");
  const jobs = [...publicActions.jobs.values()];
  panel.hidden = !jobs.length;
  byId("public-job-count").textContent = `${jobs.length} job${jobs.length === 1 ? "" : "s"}`;
  target.replaceChildren();
  jobs.forEach((job) => {
    const item = document.createElement("div");
    item.className = "public-job";

    const identity = document.createElement("div");
    const type = document.createElement("strong");
    type.textContent = job.job_type === "token_onboarding" ? "Add Token" : "Fact recovery";
    const id = document.createElement("small");
    id.textContent = job.job_id;
    identity.append(type, id);

    const status = document.createElement("span");
    status.className = "public-job-status";
    status.dataset.status = job.status || "unknown";
    status.textContent = job.status || "unknown";

    const detail = document.createElement("div");
    const scope = document.createElement("strong");
    scope.textContent = jobScope(job);
    const stage = document.createElement("span");
    stage.textContent = job.stage || (
      job.publication_committed ? "Published" : "Waiting for worker"
    );
    detail.append(scope, stage);

    item.append(identity, status, detail);
    target.append(item);
  });
}

function rememberJob(job) {
  if (!job?.job_id) return;
  publicActions.jobs.set(job.job_id, job);
  storeJobIds();
  renderJobs();
  scheduleJobPoll(800);
}

async function refreshJobs() {
  const jobIds = [...publicActions.jobs.entries()]
    .filter(([, job]) => (
      !TERMINAL_JOB_STATUSES.has(job.status)
      && job.polling_stopped !== true
    ))
    .map(([jobId]) => jobId);
  if (!jobIds.length) return;
  let completedDuringPoll = false;
  await Promise.all(jobIds.map(async (jobId) => {
    const previous = publicActions.jobs.get(jobId) || { job_id: jobId };
    try {
      const job = await request(`/api/actions/jobs/${encodeURIComponent(jobId)}`);
      publicActions.jobs.set(jobId, job);
      if (
        !TERMINAL_JOB_STATUSES.has(previous.status)
        && TERMINAL_JOB_STATUSES.has(job.status)
      ) {
        completedDuringPoll = true;
      }
    } catch (error) {
      publicActions.jobs.set(jobId, {
        ...previous,
        polling_stopped: true,
        stage: error.status === 404
          ? "Status is no longer available"
          : "Automatic status checks stopped; reload to retry",
      });
    }
  }));
  renderJobs();
  if (completedDuringPoll) loadRetryWindows().catch(showGlobalError);
  const hasActive = [...publicActions.jobs.values()].some(
    (job) => (
      !TERMINAL_JOB_STATUSES.has(job.status)
      && job.polling_stopped !== true
    ),
  );
  if (hasActive) scheduleJobPoll(5000);
}

function scheduleJobPoll(delay) {
  if (publicActions.pollTimer) window.clearTimeout(publicActions.pollTimer);
  publicActions.pollTimer = window.setTimeout(() => {
    publicActions.pollTimer = null;
    refreshJobs().catch(showGlobalError);
  }, delay);
}

byId("public-token-resolve-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  clearGlobalError();
  publicActions.candidate = null;
  byId("public-token-preview").hidden = true;
  const button = byId("public-token-resolve");
  button.disabled = true;
  try {
    const candidate = await request("/api/actions/tokens/resolve", {
      method: "POST",
      body: JSON.stringify({
        chain: byId("public-token-chain").value,
        contract_address: byId("public-token-contract").value.trim(),
      }),
    });
    renderTokenCandidate(candidate);
  } catch (error) {
    showStatus("public-token-status", error.message, "error");
  } finally {
    button.disabled = false;
  }
});

byId("public-token-add").addEventListener("click", async () => {
  const identity = publicActions.candidate?.identity;
  if (!identity) return;
  clearGlobalError();
  const button = byId("public-token-add");
  button.disabled = true;
  try {
    const job = await request("/api/actions/tokens", {
      method: "POST",
      body: JSON.stringify({
        chain: identity.chain,
        contract_address: identity.contract_address,
        expected_token_symbol: identity.token_symbol,
      }),
    });
    rememberJob(job);
    showStatus(
      "public-token-status",
      job.status === "succeeded"
        ? "Token collection completed."
        : "Token collection accepted. Progress appears below.",
      job.status === "succeeded" ? "success" : "",
    );
  } catch (error) {
    showStatus("public-token-status", error.message, "error");
    button.disabled = false;
  }
});

byId("public-retry-body").addEventListener("click", async (event) => {
  const button = event.target.closest("[data-retry-index]");
  if (!button) return;
  const window = publicActions.retryWindows[Number(button.dataset.retryIndex)];
  if (!window) return;
  clearGlobalError();
  button.disabled = true;
  try {
    const job = await request("/api/actions/quality/retry", {
      method: "POST",
      body: JSON.stringify({
        token_symbol: window.token_symbol,
        start_date: window.start_date,
        end_date: window.end_date,
        queue_type: window.queue_type,
      }),
    });
    rememberJob(job);
    showStatus(
      "public-retry-status",
      "Exact retry accepted. Progress appears below.",
      "",
    );
  } catch (error) {
    showStatus("public-retry-status", error.message, "error");
    button.disabled = false;
  }
});

byId("public-retry-refresh").addEventListener("click", () => {
  clearGlobalError();
  loadRetryWindows().catch(showGlobalError);
});

function initializePublicActions() {
  loadStoredJobIds().forEach((jobId) => {
    publicActions.jobs.set(jobId, {
      job_id: jobId,
      status: "unknown",
      stage: "Checking status",
    });
  });
  renderJobs();
  if (publicActions.jobs.size) scheduleJobPoll(100);
  loadRetryWindows().catch(showGlobalError);
  if (globalThis.lucide) globalThis.lucide.createIcons();
}

initializePublicActions();
