const admin = {
  session: null,
  pollTimer: null,
  tokenCandidate: null,
  retryWindows: [],
  manualReviewItems: [],
};

const byId = (id) => document.getElementById(id);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function showAdminError(error) {
  byId("admin-error").hidden = false;
  byId("admin-error").textContent = error.message || String(error);
}

function clearAdminError() {
  byId("admin-error").hidden = true;
}

async function request(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (options.body) headers["Content-Type"] = "application/json";
  if (admin.session?.csrf_token && options.method && options.method !== "GET") {
    headers["X-CSRF-Token"] = admin.session.csrf_token;
  }
  const response = await fetch(path, { ...options, headers });
  const payload = await response.json();
  if (!response.ok) {
    const error = new Error(payload.error || "Administrator request failed");
    error.code = payload.error_code || "";
    error.retryable = payload.retryable === true;
    throw error;
  }
  return payload;
}

function setDefaultDates() {
  const end = new Date();
  end.setUTCDate(end.getUTCDate() - 1);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - 29);
  byId("refresh-end").value = end.toISOString().slice(0, 10);
  byId("refresh-start").value = start.toISOString().slice(0, 10);
  byId("refresh-end").max = end.toISOString().slice(0, 10);
}

async function loadTokens() {
  const payload = await request("/api/admin/tokens");
  byId("refresh-token").innerHTML = payload.tokens
    .map((token) => `<option value="${escapeHtml(token)}">${escapeHtml(token)}</option>`)
    .join("");
}

function retryWindowLabel(window) {
  const reason = (window.reason_codes || []).join(", ") || "missing observation";
  const queueLabel = window.queue_type === "historical_gap"
    ? "Historical backfill"
    : "Recent D-1 retry";
  return `${window.token_symbol} · ${window.start_date} → ${window.end_date}`
    + ` · ${queueLabel} · ${reason}`;
}

async function loadRetryWindows() {
  const payload = await request("/api/admin/quality/retryable");
  admin.retryWindows = payload.windows || [];
  byId("retry-window-count").textContent = `${admin.retryWindows.length} windows`;
  byId("retry-window").innerHTML = admin.retryWindows.length
    ? admin.retryWindows
        .map((window, index) => (
          `<option value="${index}">${escapeHtml(retryWindowLabel(window))}</option>`
        ))
        .join("")
    : '<option value="">No audited retry windows</option>';
  byId("retry-button").disabled = !admin.retryWindows.length;
}

function renderManualReviewItems(items) {
  byId("manual-review-count").textContent = `${items.length} findings`;
  byId("manual-review-body").innerHTML = items.length
    ? items.map((item) => {
        const categoryLabel = item.category === "hard_invalid"
          ? "Hard invalid value"
          : item.category === "stale_market_unknown"
          ? "Unknown market lifecycle"
          : item.category;
        const sources = (item.source_url_hints || []).length
          ? item.source_url_hints.map((url, index) => (
              `<a class="review-source-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">`
              + `Source ${index + 1}</a>`
            )).join(" ")
          : '<span class="missing">No source hint</span>';
        const reason = item.reason_message
          ? `${item.reason_code}: ${item.reason_message}`
          : item.reason_code;
        return `<tr>
          <td>${escapeHtml(item.date)}</td>
          <td><strong>${escapeHtml(item.token_symbol)}</strong></td>
          <td class="manual-review-market">${escapeHtml(item.market_id)}</td>
          <td><strong>${escapeHtml(categoryLabel)}</strong><br><span>${escapeHtml(reason)}</span></td>
          <td>${sources}</td>
          <td><span class="manual-only-tag">Manual primary-source check</span></td>
        </tr>`;
      }).join("")
    : '<tr><td colspan="6" class="empty-jobs">No manual-review findings</td></tr>';
}

async function loadManualReviews() {
  const payload = await request("/api/admin/quality/manual-review");
  admin.manualReviewItems = payload.review_items || [];
  renderManualReviewItems(admin.manualReviewItems);
}

function statusClass(status) {
  return ["succeeded", "partial", "failed", "running", "queued", "interrupted"].includes(status)
    ? status
    : "unknown";
}

function renderJobs(jobs) {
  byId("jobs-summary").textContent = `${jobs.length} recent jobs`;
  byId("jobs-body").innerHTML = jobs.length
    ? jobs.map((job) => `<tr>
        <td>${escapeHtml(new Date(job.created_at).toLocaleString())}</td>
        <td>${escapeHtml(job.job_type || "refresh")}</td>
        <td><strong>${escapeHtml(job.token_symbol)}</strong></td>
        <td>${escapeHtml(job.start_date)} → ${escapeHtml(job.end_date)}</td>
        <td><span class="job-status ${statusClass(job.status)}">${escapeHtml(job.status)}</span></td>
        <td>${escapeHtml(job.stage || "--")}</td>
        <td>${job.finished_at ? escapeHtml(new Date(job.finished_at).toLocaleString()) : "--"}</td>
        <td>${escapeHtml(job.error_code ? `${job.error_code}: ${job.error || ""}` : job.error || "--")}</td>
      </tr>`).join("")
    : '<tr><td colspan="8" class="empty-jobs">No refresh jobs</td></tr>';
}

async function loadJobs() {
  const payload = await request("/api/admin/jobs");
  renderJobs(payload.jobs);
}

function startPolling() {
  if (admin.pollTimer) window.clearInterval(admin.pollTimer);
  admin.pollTimer = window.setInterval(
    () => Promise.all([
      loadJobs(),
      loadRetryWindows(),
      loadManualReviews(),
    ]).catch(showAdminError),
    5000,
  );
}

async function showWorkspace(session) {
  admin.session = session;
  byId("login-view").hidden = true;
  byId("admin-view").hidden = false;
  byId("session-user").textContent = session.login_required === false ? "Open access" : session.username;
  byId("logout-button").hidden = session.login_required === false;
  setDefaultDates();
  await Promise.all([
    loadTokens(),
    loadJobs(),
    loadRetryWindows(),
    loadManualReviews(),
  ]);
  startPolling();
}

async function initializeAdmin() {
  const session = await request("/api/admin/session");
  if (!session.configured) {
    byId("login-status").textContent = "ADMIN_PASSWORD_HASH is not configured";
    byId("login-button").disabled = true;
    return;
  }
  byId("login-status").textContent = session.login_required === false
    ? "Administrator login disabled"
    : "Server-side authentication enabled";
  if (session.authenticated) await showWorkspace(session);
}

byId("login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  clearAdminError();
  byId("login-button").disabled = true;
  try {
    const session = await request("/api/admin/login", {
      method: "POST",
      body: JSON.stringify({
        username: byId("admin-username").value,
        password: byId("admin-password").value,
      }),
    });
    byId("admin-password").value = "";
    await showWorkspace(session);
  } catch (error) {
    showAdminError(error);
  } finally {
    byId("login-button").disabled = false;
  }
});

byId("refresh-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  clearAdminError();
  byId("refresh-button").disabled = true;
  try {
    await request("/api/admin/jobs", {
      method: "POST",
      body: JSON.stringify({
        job_type: "refresh",
        token_symbol: byId("refresh-token").value,
        start_date: byId("refresh-start").value,
        end_date: byId("refresh-end").value,
      }),
    });
    await loadJobs();
  } catch (error) {
    showAdminError(error);
  } finally {
    byId("refresh-button").disabled = false;
  }
});

byId("token-resolve-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  clearAdminError();
  admin.tokenCandidate = null;
  byId("token-preview").hidden = true;
  byId("token-onboarding-status").hidden = true;
  byId("resolve-token-button").disabled = true;
  try {
    const candidate = await request("/api/admin/tokens/resolve", {
      method: "POST",
      body: JSON.stringify({
        chain: byId("token-chain").value,
        contract_address: byId("token-contract").value.trim(),
      }),
    });
    admin.tokenCandidate = candidate;
    const identity = candidate.identity;
    const pools = candidate.discovery?.top_pools || [];
    byId("token-preview-identity").textContent = `${identity.token_symbol} · ${identity.token_name}`;
    byId("token-preview-address").textContent = `${identity.chain}:${identity.contract_address}`;
    byId("token-preview-pools").textContent = `${candidate.discovery?.usable_pool_count || 0} validated`;
    byId("token-preview-top-pool").textContent = pools.length
      ? `${pools[0].dex} · ${pools[0].pool_name}`
      : "No usable pool";
    byId("token-preview").hidden = false;
    byId("add-token-button").disabled = false;
    byId("add-token-button").querySelector("span").textContent = candidate.already_configured
      ? "Confirm existing Token"
      : "Add & collect";
    byId("token-onboarding-status").textContent = candidate.already_configured
      ? `Already configured (${candidate.registration.origin}, ${candidate.registration.status}). Confirmation is idempotent.`
      : "Identity and pool membership validated. No CEX pair has been inferred.";
    byId("token-onboarding-status").hidden = false;
  } catch (error) {
    showAdminError(error);
  } finally {
    byId("resolve-token-button").disabled = false;
  }
});

byId("add-token-button").addEventListener("click", async () => {
  if (!admin.tokenCandidate) return;
  clearAdminError();
  byId("add-token-button").disabled = true;
  try {
    const identity = admin.tokenCandidate.identity;
    const job = await request("/api/admin/tokens", {
      method: "POST",
      body: JSON.stringify({
        chain: identity.chain,
        contract_address: identity.contract_address,
        expected_token_symbol: identity.token_symbol,
        history_days: Number(byId("token-history-days").value),
      }),
    });
    byId("token-onboarding-status").textContent = job.status === "succeeded"
      ? `${identity.token_symbol} was already active; no duplicate registry entry was created.`
      : `${identity.token_symbol} onboarding queued as job ${job.job_id}.`;
    byId("token-onboarding-status").hidden = false;
    await Promise.all([loadTokens(), loadJobs()]);
  } catch (error) {
    showAdminError(error);
  } finally {
    byId("add-token-button").disabled = false;
  }
});

byId("retry-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  clearAdminError();
  const index = Number(byId("retry-window").value);
  const window = admin.retryWindows[index];
  if (!window) return;
  byId("retry-button").disabled = true;
  try {
    await request("/api/admin/jobs", {
      method: "POST",
      body: JSON.stringify({
        job_type: "retry_failed",
        token_symbol: window.token_symbol,
        start_date: window.start_date,
        end_date: window.end_date,
        queue_type: window.queue_type,
      }),
    });
    await loadJobs();
  } catch (error) {
    showAdminError(error);
  } finally {
    byId("retry-button").disabled = false;
  }
});

byId("logout-button").addEventListener("click", async () => {
  try {
    await request("/api/admin/logout", { method: "POST", body: "{}" });
  } finally {
    admin.session = null;
    if (admin.pollTimer) window.clearInterval(admin.pollTimer);
    window.location.reload();
  }
});

byId("reload-jobs").addEventListener("click", () => loadJobs().catch(showAdminError));

initializeAdmin().catch(showAdminError);
if (window.lucide) window.lucide.createIcons();
