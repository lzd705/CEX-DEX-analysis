const admin = {
  session: null,
  pollTimer: null,
  tokenCandidate: null,
  retryWindows: [],
  manualReviewItems: [],
  tokenReviews: [],
  reviewLoadGeneration: 0,
  reviewActions: new Set(),
  reviewElements: new Map(),
  focusedReviewHash: null,
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
    const startFailed = payload.onboarding_error_code === "onboarding_start_failed";
    const error = new Error(startFailed
      ? "Onboarding start failed. The request remains approved; review it before trying Start again."
      : payload.error || "Administrator request failed");
    error.code = startFailed ? payload.onboarding_error_code : payload.error_code || "";
    error.retryable = payload.retryable === true;
    if (startFailed) error.review = payload;
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

function canReviewTokens() {
  return admin.session?.authenticated === true
    && admin.session.login_required === true && Boolean(admin.session.csrf_token);
}

function validReviewIdentity(review) {
  return typeof review?.request_id === "string" && /^[0-9a-f]{32}$/.test(review.request_id)
    && Number.isSafeInteger(review.revision) && review.revision > 0;
}

function reviewText(tag, value, className = "") {
  const node = document.createElement(tag);
  node.textContent = String(value ?? "—").slice(0, 100000);
  if (className) node.className = className;
  return node;
}

function showReviewStatus(message) {
  const target = byId("token-review-status");
  target.textContent = message;
  target.hidden = false;
}

function linkedReviewId() {
  const match = /^#token-review=([0-9a-f]{32})$/.exec(window.location.hash);
  return match ? match[1] : null;
}

function focusLinkedReview() {
  const hash = window.location.hash;
  const requestId = linkedReviewId();
  if (!requestId || admin.focusedReviewHash === hash) return;
  const row = byId(`token-review-${requestId}`);
  if (!row) return;
  row.classList.add("token-review-highlight");
  row.focus();
  admin.focusedReviewHash = hash;
}

function renderTokenReviews() {
  const target = byId("token-reviews-body");
  const scroller = byId("token-reviews-scroll");
  const scrollPosition = [scroller.scrollTop, scroller.scrollLeft];
  const previousUi = new Map();
  admin.reviewElements.forEach((nodes, requestId) => {
    previousUi.set(requestId, new Map([...nodes].map(([key, node]) => [key, {
      open: node.open, scrollTop: node.scrollTop, scrollLeft: node.scrollLeft,
      focused: document.activeElement === node,
    }])));
  });
  admin.reviewElements = new Map();
  target.replaceChildren();
  byId("token-review-count").textContent = `${admin.tokenReviews.length} recent requests`;
  byId("token-review-access").textContent = canReviewTokens()
    ? "Actions use the displayed revision. A changed request must be reviewed again."
    : "Read only. Sign in as the configured administrator to submit, decide, retry email, or start onboarding.";
  if (!admin.tokenReviews.length) {
    const row = document.createElement("tr");
    const cell = reviewText("td", "No Token review requests", "empty-jobs");
    cell.colSpan = 6; row.append(cell); target.append(row);
  }
  admin.tokenReviews.forEach((review) => {
    const row = document.createElement("tr");
    const nodes = new Map([["row", row]]);
    if (validReviewIdentity(review)) {
      admin.reviewElements.set(review.request_id, nodes);
      row.id = `token-review-${review.request_id}`;
      row.tabIndex = -1;
      if (window.location.hash === `#token-review=${review.request_id}`) row.className = "token-review-highlight";
    }
    const identity = document.createElement("td");
    identity.append(reviewText("strong", review.token_symbol),
      reviewText("p", `${review.chain}:${review.contract_address}`),
      reviewText("p", `Request ${review.request_id} · revision ${review.revision}`),
      reviewText("p", `Created ${review.created_at} · ${review.requested_history_days} days`));
    const state = document.createElement("td");
    state.append(reviewText("strong", review.status),
      reviewText("p", review.reviewer ? `Decision: ${review.reviewer} · ${review.reviewed_at}` : "Awaiting reviewer decision"),
      reviewText("p", review.onboarding_job_id ? `Job ${review.onboarding_job_id}` : "No onboarding job queued"));
    if (review.onboarding_error_code) state.append(reviewText("p", review.onboarding_error_code));
    const notification = review.notification || {};
    const mail = document.createElement("td");
    mail.append(reviewText("strong", notification.status),
      reviewText("p", `${notification.attempts ?? 0}/3 delivery attempts`));
    if (notification.error_code) mail.append(reviewText("p", notification.error_code));
    if (notification.retry_at) mail.append(reviewText("p", `Retry after ${notification.retry_at}`));
    if (notification.sent_at) mail.append(reviewText("p", `Sent ${notification.sent_at}`));
    const evidence = document.createElement("td");
    const details = document.createElement("details");
    details.append(reviewText("summary", "Candidate and digest"),
      reviewText("p", review.candidate_sha256),
      reviewText("pre", JSON.stringify(review.candidate, null, 2)));
    nodes.set("candidate", details);
    nodes.set("candidate-summary", details.children[0]);
    nodes.set("candidate-text", details.children[2]);
    evidence.append(details);
    const audit = document.createElement("td");
    const auditDetails = document.createElement("details");
    auditDetails.append(reviewText("summary", "Audit history"),
      reviewText("pre", JSON.stringify((review.audit || []).slice(0, 50), null, 2)));
    nodes.set("audit", auditDetails);
    nodes.set("audit-summary", auditDetails.children[0]);
    nodes.set("audit-text", auditDetails.children[1]);
    audit.append(auditDetails);
    const controls = document.createElement("td");
    const buttons = document.createElement("div");
    buttons.className = "token-review-actions";
    const addAction = (label, action) => {
      const button = reviewText("button", label, "admin-secondary");
      button.type = "button";
      button.disabled = admin.reviewActions.has(review.request_id);
      // This closure owns the revision displayed in this row, not a later poll's value.
      button.addEventListener("click", () => actOnTokenReview(review, action));
      nodes.set(action, button);
      buttons.append(button);
    };
    if (canReviewTokens() && validReviewIdentity(review)) {
      if (review.status === "pending_review") {
        addAction("Approve", "approve"); addAction("Reject", "reject");
      }
      if (review.status === "approved") addAction("Start onboarding", "start");
      if (["failed", "disabled", "unconfigured"].includes(notification.status)
          && Number.isInteger(notification.attempts) && notification.attempts < 3
          && (!notification.retry_at || Date.parse(notification.retry_at) <= Date.now())) {
        addAction("Retry email", "retry");
      }
    }
    controls.append(buttons);
    row.append(identity, state, mail, evidence, audit, controls);
    target.append(row);
  });
  // Recreate action closures with current revisions, but retain the reader's place.
  admin.reviewElements.forEach((nodes, requestId) => {
    const saved = previousUi.get(requestId);
    if (!saved) return;
    let focusKey;
    saved.forEach((state, key) => {
      if (state.focused) focusKey = key;
      const node = nodes.get(key);
      if (!node) return;
      if (typeof state.open === "boolean") node.open = state.open;
      node.scrollTop = state.scrollTop;
      node.scrollLeft = state.scrollLeft;
    });
    if (focusKey) {
      const control = nodes.get(focusKey);
      const focusTarget = control && !control.disabled ? control : nodes.get("row");
      focusTarget.focus({preventScroll: true});
    }
  });
  scroller.scrollTop = scrollPosition[0];
  scroller.scrollLeft = scrollPosition[1];
  focusLinkedReview();
}

async function loadTokenReviews() {
  const generation = ++admin.reviewLoadGeneration;
  const payload = await request("/api/admin/token-reviews");
  if (generation !== admin.reviewLoadGeneration) return;
  if (!Array.isArray(payload.reviews)) throw new Error("Token review list is unavailable");
  const reviews = payload.reviews.slice(0, 50);
  const linkedId = linkedReviewId();
  let linkedReviewMissing = false;
  if (linkedId && !reviews.some(review => review?.request_id === linkedId)) {
    let linkedReview;
    try {
      linkedReview = await request(`/api/admin/token-reviews/${linkedId}`);
    } catch (error) {
      if (generation !== admin.reviewLoadGeneration) return;
      if (error.code !== "review_not_found") throw error;
      linkedReviewMissing = true;
    }
    if (generation !== admin.reviewLoadGeneration) return;
    if (linkedReview && (!validReviewIdentity(linkedReview) || linkedReview.request_id !== linkedId)) {
      throw new Error("Linked Token review is unavailable");
    }
    if (linkedReview) reviews.push(linkedReview);
  }
  const current = new Map(admin.tokenReviews.map(review => [review.request_id, review]));
  admin.tokenReviews = reviews.map(review => {
    const previous = current.get(review.request_id);
    return previous && previous.revision > review.revision ? previous : review;
  });
  renderTokenReviews();
  if (linkedReviewMissing) {
    showReviewStatus("Linked Token review is unavailable; showing recent requests.");
  }
}

async function actOnTokenReview(review, action) {
  const suffixes = {approve:"decision", reject:"decision", retry:"notification/retry", start:"start"};
  if (!canReviewTokens() || !validReviewIdentity(review)
      || !Object.hasOwn(suffixes, action) || admin.reviewActions.has(review.request_id)) return;
  if ((["approve", "reject"].includes(action) && review.status !== "pending_review")
      || (action === "start" && review.status !== "approved")) return;
  admin.reviewActions.add(review.request_id);
  ++admin.reviewLoadGeneration;
  renderTokenReviews();
  const payload = {expected_revision: review.revision};
  if (["approve", "reject"].includes(action)) payload.decision = action;
  let updated;
  try {
    updated = await request(`/api/admin/token-reviews/${review.request_id}/${suffixes[action]}`, {
      method:"POST", body:JSON.stringify(payload),
    });
    showReviewStatus(`Request ${updated.request_id} · review ${updated.status}. Email notification: ${updated.notification?.status || "unknown"}.`
      + (action === "approve" ? " Approval does not start collection. Use Start onboarding separately." : ""));
  } catch (error) {
    updated = error.review;
    showReviewStatus(error.code === "stale_revision"
      ? "This request changed. Reloading; review the updated request again before choosing an action."
      : error.message);
  } finally {
    if (validReviewIdentity(updated) && updated.request_id === review.request_id) {
      admin.tokenReviews = admin.tokenReviews.map(item => item.request_id === updated.request_id
        && item.revision <= updated.revision ? updated : item);
    }
    admin.reviewActions.delete(review.request_id);
    renderTokenReviews();
    await loadTokenReviews().catch(showAdminError);
    if (action === "start") await loadJobs().catch(showAdminError);
  }
}

function startPolling() {
  if (admin.pollTimer) window.clearInterval(admin.pollTimer);
  admin.pollTimer = window.setInterval(
    () => Promise.all([
      loadJobs(),
      loadRetryWindows(),
      loadManualReviews(),
      loadTokenReviews(),
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
  byId("add-token-button").disabled = true;
  setDefaultDates();
  await Promise.all([
    loadTokens(),
    loadJobs(),
    loadRetryWindows(),
    loadManualReviews(),
    loadTokenReviews(),
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
  byId("add-token-button").disabled = true;
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
    byId("add-token-button").disabled = candidate.already_configured === true || !canReviewTokens();
    byId("add-token-button").querySelector("span").textContent = candidate.already_configured
      ? "Already active"
      : "Submit for review";
    byId("token-onboarding-status").textContent = candidate.already_configured
      ? "This contract is already active and cannot be submitted for review."
      : "Identity and pool membership validated. Submission does not add a catalog entry or start collection.";
    byId("token-onboarding-status").hidden = false;
  } catch (error) {
    showAdminError(error);
  } finally {
    byId("resolve-token-button").disabled = false;
  }
});

byId("add-token-button").addEventListener("click", async () => {
  const candidate = admin.tokenCandidate;
  if (!candidate || candidate.already_configured || !canReviewTokens() || byId("add-token-button").disabled) return;
  clearAdminError();
  byId("add-token-button").disabled = true;
  try {
    const identity = candidate.identity;
    const review = await request("/api/admin/tokens", {
      method: "POST",
      body: JSON.stringify({
        chain: identity.chain,
        contract_address: identity.contract_address,
        expected_token_symbol: identity.token_symbol,
        history_days: Number(byId("token-history-days").value),
      }),
    });
    if (admin.tokenCandidate !== candidate) return;
    byId("token-onboarding-status").textContent = `Request ${review.request_id} · ${review.token_symbol} · ${review.chain}:${review.contract_address}. `
      + `Review: ${review.status}. Email notification: ${review.notification?.status || "unknown"}. `
      + `${review.deduplicated ? "Existing request; duplicate submission." : "New request."} `
      + "This submission adds no catalog entry and starts no collection. Review the current state below; approval and Start are separate actions.";
    byId("token-onboarding-status").hidden = false;
    await loadTokenReviews().catch(showAdminError);
  } catch (error) {
    if (admin.tokenCandidate !== candidate) return;
    showAdminError(error);
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
window.addEventListener("hashchange", () => {
  admin.focusedReviewHash = null;
  focusLinkedReview();
});

initializeAdmin().catch(showAdminError);
if (window.lucide) window.lucide.createIcons();
