const admin = {
  session: null,
  pollTimer: null,
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
  if (!response.ok) throw new Error(payload.error || "Administrator request failed");
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

function statusClass(status) {
  return ["succeeded", "failed", "running", "queued", "interrupted"].includes(status)
    ? status
    : "unknown";
}

function renderJobs(jobs) {
  byId("jobs-summary").textContent = `${jobs.length} recent jobs`;
  byId("jobs-body").innerHTML = jobs.length
    ? jobs.map((job) => `<tr>
        <td>${escapeHtml(new Date(job.created_at).toLocaleString())}</td>
        <td><strong>${escapeHtml(job.token_symbol)}</strong></td>
        <td>${escapeHtml(job.start_date)} → ${escapeHtml(job.end_date)}</td>
        <td><span class="job-status ${statusClass(job.status)}">${escapeHtml(job.status)}</span></td>
        <td>${job.finished_at ? escapeHtml(new Date(job.finished_at).toLocaleString()) : "--"}</td>
        <td>${escapeHtml(job.error || "--")}</td>
      </tr>`).join("")
    : '<tr><td colspan="6" class="empty-jobs">No refresh jobs</td></tr>';
}

async function loadJobs() {
  const payload = await request("/api/admin/jobs");
  renderJobs(payload.jobs);
}

function startPolling() {
  if (admin.pollTimer) window.clearInterval(admin.pollTimer);
  admin.pollTimer = window.setInterval(() => loadJobs().catch(showAdminError), 5000);
}

async function showWorkspace(session) {
  admin.session = session;
  byId("login-view").hidden = true;
  byId("admin-view").hidden = false;
  byId("session-user").textContent = session.username;
  setDefaultDates();
  await Promise.all([loadTokens(), loadJobs()]);
  startPolling();
}

async function initializeAdmin() {
  const session = await request("/api/admin/session");
  if (!session.configured) {
    byId("login-status").textContent = "ADMIN_PASSWORD_HASH is not configured";
    byId("login-button").disabled = true;
    return;
  }
  byId("login-status").textContent = "Server-side authentication enabled";
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
