/* Served only by the local live-CEX runner, after the dashboard application. */
(() => {
  const context = document.getElementById("opportunity-current-context");
  if (!context || document.getElementById("local-opportunity-refresh")) return;

  const endpoint = "/api/local/opportunity-refresh";
  const controls = document.createElement("div");
  controls.className = "module-context";
  const button = document.createElement("button");
  button.id = "local-opportunity-refresh";
  button.type = "button";
  button.className = "icon-command";
  button.textContent = "Refresh live data";
  const help = document.createElement("span");
  help.className = "module-chip";
  help.textContent = "UNI/USDT + CAKE/USDT · Binance + Bybit · Manual snapshots expire after 2 minutes";
  const status = document.createElement("span");
  status.id = "local-opportunity-refresh-status";
  status.className = "module-chip";
  status.setAttribute("role", "status");
  status.setAttribute("aria-live", "polite");
  button.setAttribute("aria-describedby", status.id);
  controls.appendChild(button);
  controls.appendChild(help);
  controls.appendChild(status);
  context.appendChild(controls);

  let busy = true;
  let timer = null;
  let statusObserved = false;
  let lastCohortId = null;

  function lock(value) {
    busy = value;
    button.disabled = value;
    controls.setAttribute("aria-busy", String(value));
  }

  function waitForStatus(seconds) {
    if (timer !== null) clearTimeout(timer);
    timer = null;
    lock(seconds > 0);
    if (seconds > 0) {
      timer = setTimeout(() => {
        timer = null;
        void checkStatus();
      }, seconds * 1000);
    }
  }

  function showState(payload, responseStatus) {
    const seconds = payload?.retry_after_seconds;
    if (!Number.isFinite(seconds) || seconds < 0
      || !["idle", "running", "succeeded", "failed"].includes(payload?.state)) {
      throw new Error("Invalid refresh status");
    }
    if (payload.state === "running" || responseStatus === 409) {
      status.textContent = "A live refresh is already running. Waiting for its result…";
      waitForStatus(Math.max(1, seconds));
    } else if (responseStatus === 429) {
      status.textContent = "Please wait before requesting another snapshot.";
      waitForStatus(Math.max(1, seconds));
    } else if (payload.state === "failed") {
      status.textContent = "Refresh failed. The previously published snapshot is still shown.";
      waitForStatus(seconds);
    } else if (payload.state === "succeeded") {
      status.textContent = "Latest refresh completed. Check the snapshot time in the results.";
      waitForStatus(seconds);
    } else {
      status.textContent = "Ready to collect a new snapshot.";
      waitForStatus(seconds);
    }
  }

  async function reconcileSuccess(payload, initialBaseline = false) {
    const cohortId = payload?.receipt?.route_cohort_id;
    if (payload.state !== "succeeded" || typeof cohortId !== "string"
      || !cohortId || cohortId === lastCohortId) return;
    lastCohortId = cohortId;
    // The normal application load owns the initial snapshot. Later confirmations
    // may complete a request whose POST response was lost, or another tab's run.
    if (initialBaseline || app.route?.kind !== "opportunities"
      || opportunityScope(app.route.filters) !== "current"
      || (Array.isArray(app.route.validationErrors) && app.route.validationErrors.length)) return;
    const loaded = await loadOpportunities();
    if (!loaded) {
      status.textContent = "Live snapshot published, but the results could not be reloaded. Reload this page to view it.";
    }
  }

  async function checkStatus() {
    lock(true);
    try {
      const response = await fetch(endpoint, {
        method: "GET", credentials: "omit", cache: "no-store",
      });
      if (!response.ok) throw new Error("Refresh status unavailable");
      const payload = await response.json();
      const initialBaseline = !statusObserved;
      showState(payload, response.status);
      statusObserved = true;
      await reconcileSuccess(payload, initialBaseline);
    } catch (_) {
      status.textContent = "Refresh status is unavailable. Click to request a new snapshot.";
      lock(false);
    }
  }

  button.addEventListener("click", async () => {
    if (busy) return;
    lock(true);
    statusObserved = true;
    status.textContent = "Fetching live order books and calculating routes…";
    try {
      const response = await fetch(endpoint, {
        method: "POST", credentials: "omit", cache: "no-store",
        headers: { "X-Opportunity-Refresh": "1" },
      });
      const payload = await response.json();
      if (!response.ok && ![409, 429, 502].includes(response.status)) {
        throw new Error("Refresh result unavailable");
      }
      showState(payload, response.status);
      if (response.ok) await reconcileSuccess(payload);
    } catch (_) {
      status.textContent = "Refresh result could not be confirmed. Existing results remain on screen.";
      waitForStatus(30);
    }
  });

  status.textContent = "Checking local refresh status…";
  void checkStatus();
})();
