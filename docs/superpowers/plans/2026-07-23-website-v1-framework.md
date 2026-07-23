# Website V1 Reusable Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable CEX/DEX dashboard framework for local and Tencent Cloud container deployment.

**Architecture:** A Python standard-library HTTP service serves a static browser
dashboard and a curated public CSV snapshot. Public runtime code can read only
`data/public/`; collection and private research remain outside the image.

**Tech Stack:** Python 3.13, HTML, CSS, JavaScript, Chart.js, Docker, unittest

## Global Constraints

- Work on branch `website-v1`.
- Do not modify the existing Tencent Cloud production website.
- Do not copy private research inputs, local state, credentials, or API keys.
- Keep the runtime portable; do not add Render-specific configuration.
- Preserve facts and data limitations; do not describe anomalies as executable arbitrage.

---

### Task 1: Public dashboard runtime

**Files:**
- Create: `tests/test_dashboard.py`
- Create: `dashboard/server.py`
- Create: `dashboard/__init__.py`
- Create: `dashboard/static/index.html`
- Create: `dashboard/static/app.js`
- Create: `dashboard/static/styles.css`
- Create: `dashboard/package.json`
- Create: `dashboard/package-lock.json`
- Create: `data/public/research/factor_panel.csv`
- Create: `data/public/research/candidate_factor_forward_returns.csv`
- Create: `data/public/research/coverage_summary.csv`
- Create: `data/public/research/cex_scope_sensitivity.csv`
- Create: `data/public/research/manifest.json`

**Interfaces:**
- Consumes: curated CSV files under `data/public/research/`
- Produces: `build_dashboard_payload() -> dict` and HTTP routes `/`, `/api/data`, `/health`

- [ ] **Step 1: Copy the existing dashboard test before runtime code**

Copy `tests/test_dashboard.py` from the committed
`origin/feature/research-report` branch of `web3project`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `python3 -m unittest tests.test_dashboard -v`

Expected: FAIL because the `dashboard` package does not exist.

- [ ] **Step 3: Copy the minimal proven public runtime and curated snapshot**

Copy the committed `dashboard/`, `data/public/`, and dashboard test files from
`origin/feature/research-report`. Do not copy `data/a_review/`, `data/research/`,
or local state files.

- [ ] **Step 4: Run the test to verify it passes**

Run: `python3 -m unittest tests.test_dashboard -v`

Expected: all dashboard tests pass.

### Task 2: Portable deployment shell

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `.gitignore`
- Create: `scripts/run_dashboard.sh`
- Create: `README.md`

**Interfaces:**
- Consumes: `dashboard/server.py` and `data/public/`
- Produces: local command `./scripts/run_dashboard.sh` and container command using `${PORT}`

- [ ] **Step 1: Add a structure test**

Add assertions to `tests/test_framework.py` that required paths exist, that
`Dockerfile` copies only `dashboard` and `data/public`, and that no
`render.yaml` exists.

- [ ] **Step 2: Run the structure test to verify it fails**

Run: `python3 -m unittest tests.test_framework -v`

Expected: FAIL because deployment files do not exist.

- [ ] **Step 3: Add deployment and documentation files**

Reuse the proven multi-stage Docker build, use `${PORT:-8765}`, document local
startup, public-data boundaries, Tencent Cloud replacement steps, and rollback.

- [ ] **Step 4: Run the complete verification**

Run: `python3 -m unittest discover -s tests -v`

Expected: all tests pass.

Run: `docker build -t cex-dex-analysis:website-v1 .`

Expected: image builds successfully when Docker is available.

### Task 3: Git publication

**Files:** all files in Tasks 1 and 2

**Interfaces:**
- Consumes: verified local working tree
- Produces: remote `main` and `website-v1` branches in `lzd705/CEX-DEX-analysis`

- [ ] **Step 1: Initialize and publish main**

Commit the original `readme.md` on `main` and push it to `origin/main`.

- [ ] **Step 2: Create and publish the framework branch**

Create `website-v1`, commit only the reviewed framework files, and push with
upstream tracking to `origin/website-v1`.

- [ ] **Step 3: Verify remote branches**

Run: `git ls-remote --heads origin main website-v1`

Expected: one commit SHA for each branch.

