# Token Contract Review Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every new Token contract submission wait for one authenticated reviewer approval and a separate explicit onboarding start.

**Architecture:** Add a focused SQLite review store and injectable email adapter, then route both public and administrator submissions into that store. Reuse the existing administrator session/CSRF boundary and existing onboarding worker only after the persisted approval gate.

**Tech Stack:** Python standard library (`sqlite3`, `smtplib`, `EmailMessage`), existing `http.server` application, vanilla JavaScript, `unittest`.

**Spec:** `docs/superpowers/specs/2026-09-09-token-review-gate-design.md`

## Global Constraints

- The only approval policy is one authenticated approval (`1/1`).
- Reviewer email is injected with `TOKEN_REVIEWER_EMAIL`; no recipient or credential is hardcoded.
- Persist review state before email, registry, collection, or publication side effects.
- Email is notification-only; approval and start require authenticated CSRF-protected POST requests.
- Duplicate submissions do not create duplicate requests or messages.
- No real email, third-party request, deployment, or production state change occurs in tests.

---

### Task 1: Durable review ledger and email boundary

**Files:**
- Create: `dashboard/token_reviews.py`
- Create: `dashboard/token_review_email.py`
- Create: `tests/test_token_reviews.py`

**Interfaces:**
- Produces: `TokenReviewStore`, `TokenReviewError`, `review_candidate_snapshot`, `public_token_review`, `TokenReviewEmailSettings`, `SmtpTokenReviewMailer`, and `build_token_review_message`.
- Consumes: canonical chain/address/symbol normalization from `scripts/token_registry.py`.

- [ ] **Step 1: Write failing storage tests**

  Add tests proving a valid request stores canonical identity, metadata digest,
  notification state, and audit event; concurrent identical inserts return one
  request; malformed databases fail closed; revision mismatches cannot mutate;
  and the UTC daily count reads persisted requests.

- [ ] **Step 2: Run the storage tests and verify RED**

  Run `python -m unittest tests.test_token_reviews.TokenReviewStoreTest -v` and
  confirm it fails because `dashboard.token_reviews` does not exist.

- [ ] **Step 3: Implement the minimum SQLite ledger**

  Create tables with a unique canonical identity, transactionally insert the
  request and first audit event, validate every loaded projection, limit lists
  to 50 records, and expose compare-and-swap transitions by `expected_revision`.

- [ ] **Step 4: Write and run failing email-boundary tests**

  Test strict configuration validation, fixed headers, plain-text body, safe
  admin URL construction, no address-derived URL, and rejection of CR/LF
  configuration values. Run `python -m unittest tests.test_token_reviews.TokenReviewEmailTest -v`.

- [ ] **Step 5: Implement and verify the email adapter**

  Build `EmailMessage` objects from server-owned settings and implement SMTP
  send with optional STARTTLS and optional credentials. Re-run
  `python -m unittest tests.test_token_reviews -v` until green.

- [ ] **Step 6: Commit the independent ledger boundary**

  Commit `dashboard/token_reviews.py`, `dashboard/token_review_email.py`,
  `tests/test_token_reviews.py`, the spec, and this plan with message
  `feat(tokens): add durable review ledger`.

### Task 2: Administrator service state machine

**Files:**
- Modify: `dashboard/admin.py`
- Modify: `tests/test_admin.py`

**Interfaces:**
- Consumes: the Task 1 store and mailer interfaces.
- Produces: `submit_token_review`, `list_token_reviews`,
  `decide_token_review`, `retry_token_review_notification`,
  `start_token_review_onboarding`, and `count_token_reviews_created_on`.

- [ ] **Step 1: Write failing submission tests**

  Prove submit re-resolves identity, rejects an already-active Token, persists
  before the fake mailer sees the message, returns the same request on a
  duplicate, records visible mail failures, and creates neither job nor runtime
  registry record before approval.

- [ ] **Step 2: Run targeted tests and verify RED**

  Run the new `AdminServiceTest` methods individually and confirm the missing
  methods fail before implementation.

- [ ] **Step 3: Implement submission and notification transitions**

  Inject the store, reviewer settings, clock, and mailer through the constructor.
  Claim each attempt in the database before sending, persist only stable error
  codes, enforce three attempts and 60-second retry cooldown, and never resend
  from duplicate submissions.

- [ ] **Step 4: Write failing decision/start tests**

  Prove only the configured login username can approve/reject; stale revisions
  fail; approval alone creates no job; rejected requests cannot start; start
  uses only stored chain/address/symbol/history; duplicate starts cannot create a
  second job; and a start failure returns the review to approved.

- [ ] **Step 5: Implement decision and explicit start**

  Reserve `onboarding_starting` with compare-and-swap before invoking
  `create_onboarding_job`, then save the job id as `onboarding_queued`; restore
  `approved` with a stable error code on a known failure.

- [ ] **Step 6: Run backend service tests**

  Run `python -m unittest tests.test_token_reviews tests.test_admin -v` and keep
  the existing onboarding tests green.

### Task 3: HTTP contracts and persisted public budget

**Files:**
- Modify: `dashboard/public_actions.py`
- Modify: `dashboard/server.py`
- Modify: `tests/test_public_actions.py`
- Modify: `tests/test_admin.py`

**Interfaces:**
- Consumes: Task 2 service methods.
- Produces: review-only public/admin submission routes and authenticated admin
  list, decision, notification retry, and start routes.

- [ ] **Step 1: Replace the public route test with a failing review contract**

  Assert `/api/actions/tokens` calls `submit_token_review` with a server-fixed
  30-day window, returns HTTP 202 and a public projection, and never calls
  `create_onboarding_job`.

- [ ] **Step 2: Add failing admin route security tests**

  Test unauthenticated, missing-CSRF, open-local, malformed id, extra-field,
  stale-revision, decision, retry, and start cases. Assert only a successful
  `/start` route can reach the onboarding service.

- [ ] **Step 3: Implement the routes and projections**

  Match only lowercase 32-hex request ids, reject query strings, require exact
  request fields, map stable review errors to 400/403/404/409/429/503, and keep
  reviewer/SMTP/internal fields out of the public response.

- [ ] **Step 4: Switch the daily public budget to persisted reviews**

  Keep three submissions per client per hour and three accepted reviews per UTC
  day. Call `count_token_reviews_created_on` for Token submissions while keeping
  job counting unchanged for quality and snapshot actions.

- [ ] **Step 5: Run route regressions**

  Run `python -m unittest tests.test_public_actions tests.test_admin -v` and
  verify all existing security-boundary tests pass.

- [ ] **Step 6: Commit the backend workflow**

  Commit the service, route, and test changes with message
  `feat(tokens): require review before onboarding`.

### Task 4: Public receipt and administrator review UI

**Files:**
- Modify: `dashboard/static/actions.html`
- Modify: `dashboard/static/actions.js`
- Modify: `dashboard/static/admin.html`
- Modify: `dashboard/static/admin.js`
- Modify: `dashboard/static/admin.css`
- Modify: `tests/test_public_actions_frontend.py`
- Modify: `tests/test_framework.py`

**Interfaces:**
- Consumes: Task 3 JSON contracts.
- Produces: a public pending-review receipt and an authenticated reviewer table
  with approve, reject, retry-email, and start controls.

- [ ] **Step 1: Write failing frontend contract tests**

  Assert the public copy says “Submit for review”, exposes the request id and
  no-catalog/no-collection state, and does not pass the Token submission result
  to `rememberJob`. Assert the admin document contains a review table and its
  script calls all four review endpoints with `expected_revision`.

- [ ] **Step 2: Run frontend tests and verify RED**

  Run `python -m unittest tests.test_public_actions_frontend tests.test_framework -v`.

- [ ] **Step 3: Implement public review receipt**

  Rename the Token action and guidance, render request/status/notification from
  the review response, keep the button disabled after success, and retain job
  polling only for fact-recovery jobs.

- [ ] **Step 4: Implement administrator controls**

  Render bounded values with `textContent`, refresh reviews with existing polls,
  and send the displayed record revision for each action. Show Start only for
  approved requests and keep every email failure distinct from review status.

- [ ] **Step 5: Parse and run frontend regressions**

  Run Node syntax checks for `actions.js` and `admin.js`, then run the two Python
  frontend suites until green.

### Task 5: Configuration, operations, and final verification

**Files:**
- Modify: `.env.example`
- Modify: `deploy/dashboard.env.example`
- Modify: `docs/admin-operations.md`
- Modify: `README.md`
- Modify: tests covering deployment/config contracts where applicable.

**Interfaces:**
- Consumes: all prior tasks.
- Produces: operator-ready setup and recovery instructions without live secrets.

- [ ] **Step 1: Add fail-closed configuration examples**

  Document every `TOKEN_REVIEW_*` variable with email disabled and recipient,
  base URL, relay, sender, username, and password blank by default.

- [ ] **Step 2: Document the operating sequence**

  Describe submit, review, notification retry, approve/reject, explicit start,
  job monitoring, SQLite backup, and failure recovery. State that email delivery
  is not approval and that the runtime registry remains untouched pre-start.

- [ ] **Step 3: Run focused and full verification**

  Run the Token review, admin, public action, onboarding, registry, frontend,
  static-delivery, framework, and release smoke suites, followed by the complete
  repository unit-test command documented by the project.

- [ ] **Step 4: Inspect the diff and security invariants**

  Confirm no real recipient or credential is committed; no public/admin submit
  route calls onboarding; only authenticated CSRF-protected Start does; tests
  never access the network; and the worktree contains no unrelated changes.

- [ ] **Step 5: Commit the UI and operations delivery**

  Commit with message `feat(tokens): expose authenticated review workflow`,
  then report branch, commit hashes, exact tests, configuration still required,
  and that no real email or deployment occurred.

