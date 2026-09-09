import shutil
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = PROJECT_ROOT / "dashboard" / "static"


# Browser APIs and fetch are the external boundary; execute the real handlers.
DOM = r"""
const assert = require('node:assert/strict');
const elements = new Map();
class Element {
  constructor(tag = 'div') {
    this.tagName = tag; this.children = []; this.listeners = {}; this.dataset = {};
    this.disabled = false; this.hidden = false; this.value = ''; this._text = '';
    this.classList = {add: () => {}, remove: () => {}, toggle: () => {}};
  }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(child => child.textContent).join(' '); }
  set innerHTML(value) {
    if (this.reviewNode) throw Error('Review values must not use innerHTML');
    this._text = String(value); this.children = [];
  }
  set id(value) { this._id = value; elements.set(value, this); }
  get id() { return this._id; }
  append(...children) { this.children.push(...children); }
  appendChild(child) { this.append(child); return child; }
  replaceChildren(...children) { this.children = children; this._text = ''; }
  addEventListener(name, callback) { this.listeners[name] = callback; }
  setAttribute(name, value) { this[name] = String(value); }
  querySelector() { return this.children[0] || (this.children[0] = new Element('span')); }
  focus() { this.focused = true; }
  scrollIntoView() {}
  async click() { if (!this.disabled) await this.listeners.click?.({target:this}); }
}
function element(id) {
  if (!elements.has(id)) { const el = new Element(); el.id = id; }
  return elements.get(id);
}
global.document = {
  getElementById: element,
  createElement: tag => { const el = new Element(tag); el.reviewNode = true; return el; },
};
const intervals = [];
const timeouts = [];
global.window = {location: {hash:'', reload() {}},
  setInterval: callback => { intervals.push(callback); return intervals.length; },
  clearInterval() {}, setTimeout: callback => { timeouts.push(callback); return timeouts.length; },
  clearTimeout() {}, addEventListener() {},
};
global.sessionStorage = {getItem: () => null, setItem() {}};
const requests = [];
const review = {
  request_id: 'a'.repeat(32), revision: 4, chain: 'eth',
  contract_address: '0x' + '12'.repeat(20), token_symbol: 'TEST',
  requested_history_days: 90, submitter: 'public:add_token',
  created_at: '2026-09-09T00:00:00+00:00', status: 'pending_review',
  candidate: {identity:{token_name:'Test <img src=x onerror=alert(1)>', token_symbol:'TEST'},
    discovery:{usable_pool_count:1, top_pools:[{pool_name:'TEST / WETH', dex:'uniswap'}]}},
  candidate_sha256:'b'.repeat(64), reviewer:null, reviewed_at:null,
  notification:{status:'disabled', attempts:0, error_code:null, retry_at:null, sent_at:null},
  onboarding_job_id:null, onboarding_error_code:null,
  audit:[{event:'created', at:'2026-09-09T00:00:00+00:00', actor:'public:add_token'}],
  deduplicated:false,
};
let listedReviews = [review];
let postResult = review;
let postStatus = 200;
let transport = async (path, options) => {
  let data;
  if (options.method === 'POST') data = postResult;
  else if (path === '/api/admin/token-reviews') data = {reviews:listedReviews, count:listedReviews.length};
  else if (path === '/api/admin/tokens') data = {tokens:[]};
  else if (path === '/api/admin/jobs') data = {jobs:[]};
  else if (path.includes('manual-review')) data = {review_items:[]};
  else data = {windows:[]};
  return {ok:options.method !== 'POST' || postStatus < 400, status:options.method === 'POST' ? postStatus : 200, json:async () => data};
};
global.fetch = async (path, options = {}) => { requests.push({path, ...options}); return transport(path, options); };
const session = {authenticated:true, login_required:true, username:'reviewer', csrf_token:'csrf-test'};
function descendants(node) { return [node, ...node.children.flatMap(descendants)]; }
function buttons() { return descendants(element('token-reviews-body')).filter(node => node.tagName === 'button'); }
function button(label) { return buttons().find(node => node.textContent === label); }
function submit(id) { return element(id).listeners.submit({preventDefault() {}}); }
"""


class PublicActionsFrontendTest(unittest.TestCase):
    def test_public_page_exposes_only_bounded_collection_contracts(self):
        html = (STATIC_ROOT / "actions.html").read_text(encoding="utf-8")
        javascript = (STATIC_ROOT / "actions.js").read_text(encoding="utf-8")
        dashboard = (STATIC_ROOT / "index.html").read_text(encoding="utf-8")

        self.assertIn('href="/actions.html">Data Actions</a>', dashboard)
        self.assertIn("Smart-contract address", html)
        self.assertIn("Audited windows only", html)
        self.assertIn("Submit for review", html)
        self.assertIn("1/1", html)
        self.assertNotIn('type="date"', html)
        self.assertNotIn("/api/admin/", javascript)
        self.assertIn('"/api/actions/tokens/resolve"', javascript)
        self.assertIn('"/api/actions/tokens"', javascript)
        self.assertIn('"/api/actions/quality/retryable"', javascript)
        self.assertIn('"/api/actions/quality/retry"', javascript)
        self.assertIn("/api/actions/jobs/", javascript)
        self.assertIn("expected_token_symbol", javascript)
        self.assertIn("queue_type: window.queue_type", javascript)
        self.assertIn("capabilities.cex", javascript)
        self.assertIn("Existing catalog record", javascript)
        self.assertIn("slice(-10)", javascript)
        self.assertIn("/^[0-9a-f]{32}$/.test(value)", javascript)

    def test_public_page_has_keyboard_and_live_status_contracts(self):
        html = (STATIC_ROOT / "actions.html").read_text(encoding="utf-8")
        styles = (STATIC_ROOT / "actions.css").read_text(encoding="utf-8")

        self.assertIn('role="tooltip"', html)
        self.assertIn('aria-live="polite"', html)
        self.assertIn('tabindex="0"', html)
        self.assertIn("@media (max-width: 700px)", styles)
        self.assertIn(".public-retry-table th", styles)

    def test_public_javascript_parses(self):
        node = shutil.which("node")
        if node is None:
            raise unittest.SkipTest("Node.js is not installed")
        for filename in ("actions.js", "admin.js"):
            subprocess.run(
                [node, "--check", str(STATIC_ROOT / filename)],
                cwd=PROJECT_ROOT, check=True, capture_output=True, text=True,
            )

    def run_behavior(self, filename, action):
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node.js is unavailable")
        source = (STATIC_ROOT / filename).read_text(encoding="utf-8")
        source = source.replace("initializePublicActions();", "")
        source = source.replace("initializeAdmin().catch(showAdminError);", "")
        program = DOM + "\n" + source + "\n(async () => {\n" + action
        program += "\nconsole.log('BEHAVIOR_OK');\n})().catch(error => {console.error(error); process.exitCode = 1;});"
        result = subprocess.run([node, "-e", program], cwd=PROJECT_ROOT,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "BEHAVIOR_OK", "Behavior did not finish")

    def test_public_receipt_preserves_duplicate_status_without_a_job(self):
        self.run_behavior("actions.js", r"""
publicActions.candidate = {identity:review};
postResult = {...review, status:'rejected', deduplicated:true};
await element('public-token-add').click();
const receipt = element('public-token-status').textContent;
assert.ok(receipt.includes(review.request_id));
assert.ok(receipt.includes('TEST') && receipt.includes(review.contract_address));
assert.ok(receipt.includes('rejected') && receipt.includes('disabled'));
assert.match(receipt, /existing|duplicate/i);
assert.match(receipt, /no catalog entry|not added to the catalog/i);
assert.match(receipt, /collection has not started|no collection/i);
assert.equal(publicActions.jobs.size, 0);
assert.equal(timeouts.length, 0);
assert.equal(element('public-token-add').disabled, true);
assert.deepEqual(JSON.parse(requests[0].body), {
 chain:'eth', contract_address:review.contract_address, expected_token_symbol:'TEST'});
""")

    def test_admin_submission_preserves_history_and_success_when_reload_fails(self):
        self.run_behavior("admin.js", r"""
admin.session = session; admin.tokenCandidate = {identity:review};
element('token-history-days').value = '90';
const original = transport;
transport = (path, options) => options.method === 'POST'
 ? original(path, options) : Promise.reject(Error('List unavailable'));
await element('add-token-button').click();
assert.equal(JSON.parse(requests[0].body).history_days, 90);
assert.ok(element('token-onboarding-status').textContent.includes(review.request_id));
assert.equal(element('add-token-button').disabled, true);
assert.ok(!element('token-onboarding-status').textContent.includes('queued as job'));
""")

    def test_review_controls_send_displayed_revision_and_keep_start_separate(self):
        self.run_behavior("admin.js", r"""
await showWorkspace(session);
assert.ok(button('Approve')); assert.ok(button('Reject')); assert.ok(!button('Start onboarding'));
const approve = button('Approve');
listedReviews = [{...review, revision:5, status:'approved'}]; postResult = listedReviews[0];
await approve.click();
const mutations = requests.filter(item => item.method === 'POST');
assert.equal(mutations.length, 1);
assert.equal(mutations[0].path, '/api/admin/token-reviews/' + 'a'.repeat(32) + '/decision');
assert.deepEqual(JSON.parse(mutations[0].body), {decision:'approve', expected_revision:4});
assert.equal(mutations[0].headers['X-CSRF-Token'], 'csrf-test');
assert.ok(button('Start onboarding')); assert.ok(!button('Approve'));
await button('Start onboarding').click();
assert.deepEqual(JSON.parse(requests.filter(item => item.method === 'POST')[1].body), {expected_revision:5});
""")

    def test_review_values_are_text_and_invalid_ids_open_mode_are_non_mutating(self):
        self.run_behavior("admin.js", r"""
listedReviews = [{...review, request_id:'../bad', token_symbol:'<img src=x onerror=alert(1)>'}];
await showWorkspace(session);
assert.equal(buttons().length, 0);
assert.equal(requests.filter(item => item.method === 'POST').length, 0);
listedReviews = [review];
await showWorkspace({...session, login_required:false, csrf_token:''});
assert.equal(buttons().length, 0);
assert.ok(element('token-reviews-body').textContent.includes('<img src=x onerror=alert(1)>'));
assert.equal(element('add-token-button').disabled, true);
""")

    def test_retry_cooldown_attempt_limit_and_hash_never_trigger_actions(self):
        self.run_behavior("admin.js", r"""
window.location.hash = '#token-review=' + review.request_id;
await showWorkspace(session);
assert.ok(button('Retry email'));
assert.ok(descendants(element('token-reviews-body')).some(node => node.focused));
assert.equal(requests.filter(item => item.method === 'POST').length, 0);
await button('Retry email').click();
const mutation = requests.find(item => item.method === 'POST');
assert.equal(mutation.path, '/api/admin/token-reviews/' + review.request_id + '/notification/retry');
assert.deepEqual(JSON.parse(mutation.body), {expected_revision:4});
for (const notification of [
 {...review.notification, attempts:3},
 {...review.notification, retry_at:'2999-01-01T00:00:00+00:00'},
 {...review.notification, status:'sending'},
]) {
 listedReviews = [{...review, notification}]; await loadTokenReviews();
 assert.ok(!button('Retry email') || button('Retry email').disabled);
}
window.location.hash = '#token-review=' + review.request_id + '/start';
await loadTokenReviews();
assert.equal(requests.filter(item => item.method === 'POST').length, 1);
""")

    def test_stale_revision_and_start_failure_reload_without_replay(self):
        self.run_behavior("admin.js", r"""
await showWorkspace(session);
postStatus=409; postResult={error:'Review changed', error_code:'stale_revision'};
await button('Approve').click();
assert.equal(requests.filter(item => item.method === 'POST').length, 1);
assert.match(element('token-review-status').textContent, /changed.*review|review.*again/i);
listedReviews=[{...review, status:'approved', revision:5}]; await loadTokenReviews();
postStatus=503; postResult={...listedReviews[0], revision:7, onboarding_error_code:'onboarding_start_failed'};
listedReviews=[postResult];
await button('Start onboarding').click();
assert.equal(requests.filter(item => item.method === 'POST').length, 2);
assert.match(element('token-review-status').textContent, /failed/i);
assert.ok(element('token-reviews-body').textContent.includes('onboarding_start_failed'));
assert.ok(button('Start onboarding'));
""")

    def test_review_poll_cannot_replace_newer_action_or_enable_inflight_buttons(self):
        self.run_behavior("admin.js", r"""
await showWorkspace(session);
const original = transport;
let releaseOld;
transport = path => new Promise(resolve => { releaseOld = resolve; });
const oldLoad = loadTokenReviews();
transport = original;
listedReviews=[{...review, revision:6, status:'approved'}]; await loadTokenReviews();
releaseOld({ok:true, status:200, json:async () => ({reviews:[review]})}); await oldLoad;
assert.ok(button('Start onboarding')); assert.ok(!button('Approve'));
let releaseAction;
transport = (path, options) => options.method === 'POST'
 ? new Promise(resolve => { releaseAction = resolve; }) : original(path, options);
const action = button('Start onboarding').click();
await loadTokenReviews();
assert.ok(buttons().every(item => item.disabled));
releaseAction({ok:true, status:200, json:async () => ({...review, revision:8, status:'onboarding_queued'})});
listedReviews=[{...review, revision:8, status:'onboarding_queued'}]; await action;
assert.equal(requests.filter(item => item.method === 'POST').length, 1);
""")

    def test_public_queued_duplicate_receipt_does_not_claim_work_is_still_pending(self):
        self.run_behavior("actions.js", r"""
publicActions.candidate = {identity:review};
postResult = {...review, status:'onboarding_queued', deduplicated:true};
await element('public-token-add').click();
const receipt = element('public-token-status').textContent;
assert.ok(receipt.includes('onboarding_queued'));
assert.ok(!receipt.includes('collection has not started for this review'));
assert.equal(publicActions.jobs.size, 0);
""")

    def test_only_recovery_jobs_remain_in_public_polling(self):
        self.run_behavior("actions.js", r"""
rememberJob({job_id:'a'.repeat(32), job_type:'token_onboarding', status:'queued'});
assert.equal(publicActions.jobs.size, 0);
rememberJob({job_id:'b'.repeat(32), job_type:'retry_failed', status:'queued'});
assert.equal(publicActions.jobs.size, 1);
publicActions.jobs.set('a'.repeat(32), {job_id:'a'.repeat(32), status:'unknown'});
transport = async path => ({ok:true, status:200, json:async () => ({
 job_id:path.split('/').pop(), job_type:path.includes('a'.repeat(32)) ? 'token_onboarding' : 'retry_failed',
 status:'queued',
})});
await refreshJobs();
assert.equal(publicActions.jobs.size, 1);
assert.ok(publicActions.jobs.has('b'.repeat(32)));
const calls = requests.length;
await refreshJobs();
assert.equal(requests.length, calls + 1);
""")

    def test_admin_invalid_action_ids_revisions_and_suffixes_never_fetch(self):
        self.run_behavior("admin.js", r"""
admin.session=session;
for (const request_id of ['../x', 'A'.repeat(32), 'a'.repeat(31), 'https://evil.test']) {
 await actOnTokenReview({...review, request_id}, 'approve');
}
for (const revision of [0, -1, true, '4', 1.2]) {
 await actOnTokenReview({...review, revision}, 'approve');
}
await actOnTokenReview(review, 'https://evil.test');
assert.equal(requests.length, 0);
listedReviews=[{...review, revision:'4'}]; await showWorkspace(session);
assert.equal(buttons().length, 0);
""")

    def test_active_token_cannot_be_submitted_from_either_preview(self):
        self.run_behavior("actions.js", r"""
renderTokenCandidate({identity:review, already_configured:true});
assert.equal(element('public-token-add').disabled, true);
await element('public-token-add').click(); assert.equal(requests.length, 0);
""")
        self.run_behavior("admin.js", r"""
admin.session=session;
postResult={identity:review, already_configured:true, registration:{origin:'static', status:'active'}};
await submit('token-resolve-form');
assert.equal(element('add-token-button').disabled, true);
await element('add-token-button').click();
assert.ok(requests.every(item => item.path !== '/api/admin/tokens'));
""")


if __name__ == "__main__":
    unittest.main()
