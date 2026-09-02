"use strict";

const crypto = require("crypto");
const vm = require("vm");

const MAX_INPUT_BYTES = 16 * 1024 * 1024;
const MAX_MEMBER_BYTES = 4 * 1024 * 1024;
const DISCLAIMER = "Historical Foundry Replay. Fixed-block counterfactual simulation under a hash-bound state override modelling a prefunded, predeployed, preapproved executor. Successful values are research estimates at the displayed Ethereum block; they are not current and are not executable candidates.";
const VOID_TAGS = new Set(["area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"]);

function fail() { process.exitCode = 1; }
function sha256(value) { return crypto.createHash("sha256").update(value).digest("hex"); }

function exactBase64(value, expectedSha) {
  if (typeof value !== "string" || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) throw new Error("base64");
  const decoded = Buffer.from(value, "base64");
  if (decoded.length === 0 || decoded.length > MAX_MEMBER_BYTES) throw new Error("size");
  if (decoded.toString("base64") !== value || sha256(decoded) !== expectedSha) throw new Error("hash");
  return decoded;
}

function datasetName(name) {
  return name.slice(5).replace(/-([a-z])/g, (_match, letter) => letter.toUpperCase());
}

function decodeEntities(text) {
  return text
    .replaceAll("&amp;", "&")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replaceAll("&quot;", '"')
    .replaceAll("&#39;", "'");
}

function parseAttributes(source) {
  const values = Object.create(null);
  let offset = 0;
  while (offset < source.length) {
    while (offset < source.length && /\s/.test(source[offset])) offset += 1;
    if (offset >= source.length || source[offset] === "/") break;
    const start = offset;
    while (offset < source.length && !/[\s=/>]/.test(source[offset])) offset += 1;
    if (offset === start) throw new Error("attribute syntax");
    const name = source.slice(start, offset).toLowerCase();
    if (Object.prototype.hasOwnProperty.call(values, name)) throw new Error("duplicate attribute");
    while (offset < source.length && /\s/.test(source[offset])) offset += 1;
    let value = "";
    if (source[offset] === "=") {
      offset += 1;
      while (offset < source.length && /\s/.test(source[offset])) offset += 1;
      const quote = source[offset];
      if (quote === '"' || quote === "'") {
        offset += 1;
        const stop = source.indexOf(quote, offset);
        if (stop < 0) throw new Error("attribute quote");
        value = source.slice(offset, stop);
        offset = stop + 1;
      } else {
        const valueStart = offset;
        while (offset < source.length && !/[\s>]/.test(source[offset])) offset += 1;
        if (offset === valueStart) throw new Error("attribute value");
        value = source.slice(valueStart, offset);
      }
    }
    values[name] = decodeEntities(value);
  }
  return values;
}

function selectorMatches(element, selector) {
  let remaining = selector.trim();
  if (!remaining || remaining.includes(" ") || remaining.includes(">")) return false;
  let tag = "";
  let index = 0;
  while (index < remaining.length && /[A-Za-z0-9_-]/.test(remaining[index])) index += 1;
  if (index) {
    tag = remaining.slice(0, index).toLowerCase();
    remaining = remaining.slice(index);
    if (element.tagName.toLowerCase() !== tag) return false;
  }
  while (remaining) {
    if (remaining[0] === "#") {
      const stop = remaining.search(/[.[]/);
      const end = stop <= 0 ? remaining.length : stop;
      if (element.id !== remaining.slice(1, end)) return false;
      remaining = remaining.slice(end);
    } else if (remaining[0] === ".") {
      const stop = remaining.slice(1).search(/[.[]/);
      const end = stop < 0 ? remaining.length : stop + 1;
      if (!element.classList.contains(remaining.slice(1, end))) return false;
      remaining = remaining.slice(end);
    } else if (remaining[0] === "[") {
      const end = remaining.indexOf("]");
      if (end < 0) return false;
      const expression = remaining.slice(1, end);
      const equals = expression.indexOf("=");
      const name = (equals < 0 ? expression : expression.slice(0, equals)).trim().toLowerCase();
      if (!Object.prototype.hasOwnProperty.call(element.attributes, name)) return false;
      if (equals >= 0) {
        let expected = expression.slice(equals + 1).trim();
        if ((expected[0] === '"' && expected.at(-1) === '"') || (expected[0] === "'" && expected.at(-1) === "'")) expected = expected.slice(1, -1);
        if (element.getAttribute(name) !== expected) return false;
      }
      remaining = remaining.slice(end + 1);
    } else {
      return false;
    }
  }
  return true;
}

class Element {
  constructor(tag, attrs = Object.create(null)) {
    this.tagName = tag.toUpperCase();
    this.attributes = attrs;
    this.children = [];
    this.parentElement = null;
    this.hidden = Object.prototype.hasOwnProperty.call(attrs, "hidden");
    this.dataset = Object.create(null);
    for (const [name, value] of Object.entries(attrs)) {
      if (name.startsWith("data-")) this.dataset[datasetName(name)] = value;
    }
    this.className = attrs.class || "";
    this.id = attrs.id || "";
    this.value = attrs.value || "";
    this.disabled = Object.prototype.hasOwnProperty.call(attrs, "disabled");
    this.checked = Object.prototype.hasOwnProperty.call(attrs, "checked");
    this._text = "";
    this._innerHTML = "";
    this.listeners = Object.create(null);
    this.style = { setProperty() {}, removeProperty() {} };
    this.classList = {
      contains: (name) => this.className.split(/\s+/).filter(Boolean).includes(name),
      toggle: (name, active) => {
        const values = new Set(this.className.split(/\s+/).filter(Boolean));
        if (active) values.add(name); else values.delete(name);
        this.className = [...values].join(" ");
        this.attributes.class = this.className;
      },
      add: (...names) => names.forEach((name) => this.classList.toggle(name, true)),
      remove: (...names) => names.forEach((name) => this.classList.toggle(name, false)),
    };
  }

  get textContent() { return this._text + this.children.map((child) => child.textContent).join(""); }
  set textContent(value) { this._text = String(value); this.children = []; this._innerHTML = ""; }
  get innerHTML() { return this._innerHTML; }
  set innerHTML(value) { this._innerHTML = String(value); this.children = []; this._text = ""; }
  get options() { return this.children.filter((child) => child.tagName === "OPTION"); }
  get firstElementChild() { return this.children[0] || null; }

  setAttribute(name, value) {
    const normalized = String(value);
    this.attributes[name] = normalized;
    if (name === "id") this.id = normalized;
    if (name === "class") this.className = normalized;
    if (name.startsWith("data-")) this.dataset[datasetName(name)] = normalized;
  }
  getAttribute(name) { return this.attributes[name] ?? null; }
  hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attributes, name); }
  removeAttribute(name) { delete this.attributes[name]; }
  addEventListener(name, callback) { (this.listeners[name] ||= []).push(callback); }
  appendChild(child) { child.parentElement = this; this.children.push(child); return child; }
  replaceChildren(...children) { this.children = []; children.forEach((child) => this.appendChild(child)); }
  remove() { if (this.parentElement) this.parentElement.children = this.parentElement.children.filter((child) => child !== this); }
  focus() {}
  click() {}
  getBoundingClientRect() { return { left: 0, top: 0, right: 0, bottom: 0, width: 0, height: 0 }; }
  querySelectorAll(selector) { return selectAll(this.children, selector); }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  closest(selector) { let current = this; while (current) { if (selectorMatches(current, selector)) return current; current = current.parentElement; } return null; }
}

function allDescendants(roots) {
  const result = [];
  const visit = (element) => { result.push(element); element.children.forEach(visit); };
  roots.forEach(visit);
  return result;
}

function selectAll(roots, selector) {
  const alternatives = selector.split(",").map((value) => value.trim()).filter(Boolean);
  return allDescendants(roots).filter((element) => alternatives.some((value) => selectorMatches(element, value)));
}

function findTagEnd(html, start) {
  let quote = null;
  for (let index = start; index < html.length; index += 1) {
    const value = html[index];
    if (quote !== null) { if (value === quote) quote = null; }
    else if (value === '"' || value === "'") quote = value;
    else if (value === ">") return index;
  }
  throw new Error("unterminated tag");
}

function parseHtml(html) {
  const root = new Element("document");
  const stack = [root];
  const ids = new Map();
  let offset = 0;
  while (offset < html.length) {
    if (html.startsWith("<!--", offset)) {
      const end = html.indexOf("-->", offset + 4);
      if (end < 0) throw new Error("unterminated comment");
      offset = end + 3;
      continue;
    }
    if (html[offset] !== "<") {
      const end = html.indexOf("<", offset);
      const stop = end < 0 ? html.length : end;
      stack.at(-1)._text += decodeEntities(html.slice(offset, stop));
      offset = stop;
      continue;
    }
    if (html.startsWith("<!", offset) || html.startsWith("<?", offset)) {
      offset = findTagEnd(html, offset + 2) + 1;
      continue;
    }
    const closing = html.startsWith("</", offset);
    const end = findTagEnd(html, offset + (closing ? 2 : 1));
    const contents = html.slice(offset + (closing ? 2 : 1), end).trim();
    if (closing) {
      const tag = contents.toLowerCase();
      let match = stack.length - 1;
      while (match > 0 && stack[match].tagName.toLowerCase() !== tag) match -= 1;
      if (match > 0) stack.length = match;
      offset = end + 1;
      continue;
    }
    let nameEnd = 0;
    while (nameEnd < contents.length && /[A-Za-z0-9:-]/.test(contents[nameEnd])) nameEnd += 1;
    if (nameEnd === 0) throw new Error("tag name");
    const tag = contents.slice(0, nameEnd).toLowerCase();
    const element = new Element(tag, parseAttributes(contents.slice(nameEnd)));
    stack.at(-1).appendChild(element);
    if (element.id) {
      if (ids.has(element.id)) throw new Error("duplicate id");
      ids.set(element.id, element);
    }
    const selfClosing = contents.endsWith("/") || VOID_TAGS.has(tag);
    if (!selfClosing) stack.push(element);
    offset = end + 1;
  }
  const elements = allDescendants(root.children);
  const body = elements.find((element) => element.tagName === "BODY") || root;
  return {
    body,
    documentElement: elements.find((element) => element.tagName === "HTML") || root,
    title: "",
    activeElement: null,
    getElementById: (id) => ids.get(id) || null,
    querySelectorAll: (selector) => selectAll(root.children, selector),
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; },
    createElement: (tag) => new Element(tag),
    createElementNS: (_namespace, tag) => new Element(tag),
    addEventListener() {},
    _ids: ids,
    _elements: elements,
  };
}

function rowProjection(markup, generation, replayId, blockNumber) {
  const fragment = parseHtml(`<table><tbody>${markup}</tbody></table>`);
  const rows = fragment.querySelectorAll("tr");
  const required = [
    "data-opportunity-id", "data-api-generation", "data-replay-id",
    "data-block-number", "data-direction", "data-notional-usd",
    "data-foundry-verified", "data-policy-net-edge-usd",
    "data-research-net-edge-usd", "data-receipt-sha256", "data-trace-sha256",
  ];
  return rows.map((row) => {
    if (Object.keys(row.attributes).length !== required.length || required.some((name) => !row.hasAttribute(name))) throw new Error("row attributes");
    if (row.getAttribute("data-api-generation") !== generation || row.getAttribute("data-replay-id") !== replayId || row.getAttribute("data-block-number") !== blockNumber) throw new Error("row identity");
    return {
      opportunity_id: row.getAttribute("data-opportunity-id"),
      direction: row.getAttribute("data-direction"),
      notional_usd: row.getAttribute("data-notional-usd"),
      foundry_verified: row.getAttribute("data-foundry-verified") === "true",
      policy_net_edge_usd: row.getAttribute("data-policy-net-edge-usd"),
      research_net_edge_usd: row.getAttribute("data-research-net-edge-usd"),
      receipt_sha256: row.getAttribute("data-receipt-sha256"),
      trace_sha256: row.getAttribute("data-trace-sha256"),
    };
  });
}

async function main() {
  const chunks = [];
  let size = 0;
  for await (const chunk of process.stdin) {
    size += chunk.length;
    if (size > MAX_INPUT_BYTES) throw new Error("input size");
    chunks.push(chunk);
  }
  const input = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  const expectedKeys = ["api_payload", "app_js_base64", "app_js_sha256", "application_sha", "asset_sha", "data_generation", "historical_html_base64", "historical_html_sha256", "navigation_js_base64", "navigation_js_sha256"];
  if (JSON.stringify(Object.keys(input).sort()) !== JSON.stringify(expectedKeys)) throw new Error("keys");
  const htmlBytes = exactBase64(input.historical_html_base64, input.historical_html_sha256);
  const navigationBytes = exactBase64(input.navigation_js_base64, input.navigation_js_sha256);
  const appBytes = exactBase64(input.app_js_base64, input.app_js_sha256);
  const html = htmlBytes.toString("utf8");
  if (Buffer.from(html, "utf8").compare(htmlBytes) !== 0) throw new Error("utf8");
  const document = parseHtml(html);
  const requiredIds = ["opportunity-current-context", "opportunity-historical-context", "opportunities-view", "historical-opportunity-inventory", "historical-opportunity-title", "historical-opportunity-count", "historical-opportunity-empty", "historical-opportunity-body", "strict-opportunities", "strict-opportunity-body", "estimate-opportunities", "estimate-opportunity-body", "unavailable-opportunities", "unavailable-opportunity-body"];
  if (requiredIds.some((id) => !document.getElementById(id))) throw new Error("missing DOM hook");
  const scopes = document.querySelectorAll("[data-opportunity-scope]");
  if (scopes.length !== 2 || html.split(DISCLAIMER).length - 1 !== 1) throw new Error("historical shell");
  const assetVersion = `${input.application_sha.slice(0, 12)}-${input.asset_sha.slice(0, 12)}`;
  const servedAssets = document._elements
    .filter((element) => ["LINK", "SCRIPT"].includes(element.tagName))
    .map((element) => element.getAttribute(element.tagName === "LINK" ? "href" : "src"))
    .filter((value) => value && ["/styles.css", "/vendor/lucide.js", "/navigation.js", "/app.js"].some((path) => value.startsWith(path)));
  const expectedAssets = ["/styles.css", "/vendor/lucide.js", "/navigation.js", "/app.js"].map((path) => `${path}?v=${assetVersion}`);
  if (JSON.stringify(servedAssets) !== JSON.stringify(expectedAssets)) throw new Error("assets");

  let fetchCount = 0;
  const storage = { getItem() { return null; }, setItem() {}, removeItem() {} };
  const location = { pathname: "/opportunities", search: "?opportunity_scope=historical", hash: "", href: "/opportunities?opportunity_scope=historical" };
  const context = {
    document, location,
    console: { log() {}, warn() {}, error() {} },
    URL, URLSearchParams, Intl, Date, Math, JSON, Object, Array, String, Number,
    Boolean, RegExp, Set, Map, Promise, AbortController,
    setTimeout, clearTimeout, queueMicrotask,
    requestAnimationFrame: (callback) => { callback(0); return 1; },
    cancelAnimationFrame() {},
    fetch: async (url) => {
      fetchCount += 1;
      if (fetchCount !== 1 || String(url) !== "/api/markets/opportunities/historical?class=all&route_type=all&availability=all&sort=net_edge_usd&dir=desc") throw new Error("fetch");
      return { ok: true, status: 200, json: async () => input.api_payload };
    },
  };
  context.window = context;
  context.globalThis = context;
  context.window.location = location;
  context.window.history = { pushState() {}, replaceState() {} };
  context.window.localStorage = storage;
  context.window.sessionStorage = storage;
  context.window.lucide = { createIcons() {} };
  context.window.addEventListener = () => {};
  context.window.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  context.window.visualViewport = null;
  context.navigator = { clipboard: { async writeText() {} } };
  context.CSS = { escape: (value) => String(value) };
  vm.createContext(context);
  vm.runInContext(navigationBytes.toString("utf8"), context, { filename: "navigation.js" });
  const bootstrap = vm.runInContext(appBytes.toString("utf8"), context, { filename: "app.js" });
  if (!bootstrap || typeof bootstrap.then !== "function") throw new Error("bootstrap missing");
  await bootstrap;
  if (fetchCount !== 1) throw new Error("fetch count");

  const root = document.getElementById("historical-opportunity-inventory");
  const rows = rowProjection(document.getElementById("historical-opportunity-body").innerHTML, root.getAttribute("data-api-generation"), root.getAttribute("data-replay-id"), root.getAttribute("data-selected-block-number"));
  const strictHidden = ["strict-opportunities", "estimate-opportunities", "unavailable-opportunities"].every((id) => document.getElementById(id).hidden === true);
  const currentScope = scopes.find((element) => element.dataset.opportunityScope === "current");
  const historicalScope = scopes.find((element) => element.dataset.opportunityScope === "historical");
  if (root.hidden !== false || document.getElementById("opportunity-current-context").hidden !== true || document.getElementById("opportunity-historical-context").hidden !== false || !currentScope || currentScope.getAttribute("aria-pressed") !== "false" || !historicalScope || historicalScope.getAttribute("aria-pressed") !== "true") throw new Error("historical visibility");
  const surfaceBytes = Buffer.from(JSON.stringify({ api_data_generation: input.data_generation, application_sha: input.application_sha, asset_sha: input.asset_sha, html_sha256: input.historical_html_sha256 }));
  process.stdout.write(JSON.stringify({
    application_sha: input.application_sha,
    asset_sha: input.asset_sha,
    html_sha256: input.historical_html_sha256,
    surface_binding_sha256: sha256(Buffer.concat([Buffer.from("historical_opportunity_surface_binding/v1\0"), surfaceBytes])),
    data_generation: root.getAttribute("data-api-generation"),
    replay_id: root.getAttribute("data-replay-id"),
    selected_block_number: Number(root.getAttribute("data-selected-block-number")),
    scenario_count: Number(root.getAttribute("data-scenario-count")),
    strict_hidden: strictHidden,
    disclaimer: DISCLAIMER,
    rows,
  }));
}

main().catch(fail);
