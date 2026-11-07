// Node 4 search UI - vanilla JS, talks to the FastAPI backend on the same origin.
(function () {
  "use strict";
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
  const PAGE_SIZE = 10;
  const state = { q: "", page: 1, version: "", versions: [], active: null };

  // ---------------------------------------------------------------- helpers
  function el(tag, attrs = {}, ...kids) {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (v == null) continue;
      if (k === "class") n.className = v;
      else if (k === "text") n.textContent = v;
      else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
      else n.setAttribute(k, v);
    }
    for (const k of kids.flat()) if (k != null) n.append(k.nodeType ? k : document.createTextNode(String(k)));
    return n;
  }
  async function api(path, opts = {}) {
    const r = await fetch(path, opts);
    let body = null;
    try { body = await r.json(); } catch (_) { /* empty */ }
    if (!r.ok) throw new Error((body && body.detail) || r.statusText);
    return body;
  }
  const fmtPR = (x) => (x == null ? "-" : Number(x).toExponential(3));
  const fmt = (x, d = 3) => (x == null ? "-" : Number(x).toFixed(d));
  function adminHeaders() {
    const t = $("#adminToken").value.trim();
    return t ? { "X-Admin-Token": t } : {};
  }

  // ---------------------------------------------------------------- tabs
  function showTab(name) {
    $$(".tab").forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
    $$(".panel").forEach((p) => p.classList.toggle("active", p.id === "tab-" + name));
    if (name === "index") loadIndexTab();
    history.replaceState(null, "", name === "search" ? location.pathname + location.search : "#" + name);
  }
  $$(".tab").forEach((b) => b.addEventListener("click", () => showTab(b.dataset.tab)));

  // ---------------------------------------------------------------- versions
  async function refreshVersions() {
    try {
      const v = await api("/api/versions");
      state.versions = v.versions.filter((x) => x.complete);
      state.active = v.active;
      const badge = $("#versionBadge");
      const act = state.versions.find((x) => x.version === v.active);
      if (v.active) {
        badge.textContent = `${v.active} · phase ${act && act.phase != null ? act.phase : "?"} · ${act ? act.docs : "?"} docs`;
        badge.className = "badge ok";
      } else {
        badge.textContent = "no data yet - waiting for an export";
        badge.className = "badge warn";
      }
      fillVersionSelects();
    } catch (e) {
      $("#versionBadge").textContent = "API unreachable";
      $("#versionBadge").className = "badge warn";
    }
  }
  function fillVersionSelects() {
    const opts = (sel, value) => {
      sel.innerHTML = "";
      for (const v of state.versions) {
        sel.append(el("option", { value: v.version, text: `${v.version} (phase ${v.phase ?? "?"})${v.active ? " ★" : ""}` }));
      }
      if (value) sel.value = value;
    };
    const vs = $("#versionSelect");
    const cur = state.version;
    vs.innerHTML = "";
    vs.append(el("option", { value: "", text: "active version" }));
    for (const v of state.versions) vs.append(el("option", { value: v.version, text: `${v.version} · phase ${v.phase ?? "?"}` }));
    vs.value = cur;
    const n = state.versions.length;
    opts($("#cmpA"), n >= 2 ? state.versions[n - 2].version : state.versions[0] && state.versions[0].version);
    opts($("#cmpB"), n ? state.versions[n - 1].version : "");
  }

  // ---------------------------------------------------------------- search
  function snippetNode(parts, fallback) {
    const p = el("p", { class: "snippet" });
    if (!parts || !parts.length) { p.textContent = fallback || ""; return p; }
    for (const [text, hit] of parts) p.append(hit ? el("mark", { text }) : document.createTextNode(text));
    return p;
  }
  function resultNode(r) {
    const bmW = 70 * r.bm25_norm, prW = 30 * r.pagerank_norm;
    return el("li", { class: "result" },
      el("div", { class: "top" },
        el("span", { class: "rank", text: r.rank + "." }),
        el("h3", {}, el("a", { href: r.url, target: "_blank", rel: "noopener", text: r.title }))),
      el("div", { class: "url", text: r.url }),
      snippetNode(r.snippet_parts, r.snippet),
      el("div", { class: "foot" },
        el("span", { class: "scorebar", title: `0.7·BM25norm=${fmt(0.7 * r.bm25_norm)}  +  0.3·PRnorm=${fmt(0.3 * r.pagerank_norm)}` },
          el("span", { class: "b", style: `width:${bmW}%` }), el("span", { class: "p", style: `width:${prW}%` })),
        el("strong", { text: "score " + fmt(r.score) }),
        el("span", { class: "chip b", text: "BM25 " + fmt(r.bm25, 2) }),
        el("span", { class: "chip p", text: "PR " + fmtPR(r.pagerank) }),
        el("span", { class: "chip", text: "id " + r.doc_id }),
        el("span", { text: "terms: " + r.matched_terms.join(", ") }),
        el("button", { class: "link", type: "button", onclick: () => openDoc(r.doc_id), text: "details" })));
  }
  async function runSearch(page = 1) {
    const q = $("#q").value.trim();
    if (!q) return;
    state.q = q; state.page = page; state.version = $("#versionSelect").value;
    const params = new URLSearchParams({ q, k: PAGE_SIZE, page });
    if (state.version) params.set("version", state.version);
    history.replaceState(null, "", "?" + params.toString());
    $("#meta").textContent = "Searching…";
    try {
      const r = await api("/api/search?" + params);
      renderResults(r);
    } catch (e) {
      $("#meta").textContent = "";
      $("#results").replaceChildren(el("li", { class: "empty", text: "⚠ " + e.message }));
      $("#pager").replaceChildren();
    }
  }
  function renderResults(r) {
    const ol = $("#results");
    ol.replaceChildren();
    const unknown = r.unknown_terms && r.unknown_terms.length ? ` · not in index: ${r.unknown_terms.join(", ")}` : "";
    $("#meta").textContent = r.total
      ? `${r.total} matching documents · ${r.took_ms} ms · ${r.version} (phase ${r.phase ?? "?"}) · terms: ${r.terms.join(", ")}${unknown}`
      : "";
    if (!r.total) {
      ol.append(el("li", { class: "empty", text: r.terms.length ? `No documents match “${r.query}”.${unknown}` : "Query has only stop-words - try more specific words." }));
      $("#pager").replaceChildren();
      return;
    }
    r.results.forEach((x) => ol.append(resultNode(x)));
    renderPager(r.total);
  }
  function renderPager(total) {
    const pages = Math.ceil(total / PAGE_SIZE);
    const pg = $("#pager");
    pg.replaceChildren();
    if (pages <= 1) return;
    const from = Math.max(1, state.page - 4), to = Math.min(pages, from + 8);
    if (state.page > 1) pg.append(el("button", { text: "‹", onclick: () => runSearch(state.page - 1) }));
    for (let i = from; i <= to; i++) pg.append(el("button", { class: i === state.page ? "cur" : null, text: i, onclick: () => runSearch(i) }));
    if (state.page < pages) pg.append(el("button", { text: "›", onclick: () => runSearch(state.page + 1) }));
  }
  $("#searchForm").addEventListener("submit", (e) => { e.preventDefault(); runSearch(1); });
  $$("#hints a").forEach((a) => a.addEventListener("click", (e) => { e.preventDefault(); $("#q").value = a.dataset.q; runSearch(1); }));

  // ---------------------------------------------------------------- compare
  function movement(r) {
    if (r.movement === "new") return el("span", { class: "mv new", text: r.in_old_corpus ? "NEW in top" : "NEW doc" });
    if (r.movement > 0) return el("span", { class: "mv up", text: "▲ " + r.movement });
    if (r.movement < 0) return el("span", { class: "mv down", text: "▼ " + -r.movement });
    return el("span", { class: "mv", text: "=" });
  }
  async function runCompare() {
    const q = $("#cq").value.trim();
    if (!q) return;
    const a = $("#cmpA").value, b = $("#cmpB").value;
    const out = $("#compareOut");
    out.textContent = "Comparing…";
    try {
      const r = await api("/api/compare?" + new URLSearchParams({ q, a, b, k: 10 }));
      const col = (res, isNew) => el("div", { class: "col" },
        el("h4", { text: `${res.version} · phase ${res.phase ?? "?"} · ${res.total} matches` }),
        res.results.length ? res.results.map((x) => el("div", { class: "crow" },
          el("span", { class: "muted", text: x.rank }),
          el("span", { class: "t", title: x.title },
            el("button", { class: "link", type: "button", onclick: () => openDoc(x.doc_id, res.version), text: x.title })),
          isNew ? movement(x)
            : (x.removed ? el("span", { class: "mv gone", text: "deleted" })
              : el("span", { class: "muted small", text: fmt(x.score) })))) : el("p", { class: "muted", text: "no matches" }));
      out.replaceChildren(col(r.a, false), col(r.b, true));
    } catch (e) {
      out.textContent = "⚠ " + e.message;
    }
  }
  $("#compareForm").addEventListener("submit", (e) => { e.preventDefault(); runCompare(); });

  // ---------------------------------------------------------------- doc drawer
  async function openDoc(docId, version) {
    const v = version || state.version;
    const body = $("#drawerBody");
    body.textContent = "Loading…";
    $("#drawer").classList.remove("hidden");
    try {
      const d = await api(`/api/doc/${encodeURIComponent(docId)}` + (v ? `?version=${v}` : ""));
      const chips = (arr) => arr.length
        ? el("div", { class: "linklist" }, arr.map((x) => el("button", { type: "button", onclick: () => openDoc(x.doc_id, v), text: x.title })))
        : el("p", { class: "muted small", text: "none" });
      body.replaceChildren(
        el("h2", { id: "drawerTitle", text: d.title }),
        el("a", { href: d.url, target: "_blank", rel: "noopener", text: d.url }),
        el("dl", { class: "stats", style: "margin:14px 0" },
          el("dt", { text: "doc_id" }), el("dd", { text: d.doc_id }),
          el("dt", { text: "PageRank" }), el("dd", { text: fmtPR(d.pagerank) }),
          el("dt", { text: "length (terms)" }), el("dd", { text: d.length }),
          el("dt", { text: "version" }), el("dd", { text: d.version })),
        el("h4", { text: `Links to (${d.links.length})` }), chips(d.links),
        el("h4", { text: `Linked from (${d.backlinks.length})` }), chips(d.backlinks),
        el("h4", { text: "Text (cleaned)" }), el("div", { class: "body", text: d.body }));
    } catch (e) {
      body.textContent = "⚠ " + e.message;
    }
  }
  $("#drawerClose").addEventListener("click", () => $("#drawer").classList.add("hidden"));
  $("#drawer").addEventListener("click", (e) => { if (e.target.id === "drawer") $("#drawer").classList.add("hidden"); });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") $("#drawer").classList.add("hidden"); });

  // ---------------------------------------------------------------- index tab
  async function loadIndexTab() {
    await refreshVersions();
    const tb = $("#versionsTbl tbody");
    tb.replaceChildren();
    for (const v of state.versions.slice().reverse()) {
      tb.append(el("tr", { class: v.active ? "active" : null },
        el("td", {}, el("strong", { text: v.version })),
        el("td", { text: v.phase ?? "?" }),
        el("td", { text: v.docs ?? "?" }),
        el("td", { class: "small", text: (v.created_at || "").replace("T", " ") }),
        el("td", {}, v.active ? el("span", { class: "muted small", text: "active" })
          : el("button", { class: "link", type: "button", onclick: () => activate(v.version), text: "activate" }))));
    }
    if (!state.versions.length) tb.append(el("tr", {}, el("td", { colspan: 5, class: "muted", text: "no versions pulled yet" })));
    try {
      const s = await api("/api/stats");
      const m = s.manifest || {};
      const rows = [
        ["version", s.version], ["phase", m.phase], ["documents", s.docs], ["terms", s.terms],
        ["postings", s.postings], ["avg doc length", s.avgdl], ["links (edges)", m.counts && m.counts.edges],
        ["PageRank iterations", m.pagerank && m.pagerank.iterations],
        ["PageRank final Δ", m.pagerank && m.pagerank.final_delta],
        ["BM25", `k1=${s.ranking.k1}, b=${s.ranking.b}`],
        ["weights", `${s.ranking.w_bm25} BM25 + ${s.ranking.w_pagerank} PR (${s.ranking.pr_norm})`],
        ["loaded", `${s.loaded_at} (${s.load_seconds}s)`]];
      $("#statsList").replaceChildren(...rows.flatMap(([k, v]) => [el("dt", { text: k }), el("dd", { text: v ?? "-" })]));
      const pt = $("#prTbl tbody");
      pt.replaceChildren(...s.top_pagerank.map((d, i) => el("tr", {},
        el("td", { text: i + 1 }),
        el("td", {}, el("button", { class: "link", type: "button", onclick: () => openDoc(d.doc_id), text: d.title })),
        el("td", { text: fmtPR(d.pagerank) }), el("td", { text: d.in_degree }))));
      const lin = $("#lineage");
      lin.replaceChildren(
        el("dl", { class: "stats" },
          el("dt", { text: "producer" }), el("dd", { text: m.producer }),
          el("dt", { text: "created" }), el("dd", { text: m.created_at }),
          el("dt", { text: "pages snapshot" }), el("dd", { text: m.pages_snapshot_id }),
          el("dt", { text: "postings snapshot" }), el("dd", { text: m.postings_snapshot_id }),
          el("dt", { text: "pagerank snapshot" }), el("dd", { text: m.pagerank_snapshot_id })),
        el("h4", { text: "Validation checks" }),
        ...(m.validation || []).map((c) => el("div", { class: "check" },
          el("span", { class: c.passed ? "ok" : "no", text: c.passed ? "✓ " : "✗ " }), `${c.check} `,
          el("span", { class: "muted", text: c.detail }))));
    } catch (e) {
      $("#statsList").replaceChildren(el("dd", { class: "muted", text: e.message }));
    }
  }
  async function activate(version) {
    try {
      await api("/api/admin/activate?version=" + encodeURIComponent(version), { method: "POST", headers: adminHeaders() });
      await loadIndexTab();
      if (state.q) runSearch(state.page);
    } catch (e) { alert("Activate failed: " + e.message); }
  }
  $("#pullBtn").addEventListener("click", async () => {
    const btn = $("#pullBtn"), out = $("#pullOut");
    btn.disabled = true; out.classList.remove("hidden"); out.textContent = "Pulling from HDFS…";
    try {
      const r = await api("/api/admin/pull", { method: "POST", headers: adminHeaders() });
      out.textContent = JSON.stringify(r, null, 2);
      await loadIndexTab();
    } catch (e) { out.textContent = "⚠ " + e.message; }
    btn.disabled = false;
  });

  // ---------------------------------------------------------------- boot
  (async function boot() {
    await refreshVersions();
    setInterval(refreshVersions, 15000);
    const p = new URLSearchParams(location.search);
    if (p.get("version")) { state.version = p.get("version"); $("#versionSelect").value = state.version; }
    if (p.get("q")) { $("#q").value = p.get("q"); runSearch(Number(p.get("page")) || 1); }
    if (location.hash === "#compare") showTab("compare");
    if (location.hash === "#index") showTab("index");
  })();
})();
