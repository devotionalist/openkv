"use strict";
(() => {
  // src/api.ts
  async function api(path, opts) {
    const res = await fetch(path, opts);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.error || res.statusText);
    }
    return data;
  }
  function postJSON(path, body) {
    return api(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
  }

  // src/dom.ts
  function $(sel) {
    const el = document.querySelector(sel);
    if (!el) throw new Error(`missing element: ${sel}`);
    return el;
  }
  function escapeHtml(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }
  function setTab(name) {
    document.querySelectorAll(".tabs button").forEach((b) => {
      b.classList.toggle("active", b.dataset.tab === name);
    });
    document.querySelectorAll(".panel").forEach((p) => {
      p.classList.toggle("active", p.id === `panel-${name}`);
    });
  }
  function bindForm(id, handler) {
    $(id).addEventListener("submit", async (e) => {
      e.preventDefault();
      const form = e.target;
      const fd = new FormData(form);
      const sub = e.submitter;
      if (sub?.name && sub.value != null && sub.value !== "") {
        fd.set(sub.name, sub.value);
      }
      try {
        await handler(fd, form);
      } catch (err) {
        alert(err instanceof Error ? err.message : String(err));
      }
    });
  }

  // src/drag.ts
  var POS_KEY = "skv-topo-pos-v1";
  var DRAG_THRESHOLD = 5;
  function loadPositions() {
    try {
      const raw = localStorage.getItem(POS_KEY);
      if (!raw) return {};
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
      return {};
    }
  }
  function savePositions(map) {
    try {
      localStorage.setItem(POS_KEY, JSON.stringify(map));
    } catch {
    }
  }
  function clearPositions() {
    try {
      localStorage.removeItem(POS_KEY);
    } catch {
    }
  }
  function defaultPosition(name, role, indexInRole) {
    if (name === "client" || role === "client") return { x: 28, y: 56 };
    if (role === "gateway") return { x: 220, y: 56 };
    if (role === "storage") {
      const col = indexInRole % 3;
      const row = Math.floor(indexInRole / 3);
      return { x: 440 + col * 200, y: 28 + row * 168 };
    }
    if (role === "meta") {
      return { x: 200 + indexInRole * 210, y: 380 };
    }
    return { x: 40 + indexInRole * 40, y: 40 };
  }
  function placeNode(el, pos) {
    el.style.left = `${Math.max(0, pos.x)}px`;
    el.style.top = `${Math.max(0, pos.y)}px`;
  }
  function expandBoard(board) {
    let maxX = 900;
    let maxY = 480;
    board.querySelectorAll(".topo-node.draggable").forEach((el) => {
      const x = parseFloat(el.style.left || "0") || 0;
      const y = parseFloat(el.style.top || "0") || 0;
      maxX = Math.max(maxX, x + el.offsetWidth + 40);
      maxY = Math.max(maxY, y + el.offsetHeight + 40);
    });
    board.style.minWidth = `${maxX}px`;
    board.style.minHeight = `${maxY}px`;
  }
  function enableNodeDrag(board, onTap, onMove) {
    let posMap = loadPositions();
    board.querySelectorAll(".topo-node.draggable").forEach((el) => {
      const name = el.dataset.name;
      if (!name || name.startsWith("_empty")) return;
      let dragging = false;
      let moved = false;
      let startX = 0;
      let startY = 0;
      let origX = 0;
      let origY = 0;
      let pointerId = -1;
      const onPointerDown = (e) => {
        if (e.button !== 0) return;
        e.preventDefault();
        dragging = true;
        moved = false;
        pointerId = e.pointerId;
        el.setPointerCapture(pointerId);
        startX = e.clientX;
        startY = e.clientY;
        origX = parseFloat(el.style.left || "0") || 0;
        origY = parseFloat(el.style.top || "0") || 0;
        el.classList.add("dragging");
        el.style.zIndex = "20";
      };
      const onPointerMove = (e) => {
        if (!dragging || e.pointerId !== pointerId) return;
        const dx = e.clientX - startX;
        const dy = e.clientY - startY;
        if (Math.hypot(dx, dy) > DRAG_THRESHOLD) moved = true;
        const nx = Math.max(0, origX + dx);
        const ny = Math.max(0, origY + dy);
        placeNode(el, { x: nx, y: ny });
        expandBoard(board);
        onMove?.();
      };
      const onPointerUp = (e) => {
        if (!dragging || e.pointerId !== pointerId) return;
        dragging = false;
        el.classList.remove("dragging");
        el.style.zIndex = "";
        try {
          el.releasePointerCapture(pointerId);
        } catch {
        }
        if (moved) {
          posMap = loadPositions();
          posMap[name] = {
            x: parseFloat(el.style.left || "0") || 0,
            y: parseFloat(el.style.top || "0") || 0
          };
          savePositions(posMap);
          onMove?.();
        } else {
          onTap(el);
        }
      };
      el.addEventListener("pointerdown", onPointerDown);
      el.addEventListener("pointermove", onPointerMove);
      el.addEventListener("pointerup", onPointerUp);
      el.addEventListener("pointercancel", onPointerUp);
    });
  }

  // src/wires.ts
  var STROKE = {
    gateway: "rgba(47, 214, 192, 0.85)",
    meta: "rgba(232, 176, 90, 0.9)",
    raft: "rgba(90, 160, 255, 0.85)",
    sync: "rgba(186, 120, 255, 0.85)"
  };
  function formatQps(n) {
    if (n == null || !Number.isFinite(n) || n < 0) return "\u2014";
    if (n < 10) return n.toFixed(1);
    if (n < 1e3) return n.toFixed(0);
    return `${(n / 1e3).toFixed(1)}k`;
  }
  function nodeCenter(board, name) {
    let el = null;
    board.querySelectorAll(".topo-node[data-name]").forEach((n) => {
      if (!el && n.dataset.name === name) el = n;
    });
    if (!el) return null;
    const x = (parseFloat(el.style.left || "0") || 0) + el.offsetWidth / 2;
    const y = (parseFloat(el.style.top || "0") || 0) + el.offsetHeight / 2;
    return { x, y };
  }
  function ensureSvg(board) {
    let svg = board.querySelector(".topo-wires");
    if (svg) return svg;
    svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.classList.add("topo-wires");
    svg.setAttribute("aria-hidden", "true");
    board.insertBefore(svg, board.firstChild);
    return svg;
  }
  function matchFilter(l, filter) {
    if (filter === "all") return true;
    if (filter === "store") {
      return l.from.startsWith("storage") && l.to.startsWith("storage");
    }
    return l.kind === filter;
  }
  function drawBoardWires(board, links, filter = "all") {
    const svg = ensureSvg(board);
    const w = Math.max(board.scrollWidth, board.offsetWidth, 980);
    const h = Math.max(board.scrollHeight, board.offsetHeight, 520);
    svg.setAttribute("width", String(w));
    svg.setAttribute("height", String(h));
    svg.style.width = `${w}px`;
    svg.style.height = `${h}px`;
    const visible = links.filter((l) => (l.count || 0) > 0 || (l.qps || 0) > 0).filter((l) => matchFilter(l, filter)).sort((a, b) => (b.qps || 0) - (a.qps || 0));
    const pairCount = /* @__PURE__ */ new Map();
    const parts = [];
    for (const l of visible.slice(0, 48)) {
      const a = nodeCenter(board, l.from);
      const b = nodeCenter(board, l.to);
      if (!a || !b) continue;
      const pk = [l.from, l.to].sort().join("|");
      const idx = pairCount.get(pk) || 0;
      pairCount.set(pk, idx + 1);
      const dx = b.x - a.x;
      const dy = b.y - a.y;
      const len = Math.hypot(dx, dy) || 1;
      const inset = 36;
      const ax = a.x + dx / len * inset;
      const ay = a.y + dy / len * inset;
      const bx = b.x - dx / len * inset;
      const by = b.y - dy / len * inset;
      const bend = (idx - (pairCount.get(pk) - 1) / 2) * 22;
      const cx = (ax + bx) / 2 - dy / len * bend;
      const cy = (ay + by) / 2 + dx / len * bend;
      const color = STROKE[l.kind] || "rgba(160,180,190,0.7)";
      const sw = Math.min(3.2, 1.2 + Math.log10(1 + (l.qps || 0) + (l.count || 0) / 100));
      const dash = l.kind === "meta" || l.kind === "sync" ? 'stroke-dasharray="6 4"' : "";
      parts.push(`
      <path d="M${ax.toFixed(1)} ${ay.toFixed(1)} Q${cx.toFixed(1)} ${cy.toFixed(1)} ${bx.toFixed(1)} ${by.toFixed(1)}"
        fill="none" stroke="${color}" stroke-width="${sw.toFixed(2)}" ${dash}
        marker-end="url(#wire-arrow-${l.kind})" opacity="0.9"/>
      <g transform="translate(${cx.toFixed(1)},${(cy - 8).toFixed(1)})">
        <rect x="-28" y="-9" width="56" height="16" rx="8" fill="rgba(8,14,20,0.82)" stroke="${color}" stroke-width="0.8"/>
        <text text-anchor="middle" dominant-baseline="middle" y="0.5"
          fill="${color}" font-size="10" font-family="ui-monospace,SFMono-Regular,Menlo,monospace">${formatQps(l.qps)}/s</text>
      </g>
    `);
    }
    svg.innerHTML = `
    <defs>
      ${["gateway", "meta", "raft", "sync"].map(
      (k) => `
        <marker id="wire-arrow-${k}" viewBox="0 0 10 10" refX="8" refY="5"
          markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0 1.5 L9 5 L0 8.5 Z" fill="${STROKE[k]}"/>
        </marker>`
    ).join("")}
    </defs>
    ${parts.join("")}
  `;
  }

  // src/topology.ts
  var WIRE_FILTER_KEY = "skv-topo-wire-filter";
  var LINK_FILTER_KEY = "skv-topo-link-filter";
  var lastTopo;
  var lastOnAction = null;
  var lastOnStoreAction = null;
  var lastSignature = "";
  var wireFilter = loadWireFilter();
  var linkFilter = loadLinkFilter();
  var currentLinks = [];
  var redrawWiresRef = null;
  function loadWireFilter() {
    try {
      const v = localStorage.getItem(WIRE_FILTER_KEY);
      if (v === "gateway" || v === "meta" || v === "raft" || v === "sync" || v === "store" || v === "all") {
        return v;
      }
    } catch {
    }
    return "all";
  }
  function saveWireFilter(v) {
    try {
      localStorage.setItem(WIRE_FILTER_KEY, v);
    } catch {
    }
  }
  function loadLinkFilter() {
    try {
      return localStorage.getItem(LINK_FILTER_KEY) || "all";
    } catch {
      return "all";
    }
  }
  function saveLinkFilter(v) {
    try {
      localStorage.setItem(LINK_FILTER_KEY, v);
    } catch {
    }
  }
  function topoSignature(topo) {
    const names = ["client"];
    for (const g of groupsOf(topo)) {
      for (const n of g.nodes || []) names.push(n.name);
    }
    return names.sort().join("|");
  }
  function groupsOf(topo) {
    return (topo.groups || topo.layers || []).filter((g) => g.nodes?.length);
  }
  function groupByRole(topo, role) {
    return groupsOf(topo).find((g) => g.role === role);
  }
  function flowsOf(topo) {
    return topo.flows || topo.edges || [];
  }
  function flowById(topo, id) {
    return flowsOf(topo).find((f) => f.id === id);
  }
  function renderTopology(root, topo, onAction, onStoreAction) {
    lastTopo = topo;
    lastOnAction = onAction;
    lastOnStoreAction = onStoreAction || null;
    ensurePopover();
    if (!topo || !groupsOf(topo).length) {
      lastSignature = "";
      root.innerHTML = '<p class="topo-hint">\u6682\u65E0\u62D3\u6251\u6570\u636E</p>';
      return;
    }
    currentLinks = topo.links || [];
    const sig = topoSignature(topo);
    const board = root.querySelector("#topo-board");
    if (board && sig === lastSignature && root.querySelector(".arch")) {
      softUpdateTopology(root, topo);
      return;
    }
    lastSignature = sig;
    mountTopology(root, topo);
  }
  function softUpdateTopology(root, topo) {
    const gw = groupByRole(topo, "gateway");
    const meta = groupByRole(topo, "meta");
    const store = groupByRole(topo, "storage");
    const allNodes = [...gw?.nodes || [], ...store?.nodes || [], ...meta?.nodes || []];
    for (const n of allNodes) {
      const el = root.querySelector(`.topo-node[data-name="${CSS.escape(n.name)}"]`);
      if (el) patchNodeCard(el, n);
    }
    const pills = root.querySelector(".topo-flow-pills");
    if (pills) {
      pills.innerHTML = [
        flowPill(flowById(topo, "client_gw")),
        flowPill(flowById(topo, "gw_store")),
        flowPill(flowById(topo, "gw_meta")),
        flowPill(flowById(topo, "store_meta")),
        flowPill(flowById(topo, "store_raft")),
        flowPill(flowById(topo, "store_sync")),
        flowPill(flowById(topo, "meta_raft"))
      ].join("");
    }
    const panels = root.querySelector(".topo-side-panels");
    if (panels) {
      const links = currentLinks;
      const storePeerLanes = peerLanes(
        links.filter(
          (l) => (l.kind === "raft" || l.kind === "sync") && l.from.startsWith("storage") && l.to.startsWith("storage")
        )
      );
      const metaPeerLanes = peerLanes(
        links.filter(
          (l) => (l.kind === "raft" || l.kind === "sync") && l.from.startsWith("meta") && l.to.startsWith("meta")
        )
      );
      panels.innerHTML = laneBlock("Store \u526F\u672C\u95F4", storePeerLanes, "\u4EC5\u540C\u4E00 Region \u7684\u526F\u672C\u6709 Raft/\u540C\u6B65\uFF08\u9ED8\u8BA4 0/1/2\uFF09") + laneBlock("Meta \u526F\u672C\u95F4", metaPeerLanes, "");
    }
    const head = root.querySelector(".link-table-head > span");
    if (head) head.textContent = `\u8282\u70B9\u5BF9\u660E\u7EC6\uFF08${currentLinks.length}\uFF09`;
    renderLinkTableBody(root, currentLinks, linkFilter);
    redrawWiresRef?.();
  }
  function mountTopology(root, topo) {
    const gw = groupByRole(topo, "gateway");
    const meta = groupByRole(topo, "meta");
    const store = groupByRole(topo, "storage");
    const links = currentLinks;
    const storePeerLanes = peerLanes(
      links.filter(
        (l) => (l.kind === "raft" || l.kind === "sync") && l.from.startsWith("storage") && l.to.startsWith("storage")
      )
    );
    const metaPeerLanes = peerLanes(
      links.filter(
        (l) => (l.kind === "raft" || l.kind === "sync") && l.from.startsWith("meta") && l.to.startsWith("meta")
      )
    );
    const boardNodes = boardNodeCards(gw?.nodes || [], store?.nodes || [], meta?.nodes || []);
    const wireBtns = ["all", "gateway", "meta", "raft", "sync", "store"].map((id) => {
      const label = id === "all" ? "\u5168\u90E8\u7EBF" : id === "gateway" ? "\u7F51\u5173" : id === "meta" ? "Meta" : id === "raft" ? "Raft" : id === "sync" ? "\u540C\u6B65" : "Store \u95F4";
      return `<button type="button" class="link-filter${wireFilter === id ? " on" : ""}" data-wire="${id}">${label}</button>`;
    }).join("");
    root.innerHTML = `
    <div class="arch">
      <div class="arch-legend">
        <span><i class="lg gateway"></i>\u7F51\u5173</span>
        <span><i class="lg meta"></i>Meta</span>
        <span><i class="lg raft"></i>Raft</span>
        <span><i class="lg sync"></i>\u540C\u6B65</span>
      </div>

      <div class="topo-board-toolbar">
        <span class="topo-board-hint">\u62D6\u62FD\u8282\u70B9 \xB7 \u5355\u51FB\u64CD\u4F5C \xB7 \u7EBF\u4E0A\u4E3A\u8282\u70B9\u5BF9 QPS</span>
        <div class="wire-filters" id="wire-filters">${wireBtns}</div>
        <button type="button" class="link-filter" id="topo-reset-layout">\u91CD\u7F6E\u5E03\u5C40</button>
      </div>

      <div class="topo-flow-pills">
        ${flowPill(flowById(topo, "client_gw"))}
        ${flowPill(flowById(topo, "gw_store"))}
        ${flowPill(flowById(topo, "gw_meta"))}
        ${flowPill(flowById(topo, "store_meta"))}
        ${flowPill(flowById(topo, "store_raft"))}
        ${flowPill(flowById(topo, "store_sync"))}
        ${flowPill(flowById(topo, "meta_raft"))}
      </div>

      <div class="topo-board-scroll">
        <div class="topo-board enter" id="topo-board">
          <div class="topo-board-guide" aria-hidden="true">
            <span class="g-gw">Gateway</span>
            <span class="g-st">Storage</span>
            <span class="g-meta">Meta</span>
          </div>
          ${boardNodes}
        </div>
      </div>

      <div class="topo-side-panels">
        ${laneBlock("Store \u526F\u672C\u95F4", storePeerLanes, "\u4EC5\u540C\u4E00 Region \u7684\u526F\u672C\u6709 Raft/\u540C\u6B65\uFF08\u9ED8\u8BA4 0/1/2\uFF09")}
        ${laneBlock("Meta \u526F\u672C\u95F4", metaPeerLanes, "")}
      </div>

      ${linkTable(links)}
    </div>
  `;
    const redrawWires = () => {
      const b = root.querySelector("#topo-board");
      if (b) drawBoardWires(b, currentLinks, wireFilter);
    };
    redrawWiresRef = redrawWires;
    const board = root.querySelector("#topo-board");
    if (board) {
      applyBoardPositions(board);
      expandBoard(board);
      redrawWires();
      enableNodeDrag(
        board,
        (el) => {
          const name = el.dataset.name;
          if (!name) return;
          const node = findNode(name);
          if (!node || !lastTopo) return;
          openPopover(el, node, lastTopo.controllable);
        },
        redrawWires
      );
      window.setTimeout(() => board.classList.remove("enter"), 450);
    }
    root.querySelectorAll("#wire-filters .link-filter").forEach((btn) => {
      btn.addEventListener("click", () => {
        root.querySelectorAll("#wire-filters .link-filter").forEach((b) => b.classList.remove("on"));
        btn.classList.add("on");
        wireFilter = btn.dataset.wire || "all";
        saveWireFilter(wireFilter);
        redrawWires();
      });
    });
    root.querySelector("#topo-reset-layout")?.addEventListener("click", () => {
      clearPositions();
      lastSignature = "";
      if (lastTopo && lastOnAction) {
        renderTopology(root, lastTopo, lastOnAction || (() => {
        }), lastOnStoreAction || void 0);
      }
    });
    bindLinkFilters(root, links);
  }
  function patchNodeCard(el, n) {
    el.classList.toggle("up", !!n.up);
    el.classList.toggle("down", !n.up);
    el.classList.toggle("leader", !!n.is_leader);
    el.classList.add("has-disk");
    applyClusterStateClass(el, n.cluster_state);
    const statusText = n.up ? "\u5728\u7EBF" : n.alive ? "\u8FDB\u7A0B\u5728/\u7AEF\u53E3\u65E0" : "\u79BB\u7EBF";
    const badge = n.is_leader ? '<span class="topo-badge">L</span>' : "";
    const dragHandle = el.classList.contains("draggable") ? '<span class="topo-drag-handle" aria-hidden="true">\u22EE\u22EE</span>' : "";
    el.innerHTML = `
      <div class="topo-node-head">
        <span class="dot ${n.up ? "on" : "off"}"></span>
        <span class="topo-node-name">${escapeHtml(n.name)}</span>
        ${badge}
        ${clusterStateBadge(n)}
        ${dragHandle}
      </div>
      <div class="topo-node-addr">${escapeHtml(n.addr || "\u2014")}</div>
      ${rateBlock(n)}
      ${diskBlock(n)}
      <div class="topo-node-meta">${escapeHtml(statusText)}${n.pid ? ` \xB7 pid ${n.pid}` : ""}${clusterStateMeta(n)}</div>`;
  }
  function boardNodeCards(gws, stores, metas) {
    const parts = [];
    parts.push(clientCard());
    gws.forEach((n, i) => parts.push(nodeCard(n, i, true)));
    stores.forEach((n, i) => parts.push(nodeCard(n, i, true)));
    metas.forEach((n, i) => parts.push(nodeCard(n, i, true)));
    return parts.join("");
  }
  function clientCard() {
    return `
    <article class="topo-node soft-node draggable" data-name="client" data-role="client" title="\u62D6\u62FD\u79FB\u52A8">
      <div class="topo-node-head"><span class="topo-node-name">Client / skv</span></div>
      <div class="topo-node-meta">\u5916\u90E8\u5BA2\u6237\u7AEF</div>
    </article>`;
  }
  function applyBoardPositions(board) {
    const saved = loadPositions();
    const roleIdx = {};
    board.querySelectorAll(".topo-node.draggable").forEach((el) => {
      const name = el.dataset.name || "";
      const role = el.dataset.role || "";
      const idx = roleIdx[role] || 0;
      roleIdx[role] = idx + 1;
      const pos = saved[name] || defaultPosition(name, role, idx);
      placeNode(el, pos);
    });
  }
  function flowPill(flow) {
    if (!flow) return "";
    return `<span class="flow-pill ${escapeHtml(flow.kind)}">${escapeHtml(flow.label)} ${formatQps2(flow.qps)}/s</span>`;
  }
  function peerLanes(links) {
    const map = /* @__PURE__ */ new Map();
    for (const l of links) {
      const [a, b] = l.from < l.to ? [l.from, l.to] : [l.to, l.from];
      const key = `${l.kind}|${a}|${b}`;
      const cur = map.get(key);
      if (!cur) {
        map.set(key, { a, b, kind: l.kind, qps: l.qps || 0, count: l.count || 0 });
      } else {
        cur.qps = Math.max(cur.qps, l.qps || 0);
        cur.count = Math.max(cur.count, l.count || 0);
      }
    }
    return [...map.values()].sort((x, y) => y.qps - x.qps || y.count - x.count);
  }
  function laneBlock(title, lanes, note) {
    if (!lanes.length) {
      return `
      <div class="lane-block">
        <div class="lane-title">${escapeHtml(title)}</div>
        <div class="lane-empty">\u6682\u65E0\u4E92\u76F8\u540C\u6B65</div>
        ${note ? `<p class="arch-store-note">${escapeHtml(note)}</p>` : ""}
      </div>`;
    }
    const rows = lanes.slice(0, 12).map(
      (l) => `
      <div class="lane-row ${escapeHtml(l.kind)}">
        <span class="lane-end mono">${escapeHtml(l.a)}</span>
        <span class="lane-mid">
          <span class="lane-line"></span>
          <span class="lane-badge">${escapeHtml(l.kind === "sync" ? "\u540C\u6B65" : "Raft")} ${formatQps2(l.qps)}/s</span>
          <span class="lane-line"></span>
        </span>
        <span class="lane-end mono">${escapeHtml(l.b)}</span>
      </div>`
    ).join("");
    return `
    <div class="lane-block">
      <div class="lane-title">${escapeHtml(title)} \xB7 ${lanes.length} \u5BF9</div>
      ${rows}
      ${note ? `<p class="arch-store-note">${escapeHtml(note)}</p>` : ""}
    </div>`;
  }
  function linkTable(links) {
    if (!links.length) {
      return `<div class="link-table-wrap" id="link-table-wrap"><p class="topo-hint">\u6682\u65E0\u8282\u70B9\u95F4\u6D41\u91CF</p></div>`;
    }
    const filters = [
      ["all", "\u5168\u90E8"],
      ["store", "Store \u95F4"],
      ["gateway", "\u7F51\u5173"],
      ["meta", "Meta"],
      ["raft", "Raft"],
      ["sync", "\u540C\u6B65"]
    ];
    return `
    <div class="link-table-wrap" id="link-table-wrap">
      <div class="link-table-head">
        <span>\u8282\u70B9\u5BF9\u660E\u7EC6\uFF08${links.length}\uFF09</span>
        <div class="link-filters">
          ${filters.map(
      ([id, label]) => `<button type="button" class="link-filter${linkFilter === id ? " on" : ""}" data-filter="${id}">${label}</button>`
    ).join("")}
        </div>
      </div>
      <table class="link-table">
        <thead><tr><th>\u7C7B\u578B</th><th>\u6E90</th><th></th><th>\u76EE\u6807</th><th>QPS</th><th>\u7D2F\u8BA1</th></tr></thead>
        <tbody id="link-table-body"></tbody>
      </table>
    </div>`;
  }
  function filterLinks(links, filter) {
    return links.filter((l) => {
      if (filter === "all") return true;
      if (filter === "store") {
        return l.from.startsWith("storage") && l.to.startsWith("storage");
      }
      return l.kind === filter;
    });
  }
  function renderLinkTableBody(root, links, filter) {
    const body = root.querySelector("#link-table-body");
    if (!body) return;
    const list = filterLinks(links, filter);
    body.innerHTML = list.length ? list.slice(0, 60).map(
      (l) => `
        <tr class="kind-${escapeHtml(l.kind)}">
          <td><span class="kind-dot ${escapeHtml(l.kind)}"></span>${escapeHtml(l.label || l.kind)}</td>
          <td class="mono">${escapeHtml(l.from)}</td>
          <td class="arrow">\u2192</td>
          <td class="mono">${escapeHtml(l.to)}</td>
          <td class="num">${formatQps2(l.qps)}/s</td>
          <td class="num muted">${l.count ?? 0}</td>
        </tr>`
    ).join("") : `<tr class="link-empty"><td colspan="6">\u8BE5\u5206\u7C7B\u4E0B\u6682\u65E0\u6D41\u91CF</td></tr>`;
  }
  function bindLinkFilters(root, links) {
    const wrap = root.querySelector("#link-table-wrap");
    if (!wrap) return;
    const btns = wrap.querySelectorAll(".link-filter[data-filter]");
    if (!btns.length) return;
    btns.forEach((btn) => {
      btn.addEventListener("click", () => {
        btns.forEach((b) => b.classList.remove("on"));
        btn.classList.add("on");
        linkFilter = btn.dataset.filter || "all";
        saveLinkFilter(linkFilter);
        renderLinkTableBody(root, currentLinks.length ? currentLinks : links, linkFilter);
      });
    });
    renderLinkTableBody(root, links, linkFilter);
  }
  function findNode(name) {
    if (!lastTopo) return void 0;
    for (const g of groupsOf(lastTopo)) {
      const n = g.nodes.find((x) => x.name === name);
      if (n) return n;
    }
    return void 0;
  }
  function formatBytes(n) {
    if (!Number.isFinite(n) || n < 0) return "\u2014";
    const units = ["B", "K", "M", "G", "T"];
    let v = n;
    let i = 0;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i += 1;
    }
    const digits = i === 0 ? 0 : v >= 10 ? 0 : 1;
    return `${v.toFixed(digits)}${units[i]}`;
  }
  function formatQps2(n) {
    if (n == null || !Number.isFinite(n) || n < 0) return "\u2014";
    if (n < 10) return n.toFixed(1);
    if (n < 1e3) return n.toFixed(0);
    return `${(n / 1e3).toFixed(1)}k`;
  }
  function rateBlock(n) {
    if (!n.up) {
      return `<div class="topo-rate muted">QPS \u2014</div>`;
    }
    const r = n.rates || {};
    const chips = [];
    if (n.role === "gateway") {
      chips.push(chip("gateway", "\u5165", r.gw));
      chips.push(chip("gateway", "\u2192Store", r.to_store));
      chips.push(chip("meta", "\u2192Meta", r.to_meta));
    } else if (n.role === "storage") {
      chips.push(chip("gateway", "\u7F51\u5173", r.gw));
      chips.push(chip("meta", "Meta", r.meta_out));
      chips.push(chip("raft", "Raft", r.raft));
      chips.push(chip("sync", "\u540C\u6B65", r.sync));
    } else if (n.role === "meta") {
      chips.push(chip("meta", "\u7F51\u5173", r.gw));
      chips.push(chip("meta", "Store", r.store));
      chips.push(chip("raft", "Raft", (r.raft || 0) + (r.sync || 0)));
    }
    return `
    <div class="topo-rate">
      <span class="topo-rate-main">${formatQps2(n.qps)}<small>/s</small></span>
    </div>
    <div class="topo-chips">${chips.join("")}</div>`;
  }
  function chip(kind, label, qps) {
    return `<span class="topo-chip ${escapeHtml(kind)}">${escapeHtml(label)} ${formatQps2(qps)}</span>`;
  }
  function diskBlock(n) {
    const d = n.disk;
    if (!d?.ok || !d.total_bytes) return "";
    const pct = Math.min(100, Math.round(d.used_bytes / d.total_bytes * 100));
    const level = pct >= 90 ? "crit" : pct >= 75 ? "warn" : "ok";
    return `
    <div class="topo-disk" title="${escapeHtml(d.path || "")}">
      <div class="topo-disk-row">
        <span>\u4E3B\u673A</span>
        <span>${formatBytes(d.used_bytes)} / ${formatBytes(d.total_bytes)} \xB7 ${pct}%</span>
      </div>
      <div class="topo-disk-bar"><i class="${level}" style="width:${pct}%"></i></div>
      <div class="topo-disk-row muted">
        <span>\u672C\u8282\u70B9</span>
        <span>${formatBytes(d.data_bytes)}</span>
      </div>
    </div>`;
  }
  var CLUSTER_STATE_CLS = ["st-joining", "st-active", "st-draining", "st-offline", "st-tombstone"];
  function applyClusterStateClass(el, state) {
    for (const c of CLUSTER_STATE_CLS) el.classList.remove(c);
    const key = (state || "").toLowerCase();
    if (key) el.classList.add(`st-${key}`);
  }
  function clusterStateBadge(n) {
    if (!n.cluster_state && !n.state_label) return "";
    const key = (n.cluster_state || "").toLowerCase();
    const label = n.state_label || n.cluster_state || "";
    return `<span class="state-pill ${key}" title="${escapeHtml(n.cluster_state || "")}">${escapeHtml(label)}</span>`;
  }
  function clusterStateMeta(n) {
    if (!n.cluster_state) return "";
    const label = n.state_label || n.cluster_state;
    return ` \xB7 ${escapeHtml(label)}`;
  }
  function nodeCard(n, delayIdx, onBoard = false) {
    const state = n.up ? "up" : "down";
    const leader = n.is_leader ? "leader" : "";
    const stCls = n.cluster_state ? `st-${n.cluster_state.toLowerCase()}` : "";
    const badge = n.is_leader ? '<span class="topo-badge">L</span>' : "";
    const statusDot = `<span class="dot ${n.up ? "on" : "off"}"></span>`;
    const style = `animation-delay:${Math.min(delayIdx, 8) * 40}ms`;
    const statusText = n.up ? "\u5728\u7EBF" : n.alive ? "\u8FDB\u7A0B\u5728/\u7AEF\u53E3\u65E0" : "\u79BB\u7EBF";
    const dragCls = onBoard ? "draggable" : "";
    const title = onBoard ? "\u62D6\u62FD\u79FB\u52A8 \xB7 \u5355\u51FB\u64CD\u4F5C" : "\u70B9\u51FB\u64CD\u4F5C";
    return `
    <article class="topo-node ${state} ${leader} ${stCls} has-disk ${dragCls}" style="${style}"
      data-name="${escapeHtml(n.name)}" data-role="${escapeHtml(n.role)}"
      data-cluster-state="${escapeHtml(n.cluster_state || "")}"
      role="button" tabindex="0" title="${title}">
      <div class="topo-node-head">
        ${statusDot}
        <span class="topo-node-name">${escapeHtml(n.name)}</span>
        ${badge}
        ${clusterStateBadge(n)}
        ${onBoard ? '<span class="topo-drag-handle" aria-hidden="true">\u22EE\u22EE</span>' : ""}
      </div>
      <div class="topo-node-addr">${escapeHtml(n.addr || "\u2014")}</div>
      ${rateBlock(n)}
      ${diskBlock(n)}
      <div class="topo-node-meta">${escapeHtml(statusText)}${n.pid ? ` \xB7 pid ${n.pid}` : ""}${clusterStateMeta(n)}</div>
    </article>`;
  }
  function ensurePopover() {
    if (document.getElementById("topo-popover")) return;
    const backdrop = document.createElement("div");
    backdrop.id = "topo-popover-backdrop";
    backdrop.className = "topo-popover-backdrop";
    backdrop.hidden = true;
    const pop = document.createElement("div");
    pop.id = "topo-popover";
    pop.className = "topo-popover";
    pop.hidden = true;
    pop.setAttribute("role", "dialog");
    pop.setAttribute("aria-modal", "true");
    document.body.append(backdrop, pop);
    backdrop.addEventListener("click", closePopover);
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") closePopover();
    });
  }
  function openPopover(anchor, node, controllable) {
    const pop = document.getElementById("topo-popover");
    const backdrop = document.getElementById("topo-popover-backdrop");
    if (!pop || !backdrop) return;
    const stopDis = !controllable || !node.alive;
    const startDis = !controllable || node.up;
    const allDis = !controllable;
    const hint = controllable ? "\u9009\u62E9\u8981\u5BF9\u8282\u70B9\u6267\u884C\u7684\u64CD\u4F5C" : "\u672A\u914D\u7F6E -run-dir\uFF0C\u65E0\u6CD5\u4E0A\u4E0B\u7EBF";
    const r = node.rates || {};
    const rate = `<div class="topo-popover-disk">
      <div>\u603B\u901F\u7387 ${formatQps2(node.qps)}/s</div>
      ${node.role === "gateway" ? `<div class="flow-line gateway">\u5BA2\u6237\u7AEF ${formatQps2(r.gw)} \xB7 \u2192Store ${formatQps2(r.to_store)} \xB7 \u2192Meta ${formatQps2(r.to_meta)}</div>` : ""}
      ${node.role === "storage" ? `<div class="flow-line gateway">\u7F51\u5173 ${formatQps2(r.gw)}</div>
        <div class="flow-line meta">Meta \u51FA\u7AD9 ${formatQps2(r.meta_out)}</div>
        <div class="flow-line raft">Raft ${formatQps2(r.raft)}</div>
        <div class="flow-line sync">\u6570\u636E\u540C\u6B65 ${formatQps2(r.sync)}</div>` : ""}
      ${node.role === "meta" ? `<div class="flow-line meta">\u7F51\u5173 ${formatQps2(r.gw)} \xB7 Store ${formatQps2(r.store)}</div>
        <div class="flow-line raft">Raft ${formatQps2((r.raft || 0) + (r.sync || 0))}</div>` : ""}
      <div class="muted">\u7D2F\u8BA1 ${node.req_total ?? 0}</div>
    </div>`;
    const disk = node.disk?.ok ? `<div class="topo-popover-disk">
        <div>\u4E3B\u673A\u78C1\u76D8 ${formatBytes(node.disk.used_bytes)} / ${formatBytes(node.disk.total_bytes)}</div>
        <div>\u672C\u8282\u70B9\u6570\u636E ${formatBytes(node.disk.data_bytes)}</div>
        <div class="muted">${escapeHtml(node.disk.path || "")}</div>
      </div>` : "";
    pop.innerHTML = `
    <div class="topo-popover-head">
      <div>
        <div class="topo-popover-title">
          <span class="dot ${node.up ? "on" : "off"}"></span>
          ${escapeHtml(node.name)}
          ${node.is_leader ? '<span class="topo-badge">L</span>' : ""}
          ${clusterStateBadge(node)}
        </div>
        <div class="topo-popover-sub">${escapeHtml(node.role)} \xB7 ${escapeHtml(node.addr || "\u2014")}${node.cluster_state ? ` \xB7 \u72B6\u6001 ${escapeHtml(node.state_label || node.cluster_state)} (${escapeHtml(node.cluster_state)})` : ""}</div>
      </div>
      <button type="button" class="topo-popover-close" aria-label="\u5173\u95ED">\xD7</button>
    </div>
    ${rate}
    ${disk}
    <p class="topo-popover-hint">${escapeHtml(hint)}</p>
    <div class="topo-popover-actions">
      ${node.role === "storage" ? (() => {
      const st = (node.cluster_state || "").toLowerCase();
      const bits = [];
      if (st === "joining" || st === "offline" || !node.up || st === "tombstone") {
        bits.push(`<button type="button" class="topo-act ok" data-store-action="online">\u4E0A\u7EBF</button>`);
      }
      if (st === "active" || st === "offline" || st === "joining" && node.alive) {
        bits.push(`<button type="button" class="topo-act danger" data-store-action="offline">\u4E0B\u7EBF</button>`);
      }
      if (st === "draining") {
        bits.push(`<span class="muted">\u7F29\u5BB9\u4E2D\u2026</span>`);
      }
      if (st === "tombstone" || st === "draining" || st === "joining" && !node.alive || st === "offline") {
        bits.push(`<button type="button" class="topo-act danger" data-store-action="kick" title="\u987B\u5DF2\u4E0B\u7EBF\u4E14\u65E0\u526F\u672C">\u8E22\u51FA</button>`);
      }
      if (bits.length) return bits.join("");
      return "";
    })() : `<button type="button" class="topo-act ok" data-action="start" ${startDis || allDis ? "disabled" : ""}>\u4E0A\u7EBF</button>
      <button type="button" class="topo-act danger" data-action="stop" ${stopDis || allDis ? "disabled" : ""}>\u4E0B\u7EBF</button>
      <button type="button" class="topo-act" data-action="restart" ${allDis ? "disabled" : ""}>\u91CD\u542F</button>`}
    </div>
  `;
    pop.querySelector(".topo-popover-close")?.addEventListener("click", closePopover);
    pop.querySelectorAll("[data-store-action]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const action = btn.dataset.storeAction;
        closePopover();
        if (action && lastOnStoreAction) lastOnStoreAction(action, node.name);
      });
    });
    pop.querySelectorAll("[data-action]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const action = btn.dataset.action;
        if (!action || !lastOnAction) return;
        closePopover();
        lastOnAction(node.name, action);
      });
    });
    backdrop.hidden = false;
    pop.hidden = false;
    const rect = anchor.getBoundingClientRect();
    const pw = pop.offsetWidth || 280;
    const ph = pop.offsetHeight || 180;
    let left = rect.left + rect.width / 2 - pw / 2;
    let top = rect.bottom + 10;
    left = Math.max(12, Math.min(left, window.innerWidth - pw - 12));
    if (top + ph > window.innerHeight - 12) {
      top = Math.max(12, rect.top - ph - 10);
    }
    pop.style.left = `${left}px`;
    pop.style.top = `${top}px`;
  }
  function closePopover() {
    const pop = document.getElementById("topo-popover");
    const backdrop = document.getElementById("topo-popover-backdrop");
    if (pop) pop.hidden = true;
    if (backdrop) backdrop.hidden = true;
  }

  // src/modal.ts
  function ensureModal() {
    let backdrop = document.getElementById("skv-modal-backdrop");
    let box = document.getElementById("skv-modal");
    if (!backdrop) {
      backdrop = document.createElement("div");
      backdrop.id = "skv-modal-backdrop";
      backdrop.className = "skv-modal-backdrop";
      backdrop.hidden = true;
      document.body.appendChild(backdrop);
    }
    if (!box) {
      box = document.createElement("div");
      box.id = "skv-modal";
      box.className = "skv-modal";
      box.hidden = true;
      box.setAttribute("role", "dialog");
      box.setAttribute("aria-modal", "true");
      document.body.appendChild(box);
    }
    return { backdrop, box };
  }
  function openModal(opts) {
    const { backdrop, box } = ensureModal();
    const fields = opts.fields || [];
    const fieldsHtml = fields.map((f) => {
      if (f.type === "checkbox") {
        return `<label class="skv-modal-check">
          <input type="checkbox" name="${escapeHtml(f.name)}" ${f.checked ? "checked" : ""} />
          <span>${escapeHtml(f.label)}</span>
          ${f.hint ? `<small>${escapeHtml(f.hint)}</small>` : ""}
        </label>`;
      }
      return `<label class="skv-modal-field">
        <span>${escapeHtml(f.label)}</span>
        <input type="${f.type || "text"}" name="${escapeHtml(f.name)}"
          value="${escapeHtml(f.value || "")}"
          placeholder="${escapeHtml(f.placeholder || "")}"
          ${f.required ? "required" : ""} autocomplete="off" />
      </label>`;
    }).join("");
    box.innerHTML = `
    <div class="skv-modal-head">
      <div>
        <h3 class="skv-modal-title">${escapeHtml(opts.title)}</h3>
        ${opts.subtitle ? `<p class="skv-modal-sub">${escapeHtml(opts.subtitle)}</p>` : ""}
      </div>
      <button type="button" class="skv-modal-x" data-modal="cancel" aria-label="\u5173\u95ED">\xD7</button>
    </div>
    <form class="skv-modal-body" id="skv-modal-form">
      ${fieldsHtml}
      <div class="skv-modal-actions">
        <button type="button" class="ghost" data-modal="cancel">${escapeHtml(opts.cancelLabel || "\u53D6\u6D88")}</button>
        <button type="submit" class="${opts.danger ? "danger" : "primary"}">${escapeHtml(opts.confirmLabel || "\u786E\u5B9A")}</button>
      </div>
    </form>
  `;
    backdrop.hidden = false;
    box.hidden = false;
    requestAnimationFrame(() => {
      backdrop.classList.add("show");
      box.classList.add("show");
    });
    const firstInput = box.querySelector("input:not([type=checkbox])");
    firstInput?.focus();
    firstInput?.select();
    return new Promise((resolve) => {
      const finish = (ok, values = {}) => {
        backdrop.classList.remove("show");
        box.classList.remove("show");
        backdrop.hidden = true;
        box.hidden = true;
        resolve({ ok, values });
      };
      const onKey = (e) => {
        if (e.key === "Escape") {
          e.preventDefault();
          cleanup();
          finish(false);
        }
      };
      const cleanup = () => {
        document.removeEventListener("keydown", onKey);
        backdrop.onclick = null;
      };
      document.addEventListener("keydown", onKey);
      backdrop.onclick = () => {
        cleanup();
        finish(false);
      };
      box.querySelectorAll("[data-modal=cancel]").forEach((el) => {
        el.addEventListener("click", () => {
          cleanup();
          finish(false);
        });
      });
      const form = box.querySelector("#skv-modal-form");
      form?.addEventListener("submit", (e) => {
        e.preventDefault();
        const values = {};
        for (const f of fields) {
          const input = form.querySelector(`[name="${f.name}"]`);
          if (!input) continue;
          if (f.type === "checkbox") values[f.name] = input.checked;
          else values[f.name] = input.value;
        }
        for (const f of fields) {
          if (f.type !== "checkbox" && f.required) {
            const v = String(values[f.name] || "").trim();
            if (!v) {
              inputShake(form.querySelector(`[name="${f.name}"]`));
              return;
            }
          }
        }
        cleanup();
        finish(true, values);
      });
    });
  }
  function inputShake(el) {
    if (!el) return;
    el.classList.add("shake");
    el.focus();
    setTimeout(() => el.classList.remove("shake"), 400);
  }
  function openConfirm(opts) {
    return openModal({
      title: opts.title,
      subtitle: opts.body,
      confirmLabel: opts.confirmLabel || "\u786E\u5B9A",
      danger: opts.danger
    }).then((r) => r.ok);
  }

  // src/regions.ts
  var COLORS = ["#1f6f6a", "#2a5f8a", "#4a6b3c", "#6b4e2e", "#3d4f7a", "#2d6b55"];
  var lastRegions = [];
  var onAfterOp = null;
  var bound = false;
  var scalePollTimer = null;
  var pendingReplicaKicks = /* @__PURE__ */ new Map();
  function pendingKey(rid, nodeId) {
    return `${rid}:${nodeId}`;
  }
  function listPendingReplicaKicks(regionId) {
    return [...pendingReplicaKicks.values()].filter((p) => p.region_id === regionId);
  }
  function rememberPendingKick(p) {
    pendingReplicaKicks.set(pendingKey(p.region_id, p.node_id), p);
  }
  function forgetPendingKick(regionId, nodeId) {
    pendingReplicaKicks.delete(pendingKey(regionId, nodeId));
  }
  function sleep(ms) {
    return new Promise((r) => window.setTimeout(r, ms));
  }
  function formatScaleLine(t) {
    const pct = t.catchup_pct != null ? t.catchup_pct : t.progress_pct != null ? t.progress_pct : Math.round((t.progress || 0) * 100);
    const shards = (t.shards || []).map((x) => `R${x}`).join(",") || "\u2014";
    const bits = [
      `${t.task_id || "?"} \xB7 ${t.state_zh || t.state_name || t.state}`,
      `node=${t.node_id || "?"}`,
      `shards=${shards}`,
      `${pct}%`
    ];
    if (t.match != null && t.commit != null) {
      bits.push(`match=${t.match}/${t.commit}${t.caught ? " \u5DF2\u8FFD\u5E73" : ""}`);
    }
    if (t.error) bits.push(`err=${t.error}`);
    return bits.join(" \xB7 ");
  }
  function renderScaleTasks(tasks) {
    const pane = document.getElementById("scale-progress-pane");
    const list = document.getElementById("scale-progress-list");
    if (!pane || !list) return;
    const show = tasks.filter((t) => t.active || !t.done && !t.failed || t.catching_up);
    const recentDone = tasks.filter((t) => t.done || t.failed).slice(-3);
    const view = [...show, ...recentDone.filter((d) => !show.some((s) => s.task_id === d.task_id))];
    if (!view.length) {
      pane.hidden = true;
      list.innerHTML = "";
      return;
    }
    pane.hidden = false;
    list.innerHTML = view.map((t) => {
      const pct = t.catchup_pct != null ? t.catchup_pct : t.progress_pct != null ? t.progress_pct : Math.round((t.progress || 0) * 100);
      const cls = t.failed ? "failed" : t.done ? "done" : "";
      const stateCls = t.failed ? "failed" : t.done ? "done" : "";
      const meta = t.match != null && t.commit != null ? `Raft \u8FFD\u5E73 match=${t.match} / commit=${t.commit}${t.caught ? " \xB7 \u5DF2\u8FFD\u5E73" : ""}` : t.error ? `\u9519\u8BEF: ${escapeHtml(t.error)}` : t.catching_up ? "\u7B49\u5F85 Learner \u65E5\u5FD7\u8FFD\u5E73\u2026" : "";
      return `<div class="scale-card ${cls}">
        <div class="scale-card-head">
          <span class="tid">${escapeHtml(t.task_id || "")}</span>
          <span class="state ${stateCls}">${escapeHtml(t.state_zh || t.state_name || "")}</span>
          <span>${escapeHtml(t.node_id || "")} \u2192 ${(t.shards || []).map((x) => `R${x}`).join(", ") || "\u2014"}</span>
          <span>${pct}%</span>
        </div>
        <div class="scale-bar" role="progressbar" aria-valuenow="${pct}" aria-valuemin="0" aria-valuemax="100"><i style="width:${pct}%"></i></div>
        ${meta ? `<div class="scale-meta">${meta}</div>` : ""}
      </div>`;
    }).join("");
  }
  async function refreshScaleTasks() {
    try {
      const data = await api("/api/scale-tasks");
      const tasks = data.tasks || [];
      renderScaleTasks(tasks);
      const active = tasks.some((t) => t.active || t.catching_up || t.state != null && t.state < 5);
      if (active && scalePollTimer == null) {
        scalePollTimer = window.setInterval(() => {
          void refreshScaleTasks();
        }, 800);
      } else if (!active && scalePollTimer != null) {
        window.clearInterval(scalePollTimer);
        scalePollTimer = null;
      }
    } catch {
    }
  }
  async function pollScaleTask(taskId, regionId, nodeId) {
    const deadline = Date.now() + 12e4;
    while (Date.now() < deadline) {
      try {
        const t = await api(`/api/scale-task?task_id=${encodeURIComponent(taskId)}`);
        setLog([`\u52A0\u526F\u672C R${regionId} \u2192 ${nodeId}`, formatScaleLine(t)].join("\n"));
        await refreshScaleTasks();
        if (t.done || t.failed) {
          if (t.failed) {
            setLog(`\u52A0\u526F\u672C\u5931\u8D25: ${t.error || t.raw || formatScaleLine(t)}`);
          } else {
            setLog(`\u52A0\u526F\u672C\u5B8C\u6210\uFF08\u5DF2\u8FFD\u5E73\u5E76 Promote\uFF09
${formatScaleLine(t)}`);
          }
          await onAfterOp?.();
          return;
        }
      } catch (e) {
        setLog(`\u67E5\u8FDB\u5EA6\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
      await sleep(600);
    }
    setLog(`\u52A0\u526F\u672C\u8D85\u65F6\uFF08\u4ECD\u53EF\u5728\u4E0A\u65B9\u8FDB\u5EA6\u9762\u677F\u67E5\u770B ${taskId}\uFF09`);
    await refreshScaleTasks();
  }
  function bindRegionStrategy(refresh2) {
    onAfterOp = refresh2;
    if (bound) return;
    bound = true;
    const panel = $("#panel-ops");
    panel.addEventListener("click", (e) => {
      const t = e.target;
      const replicaBtn = t.closest("button[data-replica-act]");
      if (replicaBtn) {
        e.stopPropagation();
        const rid2 = Number(replicaBtn.dataset.rid);
        const nodeId = replicaBtn.dataset.nodeId || "";
        const address = replicaBtn.dataset.address || "";
        const action2 = replicaBtn.dataset.replicaAct || "";
        void runReplicaAction(rid2, nodeId, address, action2);
        return;
      }
      const act = t.closest("button[data-region-act]");
      if (!act) return;
      e.stopPropagation();
      const rid = Number(act.dataset.rid);
      const action = act.dataset.regionAct || "";
      void runRegionAction(rid, action);
    });
  }
  async function runReplicaAction(rid, nodeId, address, action) {
    if (!nodeId) return;
    if (action === "offline") {
      const ok = await openConfirm({
        title: `\u4E0B\u7EBF\u526F\u672C \xB7 R${rid}`,
        body: `\u4ECE Region ${rid} \u6210\u5458\u4E2D\u6458\u9664 ${nodeId}\uFF08\u975E\u6574\u673A\u4E0B\u7EBF\uFF09\u3002\u6458\u9664\u540E\u8BF7\u518D\u300C\u8E22\u51FA\u300D\u9500\u6BC1\u672C\u5730\u6570\u636E\u3002`,
        confirmLabel: "\u4E0B\u7EBF\u526F\u672C"
      });
      if (!ok) return;
      setLog(`\u4E0B\u7EBF\u526F\u672C R${rid} / ${nodeId}\u2026`);
      try {
        const resp = await postJSON(
          "/api/replica-offline",
          { region_id: rid, node_id: nodeId }
        );
        setLog((resp.logs || [JSON.stringify(resp)]).join("\n"));
        if (resp.ok && resp.pending_kick) {
          rememberPendingKick({
            region_id: rid,
            node_id: nodeId,
            address: resp.address || address
          });
        }
        if (!resp.ok) alert(resp.hint || "\u4E0B\u7EBF\u526F\u672C\u5931\u8D25");
        await onAfterOp?.();
      } catch (e) {
        setLog(`\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
      return;
    }
    if (action === "kick") {
      const ok = await openConfirm({
        title: `\u8E22\u51FA\u526F\u672C \xB7 R${rid}`,
        body: `\u9500\u6BC1 ${nodeId} \u4E0A Region ${rid} \u7684\u672C\u5730\u6570\u636E\uFF08Store \u8FDB\u7A0B\u4ECD\u5728\u96C6\u7FA4\uFF09\u3002\u987B\u5DF2\u5148\u4E0B\u7EBF\u3002`,
        confirmLabel: "\u8E22\u51FA"
      });
      if (!ok) return;
      setLog(`\u8E22\u51FA\u526F\u672C R${rid} / ${nodeId}\u2026`);
      try {
        const resp = await postJSON("/api/replica-kick", {
          region_id: rid,
          node_id: nodeId,
          address
        });
        setLog((resp.logs || [JSON.stringify(resp)]).join("\n"));
        if (resp.ok) forgetPendingKick(rid, nodeId);
        if (!resp.ok) alert(resp.hint || "\u8E22\u51FA\u5931\u8D25");
        await onAfterOp?.();
      } catch (e) {
        setLog(`\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
  }
  function renderRegions(data) {
    lastRegions = data.regions || [];
    const map = document.getElementById("range-map");
    if (!map) return;
    if (!lastRegions.length) {
      map.innerHTML = '<div class="range-seg" style="flex:1;opacity:.5">\u65E0 Region</div>';
      return;
    }
    map.innerHTML = lastRegions.map((r, i) => {
      const bg = COLORS[i % COLORS.length];
      return `<div class="range-seg" data-region-id="${r.id}" style="background:${bg}" title="${escapeHtml(
        r.start_key
      )} \u2192 ${escapeHtml(r.end_key)}">R${r.id}</div>`;
    }).join("");
  }
  function regionActionButtons(rid) {
    return `<div class="region-acts">
    <button type="button" class="ghost btn-xs" data-region-act="split" data-rid="${rid}">\u5206\u88C2</button>
    <button type="button" class="ghost btn-xs" data-region-act="merge-right" data-rid="${rid}">\u5408\u5E76\u53F3\u90BB</button>
    <button type="button" class="ghost btn-xs" data-region-act="add-replica" data-rid="${rid}">\u589E\u52A0\u526F\u672C</button>
    <button type="button" class="ghost btn-xs" data-region-act="place" data-rid="${rid}">\u653E\u7F6E</button>
    <button type="button" class="ghost btn-xs" data-region-act="suggest-place" data-rid="${rid}">\u5EFA\u8BAE\u653E\u7F6E</button>
  </div>`;
  }
  function findRegion(id) {
    return lastRegions.find((r) => r.id === id);
  }
  function suggestSplitKey(r) {
    const s = r.start_key;
    const e = r.end_key;
    if (s && s !== "\u2212\u221E" && !s.startsWith("0x") && e && e !== "+\u221E" && !e.startsWith("0x")) {
      if (s.length === 1 && e.length >= 1) {
        const mid = String.fromCharCode(Math.floor((s.charCodeAt(0) + e.charCodeAt(0)) / 2));
        if (mid > s && (e === "+\u221E" || mid < e)) return mid;
      }
    }
    if (!s || s === "\u2212\u221E") return "m";
    return s + "m";
  }
  function rangeLabel(r) {
    return `[${r.start_key || "\u2212\u221E"}, ${r.end_key || "+\u221E"})`;
  }
  function setLog(text) {
    const el = document.getElementById("ops-log") || document.getElementById("nodes-log");
    if (el) el.textContent = text;
  }
  async function runRegionAction(rid, action) {
    const r = findRegion(rid);
    if (!r) {
      setLog(`\u672A\u77E5 Region ${rid}`);
      return;
    }
    if (action === "split") {
      const modal = await openModal({
        title: `\u5206\u88C2 Region ${rid}`,
        subtitle: `\u533A\u95F4 ${rangeLabel(r)} \xB7 \u4EC5\u5207\u5143\u6570\u636E\uFF0C\u4E0D\u8FC1\u526F\u672C\uFF08\u65E0\u611F\u6269\u5BB9\u8D70 join + balance\uFF09`,
        confirmLabel: "\u6267\u884C\u5206\u88C2",
        fields: [
          {
            name: "split_key",
            label: "Split key",
            value: suggestSplitKey(r),
            placeholder: "m",
            required: true
          }
        ]
      });
      if (!modal.ok) return;
      const key = String(modal.values.split_key || "").trim();
      setLog(`\u5206\u88C2 R${rid}\u2026`);
      try {
        const resp = await postJSON("/api/split", {
          region_id: rid,
          split_key: key
        });
        setLog((resp.logs || [JSON.stringify(resp)]).join("\n"));
        await onAfterOp?.();
      } catch (e) {
        setLog(`\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
      return;
    }
    if (action === "merge-right") {
      setLog(`\u67E5\u627E R${rid} \u53F3\u90BB\u2026`);
      try {
        const found = await api(
          `/api/find-merge-right?left_id=${rid}`
        );
        const right_id = found.right_id;
        if (right_id == null) {
          setLog(found.raw || "\u65E0\u53F3\u90BB\u53EF\u5408\u5E76");
          return;
        }
        const right = findRegion(right_id);
        const ok = await openConfirm({
          title: `\u5408\u5E76 Region`,
          body: `\u5C06 R${rid} ${rangeLabel(r)} \u4E0E R${right_id} ${right ? rangeLabel(right) : ""} \u5408\u5E76\u4E3A\u5DE6 Region\u3002`,
          confirmLabel: "\u6267\u884C\u5408\u5E76",
          danger: true
        });
        if (!ok) return;
        setLog(`\u5408\u5E76 R${rid}+R${right_id}\u2026`);
        const resp = await postJSON("/api/merge", { left_id: rid, right_id });
        setLog((resp.logs || [JSON.stringify(resp)]).join("\n"));
        await onAfterOp?.();
      } catch (e) {
        setLog(`\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
      return;
    }
    if (action === "add-replica") {
      const cur = (r.replicas || []).map((x) => x.node_id).join(", ") || "\u2014";
      setLog(`\u67E5\u8BE2\u53EF\u52A0\u526F\u672C\u7684 Store\uFF08R${rid}\uFF09\u2026`);
      let suggested = "";
      let candidates = [];
      try {
        const sug = await api(`/api/suggest-replica?region_id=${rid}`);
        suggested = sug.suggested || "";
        candidates = sug.candidates || [];
        setLog(
          [
            `\u5DF2\u6709\u526F\u672C: ${(sug.have_peers || []).join(",") || "\u2014"}`,
            candidates.length ? `\u5019\u9009: ${candidates.map((c) => c.id).join(", ")}` : "\u65E0\u53EF\u7528 Active Store\uFF08\u53EF\u5148\u52A0\u5165\u5F85\u52A0\u5165\u8282\u70B9\uFF09"
          ].join("\n")
        );
      } catch (e) {
        setLog(`\u5EFA\u8BAE\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
      const modal = await openModal({
        title: `\u589E\u52A0\u526F\u672C \xB7 R${rid}`,
        subtitle: candidates.length ? `\u5F53\u524D\u526F\u672C\uFF1A${cur} \xB7 \u5019\u9009 ${candidates.map((c) => c.id).join(", ")}\uFF08\u4F1A\u62AC RF\uFF09` : `\u5F53\u524D\u526F\u672C\uFF1A${cur} \xB7 \u6682\u65E0\u7A7A\u95F2 Active Store\uFF0C\u53EF\u5148\u5728\u4E0B\u65B9\u300C\u5F85\u52A0\u5165\u300D\u52A0\u5165\u8282\u70B9`,
        confirmLabel: "\u589E\u52A0\u526F\u672C",
        fields: [
          {
            name: "node_id",
            label: "\u76EE\u6807 Store ID\uFF08\u987B Active\uFF0C\u4E14\u4E0D\u5728\u5F53\u524D\u526F\u672C\u4E2D\uFF09",
            value: suggested,
            placeholder: candidates[0]?.id || "storage-3",
            required: true
          }
        ]
      });
      if (!modal.ok) return;
      const node_id = String(modal.values.node_id || "").trim();
      if (!node_id) return;
      setLog(`SCALE_OUT ${node_id} \u2192 R${rid}\u2026`);
      try {
        const resp = await postJSON("/api/add-replica", { region_id: rid, node_id });
        if (!resp.ok) {
          setLog(resp.hint || resp.response || "\u52A0\u526F\u672C\u5931\u8D25");
          alert(resp.hint || resp.response || "\u52A0\u526F\u672C\u5931\u8D25");
          return;
        }
        const taskId = resp.task_id || "";
        if (!taskId) {
          setLog(JSON.stringify(resp, null, 2));
          await onAfterOp?.();
          return;
        }
        setLog(`\u5DF2\u5165\u961F ${taskId}\uFF08${resp.state_zh || "\u6392\u961F\u4E2D"}\uFF09\xB7 \u8F6E\u8BE2\u8FFD\u5E73\u8FDB\u5EA6\u2026`);
        await refreshScaleTasks();
        await pollScaleTask(taskId, rid, node_id);
      } catch (e) {
        setLog(`\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
      return;
    }
    if (action === "suggest-place") {
      setLog(`\u5EFA\u8BAE\u653E\u7F6E R${rid}\u2026`);
      try {
        const resp = await api(`/api/suggest-place?region_id=${rid}`);
        const peers = resp.peers?.join(",") || "";
        setLog(
          [resp.raw || "", resp.avoid?.length ? `avoid: ${resp.avoid.join(",")}` : "", peers ? `peers: ${peers}` : "\u65E0\u5EFA\u8BAE"].filter(Boolean).join("\n")
        );
        if (!peers) return;
        const ok = await openConfirm({
          title: `\u5EFA\u8BAE\u653E\u7F6E R${rid}`,
          body: `\u76EE\u6807 peers\uFF1A${peers}`,
          confirmLabel: "\u91C7\u7528\u5E76 Place"
        });
        if (!ok) return;
        const placeResp = await postJSON("/api/place", {
          region_id: rid,
          peers: peers.split(",")
        });
        setLog(JSON.stringify(placeResp, null, 2));
        await onAfterOp?.();
      } catch (e) {
        setLog(`\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
      return;
    }
    if (action === "place") {
      const cur = (r.replicas || []).map((x) => x.node_id).join(",");
      const modal = await openModal({
        title: `\u653E\u7F6E Region ${rid}`,
        subtitle: `\u5F53\u524D\u526F\u672C\uFF1A${cur || "\u2014"}`,
        confirmLabel: "\u6267\u884C Place",
        fields: [
          {
            name: "peers",
            label: "Peers\uFF08\u9017\u53F7\u5206\u9694 Store ID\uFF09",
            value: cur,
            placeholder: "storage-0,storage-1,storage-2",
            required: true
          }
        ]
      });
      if (!modal.ok) return;
      const peers = String(modal.values.peers || "").split(",").map((s) => s.trim()).filter(Boolean);
      if (!peers.length) return;
      setLog(`Place R${rid}\u2026`);
      try {
        const resp = await postJSON("/api/place", { region_id: rid, peers });
        setLog(JSON.stringify(resp, null, 2));
        await onAfterOp?.();
      } catch (e) {
        setLog(`\u5931\u8D25: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
  }

  // src/render.ts
  function renderOverview(ov, onNodeAction, onStoreAction) {
    const metaOk = ov.meta_up === ov.meta_total && ov.meta_total > 0;
    const stOk = ov.storage_up >= Math.ceil(ov.storage_total * 2 / 3);
    const stats = [
      ["Meta", `${ov.meta_up}/${ov.meta_total}`, metaOk ? "good" : "bad"],
      ["Storage", `${ov.storage_up}/${ov.storage_total}`, stOk ? "good" : "warn"],
      ["Gateway", ov.gateway_up ? "UP" : "DOWN", ov.gateway_up ? "good" : "bad"],
      ["Regions", String(ov.region_count), ""],
      ["Route ver", String(ov.route_version), ""],
      ["next_ts", String(ov.next_ts ?? "\u2014"), ""]
    ];
    $("#stat-row").innerHTML = stats.map(
      ([label, value, cls]) => `<div class="stat"><div class="label">${label}</div><div class="value ${cls}">${value}</div></div>`
    ).join("");
    $("#tso-dl").innerHTML = `
    <dt>gc_safe_ts</dt><dd>${ov.gc_safe_ts ?? "\u2014"}</dd>
    <dt>next_ts</dt><dd>${ov.next_ts ?? "\u2014"}</dd>
    <dt>min_start_hint</dt><dd>${ov.min_start_hint ?? "\u2014"}</dd>
    <dt>meta leader</dt><dd>${escapeHtml(ov.who_leader || "\u2014")}</dd>
  `;
    $("#fetched-at").textContent = ov.fetched_at || "";
    const hint = $("#topo-hint");
    const topo = ov.topology;
    if (topo?.controllable) {
      hint.textContent = "\u62D6\u62FD\u8282\u70B9\u6539\u4F4D\u7F6E \xB7 \u5355\u51FB\u6253\u5F00\u4E0A\u7EBF/\u4E0B\u7EBF/\u91CD\u542F";
    } else {
      hint.textContent = "\u62D6\u62FD\u8282\u70B9\u6539\u4F4D\u7F6E \xB7 \u914D\u7F6E -run-dir \u540E\u53EF\u4E0A\u4E0B\u7EBF";
    }
    renderTopology($("#topology"), topo, onNodeAction, onStoreAction);
  }
  function applyTopology(topo, onNodeAction, onStoreAction) {
    renderTopology($("#topology"), topo, onNodeAction, onStoreAction);
  }
  function stateKey(state) {
    return state.toLowerCase();
  }
  function stateLabel(state) {
    switch (stateKey(state)) {
      case "joining":
        return "\u5F85\u52A0\u5165";
      case "active":
        return "\u5728\u7528";
      case "draining":
        return "\u7F29\u5BB9\u4E2D";
      case "offline":
        return "\u5FC3\u8DF3\u8D85\u65F6";
      case "tombstone":
        return "\u5DF2\u817E\u7A7A";
      default:
        return state;
    }
  }
  function statePill(state) {
    const key = stateKey(state);
    const label = stateLabel(state);
    return `<span class="state-pill ${escapeHtml(key)}" title="${escapeHtml(state)}">${escapeHtml(label)} <small>${escapeHtml(state)}</small></span>`;
  }
  function isPending(n) {
    return stateKey(n.state) === "joining";
  }
  var expandedRegions = /* @__PURE__ */ new Set();
  function storeLifecycleActions(n) {
    if (n.role !== "storage") return "\u2014";
    const st = stateKey(n.state);
    const regions = n.regions ?? 0;
    const bits = [];
    const canOnline = st === "joining" || st === "offline" || !n.up && st !== "tombstone" && st !== "draining";
    const canOffline = st === "active" || st === "offline" || st === "joining" && !!n.up;
    const canKick = st === "tombstone" || (st === "draining" || st === "offline") && regions === 0 || st === "joining" && !n.up;
    if (canOnline && st !== "draining") {
      bits.push(
        `<button type="button" class="ghost sm" data-store-action="online" data-node-id="${escapeHtml(n.id)}">\u4E0A\u7EBF</button>`
      );
    }
    if (canOffline) {
      bits.push(
        `<button type="button" class="ghost sm danger-text" data-store-action="offline" data-node-id="${escapeHtml(n.id)}" title="Active\u2192Draining \u8FC1\u526F\u672C\uFF1BJoining\u2192\u505C\u8FDB\u7A0B">\u4E0B\u7EBF</button>`
      );
    }
    if (st === "draining") {
      bits.push(
        `<span class="muted" title="regions=${regions}">\u7F29\u5BB9\u4E2D\u2026</span>`
      );
    }
    if (canKick) {
      bits.push(
        `<button type="button" class="ghost sm danger-text" data-store-action="kick" data-node-id="${escapeHtml(n.id)}" title="\u987B\u5DF2\u4E0B\u7EBF\u4E14\u65E0\u526F\u672C">\u8E22\u51FA</button>`
      );
    } else if (st === "draining" && regions > 0) {
      bits.push(`<span class="muted">\u8FC1\u7A7A\u540E\u53EF\u8E22\u51FA (${regions})</span>`);
    }
    return bits.length ? `<div class="row-actions">${bits.join("")}</div>` : "\u2014";
  }
  function formatRange(r) {
    const a = r.start_key || "\u2212\u221E";
    const b = r.end_key || "+\u221E";
    return `${escapeHtml(a)} \u2026 ${escapeHtml(b)}`;
  }
  function regionReplicaActions(rid, nodeId, addr, pendingKick) {
    if (pendingKick) {
      return `<div class="row-actions">
      <span class="muted">\u5DF2\u4E0B\u7EBF</span>
      <button type="button" class="ghost sm danger-text" data-replica-act="kick" data-rid="${rid}" data-node-id="${escapeHtml(nodeId)}" data-address="${escapeHtml(addr)}">\u8E22\u51FA</button>
    </div>`;
    }
    return `<div class="row-actions">
    <button type="button" class="ghost sm danger-text" data-replica-act="offline" data-rid="${rid}" data-node-id="${escapeHtml(nodeId)}" title="\u4ECE\u672C Region \u6458\u9664\u8BE5\u526F\u672C\uFF08\u975E\u6574\u673A\uFF09">\u4E0B\u7EBF</button>
  </div>`;
  }
  function regionReplicaSubtable(r, nodeById) {
    const pending = listPendingReplicaKicks(r.id);
    const liveIds = new Set((r.replicas || []).map((x) => x.node_id));
    const liveRows = (r.replicas || []).map((rep) => {
      const n = nodeById.get(rep.node_id);
      const state = n?.state || "\u2014";
      const up = n?.up;
      const role = rep.is_leader ? '<span class="region-chip leader">Leader</span>' : '<span class="region-chip">Follower</span>';
      const addr = rep.address || n?.address || "";
      return `<tr>
        <td>${escapeHtml(rep.node_id)}</td>
        <td>${escapeHtml(addr || "\u2014")}</td>
        <td>${n ? statePill(state) : escapeHtml(state)}</td>
        <td>${role}</td>
        <td><span class="dot ${up ? "on" : "off"}"></span>${up ? "UP" : up === false ? "DOWN" : "\u2014"}</td>
        <td>${regionReplicaActions(r.id, rep.node_id, addr, false)}</td>
      </tr>`;
    });
    const pendingRows = pending.filter((p) => !liveIds.has(p.node_id)).map(
      (p) => `<tr class="replica-pending-kick">
        <td>${escapeHtml(p.node_id)}</td>
        <td>${escapeHtml(p.address || "\u2014")}</td>
        <td><span class="state-pill offline">\u5DF2\u4E0B\u7EBF <small>\u5F85\u8E22\u51FA</small></span></td>
        <td>\u2014</td>
        <td>\u2014</td>
        <td>${regionReplicaActions(r.id, p.node_id, p.address, true)}</td>
      </tr>`
    );
    const rows = [...liveRows, ...pendingRows].join("");
    return `<table class="region-child">
    <thead><tr><th>\u8282\u70B9</th><th>\u5730\u5740</th><th>\u72B6\u6001</th><th>\u89D2\u8272</th><th>\u53EF\u8FBE</th><th>\u526F\u672C\u64CD\u4F5C</th></tr></thead>
    <tbody>${rows || emptyRow(6, "\u65E0\u526F\u672C")}</tbody>
  </table>`;
  }
  function renderRegionMasterRows(regions, nodeById) {
    if (!regions.length) return emptyRow(7, "\u6682\u65E0 Region");
    return regions.map((r) => {
      const open = expandedRegions.has(r.id);
      const leader = (r.replicas || []).find((x) => x.is_leader)?.node_id || "\u2014";
      const nPeer = (r.replicas || []).length;
      return `
      <tr class="region-row ${open ? "open" : ""}" data-region-toggle="${r.id}" role="button" tabindex="0">
        <td class="expand-cell">${open ? "\u25BC" : "\u25B6"}</td>
        <td><span class="region-chip">R${r.id}</span></td>
        <td class="mono">${formatRange(r)}</td>
        <td>${r.epoch ?? "\u2014"}</td>
        <td>${nPeer}</td>
        <td class="mono">${escapeHtml(leader)}</td>
        <td>${regionActionButtons(r.id)}</td>
      </tr>
      <tr class="region-detail ${open ? "" : "hidden"}" data-region-detail="${r.id}">
        <td colspan="7">${regionReplicaSubtable(r, nodeById)}</td>
      </tr>`;
    }).join("");
  }
  function pendingStoreActions(n) {
    return storeLifecycleActions(n);
  }
  function bindStoreActions(root, onStoreAction) {
    if (!onStoreAction) return;
    root.querySelectorAll("[data-store-action]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const action = btn.dataset.storeAction;
        const nodeId = btn.dataset.nodeId;
        if (action && nodeId) onStoreAction(action, nodeId);
      });
    });
  }
  function emptyRow(cols, msg) {
    return `<tr><td colspan="${cols}" class="muted">${escapeHtml(msg)}</td></tr>`;
  }
  function renderNodes(data, onStoreAction, route) {
    const nodes = data.nodes || [];
    const pending = nodes.filter(isPending);
    const nodeById = new Map(nodes.map((n) => [n.id, n]));
    const regions = route?.regions || [];
    const legend = $("#node-state-legend");
    if (legend) {
      legend.innerHTML = [
        ["joining", "\u5F85\u52A0\u5165"],
        ["active", "\u5728\u7528"],
        ["draining", "\u7F29\u5BB9\u4E2D"],
        ["offline", "\u5FC3\u8DF3\u8D85\u65F6"],
        ["tombstone", "\u5DF2\u817E\u7A7A"]
      ].map(([k, lab]) => `<span class="state-pill ${k}">${lab} <small>${k}</small></span>`).join("");
    }
    const activeBody = $("#node-table-active tbody");
    activeBody.innerHTML = renderRegionMasterRows(regions, nodeById);
    activeBody.querySelectorAll("[data-region-toggle]").forEach((row) => {
      const toggle = () => {
        const id = Number(row.dataset.regionToggle);
        if (Number.isNaN(id)) return;
        if (expandedRegions.has(id)) expandedRegions.delete(id);
        else expandedRegions.add(id);
        renderNodes(data, onStoreAction, route);
      };
      row.addEventListener("click", (e) => {
        const t = e.target;
        if (t.closest("button") || t.closest(".region-acts") || t.closest(".row-actions")) return;
        toggle();
      });
      row.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          toggle();
        }
      });
    });
    const pendingBody = $("#node-table-pending tbody");
    pendingBody.innerHTML = pending.length ? pending.map(
      (n) => `<tr class="row-state-joining" data-state="${escapeHtml(n.state)}">
      <td>${escapeHtml(n.id)}</td>
      <td>${escapeHtml(n.role)}</td>
      <td>${escapeHtml(n.address)}</td>
      <td>${statePill(n.state)}</td>
      <td><span class="dot ${n.up ? "on" : "off"}"></span>${n.up ? "UP" : "DOWN"}</td>
      <td>${pendingStoreActions(n)}</td>
    </tr>`
    ).join("") : emptyRow(6, "\u6682\u65E0\u5F85\u52A0\u5165\u8282\u70B9\uFF08\u65B0 Store REGISTER \u540E\u51FA\u73B0\u5728\u6B64\uFF09");
    const dl = document.getElementById("pending-node-ids");
    if (dl) {
      dl.innerHTML = pending.map((n) => `<option value="${escapeHtml(n.id)}"></option>`).join("");
    }
    const joinInput = document.querySelector('#form-nodes-join input[name="node_id"]');
    if (joinInput && !joinInput.value && pending[0]) {
      joinInput.placeholder = pending[0].id;
    }
    bindStoreActions(pendingBody, onStoreAction);
  }

  // src/main.ts
  var busy = false;
  async function refresh() {
    const errors = [];
    let route = null;
    try {
      const ov = await api("/api/overview");
      renderOverview(ov, handleNodeAction, handleStoreAction);
    } catch (e) {
      errors.push(`overview: ${e instanceof Error ? e.message : String(e)}`);
    }
    try {
      route = await api("/api/route");
      renderRegions(route);
    } catch (e) {
      errors.push(`route: ${e instanceof Error ? e.message : String(e)}`);
    }
    try {
      const nodes = await api("/api/nodes");
      renderNodes(nodes, handleStoreAction, route);
    } catch (e) {
      errors.push(`nodes: ${e instanceof Error ? e.message : String(e)}`);
    }
    await refreshScaleTasks();
    if (errors.length) {
      console.error(errors);
      $("#fetched-at").textContent = `\u90E8\u5206\u5237\u65B0\u5931\u8D25: ${errors.join(" \xB7 ")}`;
    }
  }
  async function handleNodeAction(name, action) {
    if (busy) return;
    const label = action === "start" ? "\u4E0A\u7EBF" : action === "stop" ? "\u4E0B\u7EBF" : "\u91CD\u542F";
    if (!confirm(`${label}\u8282\u70B9 ${name}\uFF1F`)) return;
    busy = true;
    const log = $("#topo-log");
    log.textContent = `${label} ${name}\u2026`;
    try {
      const r = await postJSON(`/api/node/${action}`, { name });
      log.textContent = JSON.stringify(r, null, 2);
      if (r.topology) {
        applyTopology(r.topology, handleNodeAction, handleStoreAction);
      }
      await refresh();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      log.textContent = `\u5931\u8D25: ${msg}`;
      alert(msg);
    } finally {
      busy = false;
    }
  }
  function opsLog() {
    return document.getElementById("ops-log") || $("#nodes-log");
  }
  async function handleStoreAction(action, nodeId) {
    if (busy) return;
    const log = opsLog();
    const labels = {
      online: "\u4E0A\u7EBF",
      offline: "\u4E0B\u7EBF",
      kick: "\u8E22\u51FA"
    };
    const label = labels[action];
    if (!label) return;
    const hints = {
      online: `\u4E0A\u7EBF ${nodeId}\uFF1F\u62C9\u8D77\u8FDB\u7A0B\uFF1B\u82E5\u4E3A\u5F85\u52A0\u5165/Offline \u5219 JOIN \u2192 Active\u3002`,
      offline: `\u4E0B\u7EBF ${nodeId}\uFF1F\u5728\u7528\u8282\u70B9\u4F1A\u8FDB\u5165 Draining \u5E76\u8FC1\u51FA\u526F\u672C\uFF1B\u5F85\u52A0\u5165\u4EC5\u505C\u8FDB\u7A0B\u3002\u8FC1\u7A7A\u540E\u624D\u80FD\u8E22\u51FA\u3002`,
      kick: `\u8E22\u51FA ${nodeId}\uFF1F\u987B\u5DF2\u4E0B\u7EBF\u4E14\u65E0 Region\uFF0C\u5C06\u64E6\u9664 Meta \u5143\u6570\u636E\u5E76\u505C\u8FDB\u7A0B\u3002`
    };
    if (!confirm(hints[action] || `${label} ${nodeId}\uFF1F`)) return;
    busy = true;
    try {
      const r = await postJSON(`/api/node-${action}`, { node_id: nodeId });
      log.textContent = (r.logs || [JSON.stringify(r, null, 2)]).join("\n");
      if (r.ok === false) {
        alert(r.hint || r.response || r.error || `${label}\u5931\u8D25`);
      }
      if (r.topology) {
        applyTopology(r.topology, handleNodeAction, handleStoreAction);
      }
      await refresh();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      log.textContent = `\u5931\u8D25: ${msg}`;
      alert(msg);
    } finally {
      busy = false;
    }
  }
  function boot() {
    $("#tabs").addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-tab]");
      if (btn?.dataset.tab) setTab(btn.dataset.tab);
    });
    $("#btn-refresh").addEventListener("click", () => {
      void refresh();
    });
    $("#btn-advance").addEventListener("click", async () => {
      try {
        const r = await postJSON("/api/advance-safe", {});
        alert(r.response || JSON.stringify(r));
        await refresh();
      } catch (e) {
        alert(e instanceof Error ? e.message : String(e));
      }
    });
    bindRegionStrategy(refresh);
    bindForm("#form-nodes-join", async (fd) => {
      const nodeId = String(fd.get("node_id")).trim();
      if (!nodeId) {
        alert("\u8BF7\u586B\u5199 Store ID");
        return;
      }
      if (!confirm(`\u52A0\u5165 Store ${nodeId}\uFF1F\u5C06\u68C0\u67E5\u53EF\u8FBE\u6027\u4E0E\u5FC3\u8DF3\u540E\u5347\u4E3A Active\u3002`)) return;
      const r = await postJSON("/api/join-node", { node_id: nodeId });
      opsLog().textContent = JSON.stringify(r, null, 2);
      if (!r.ok) alert(r.hint || r.response || "\u52A0\u5165\u5931\u8D25\uFF08\u8282\u70B9\u4E0D\u53EF\u7528\uFF09");
      await refresh();
    });
    bindForm("#form-balance", async (fd) => {
      const r = await postJSON("/api/balance", {
        count: Number(fd.get("count")) || 1
      });
      opsLog().textContent = JSON.stringify(r, null, 2);
      await refresh();
    });
    bindForm("#form-kv-get", async (fd) => {
      const key = encodeURIComponent(String(fd.get("key")));
      const stale = fd.get("stale") ? "&stale=1" : "";
      const r = await api(`/api/kv/get?key=${key}${stale}`);
      $("#console-log").textContent = JSON.stringify(r, null, 2);
    });
    bindForm("#form-kv-put", async (fd) => {
      const r = await postJSON("/api/kv/put", {
        key: String(fd.get("key")),
        value: String(fd.get("value"))
      });
      $("#console-log").textContent = JSON.stringify(r, null, 2);
    });
    bindForm("#form-locate", async (fd) => {
      const key = encodeURIComponent(String(fd.get("key")));
      const r = await api(`/api/locate?key=${key}`);
      $("#console-log").textContent = JSON.stringify(r, null, 2);
    });
    const runDS = async (body) => {
      const r = await postJSON("/api/ds", body);
      $("#console-log").textContent = JSON.stringify(r, null, 2);
    };
    bindForm("#form-ds-hash", async (fd) => {
      await runDS({
        op: String(fd.get("op") || "hgetall"),
        key: String(fd.get("key")),
        field: String(fd.get("field") || ""),
        value: String(fd.get("value") || "")
      });
    });
    bindForm("#form-ds-set", async (fd) => {
      await runDS({
        op: String(fd.get("op") || "smembers"),
        key: String(fd.get("key")),
        member: String(fd.get("member") || "")
      });
    });
    bindForm("#form-ds-list", async (fd) => {
      await runDS({
        op: String(fd.get("op") || "lrange"),
        key: String(fd.get("key")),
        value: String(fd.get("value") || ""),
        start: Number(fd.get("start") || 0),
        stop: Number(fd.get("stop") ?? -1)
      });
    });
    bindForm("#form-ds-zset", async (fd) => {
      await runDS({
        op: String(fd.get("op") || "zrange"),
        key: String(fd.get("key")),
        member: String(fd.get("member") || ""),
        score: Number(fd.get("score") || 0),
        start: Number(fd.get("start") || 0),
        stop: Number(fd.get("stop") ?? -1)
      });
    });
    void refresh();
    window.setInterval(() => void refresh(), 8e3);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
//# sourceMappingURL=app.js.map
