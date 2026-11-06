// Copyright 2026 devotionalist
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {
  clearPositions,
  defaultPosition,
  enableNodeDrag,
  expandBoard,
  loadPositions,
  placeNode,
} from "./drag";
import { escapeHtml } from "./dom";
import type { FlowEdge, NodeLink, Topology, TopoGroup, TopoNode } from "./types";
import { drawBoardWires, type WireFilter } from "./wires";

export type NodeAction = "start" | "stop" | "restart";

const WIRE_FILTER_KEY = "skv-topo-wire-filter";
const LINK_FILTER_KEY = "skv-topo-link-filter";

let lastTopo: Topology | undefined;
let lastOnAction: ((name: string, action: NodeAction) => void) | null = null;
let lastOnStoreAction: ((action: string, nodeId: string) => void) | null = null;
let lastSignature = "";
let wireFilter: WireFilter = loadWireFilter();
let linkFilter = loadLinkFilter();
let currentLinks: NodeLink[] = [];
let redrawWiresRef: (() => void) | null = null;

function loadWireFilter(): WireFilter {
  try {
    const v = localStorage.getItem(WIRE_FILTER_KEY);
    if (v === "gateway" || v === "meta" || v === "raft" || v === "sync" || v === "store" || v === "all") {
      return v;
    }
  } catch {
    /* ignore */
  }
  return "all";
}

function saveWireFilter(v: WireFilter): void {
  try {
    localStorage.setItem(WIRE_FILTER_KEY, v);
  } catch {
    /* ignore */
  }
}

function loadLinkFilter(): string {
  try {
    return localStorage.getItem(LINK_FILTER_KEY) || "all";
  } catch {
    return "all";
  }
}

function saveLinkFilter(v: string): void {
  try {
    localStorage.setItem(LINK_FILTER_KEY, v);
  } catch {
    /* ignore */
  }
}

function topoSignature(topo: Topology): string {
  const names: string[] = ["client"];
  for (const g of groupsOf(topo)) {
    for (const n of g.nodes || []) names.push(n.name);
  }
  return names.sort().join("|");
}

function groupsOf(topo: Topology): TopoGroup[] {
  return (topo.groups || topo.layers || []).filter((g) => g.nodes?.length);
}

function groupByRole(topo: Topology, role: string): TopoGroup | undefined {
  return groupsOf(topo).find((g) => g.role === role);
}

function flowsOf(topo: Topology): FlowEdge[] {
  return topo.flows || topo.edges || [];
}

function flowById(topo: Topology, id: string): FlowEdge | undefined {
  return flowsOf(topo).find((f) => f.id === id);
}

/**
 *   Client ──gw──▶ Gateway ──gw──▶ Storage
 *                     │                │
 *                     │ meta           │ meta
 *                     └─────▶ Meta ◀───┘
 *   Storage 内：raft / sync；Meta 内：raft
 */
export function renderTopology(
  root: HTMLElement,
  topo: Topology | undefined,
  onAction: (name: string, action: NodeAction) => void,
  onStoreAction?: (action: string, nodeId: string) => void,
): void {
  lastTopo = topo;
  lastOnAction = onAction;
  lastOnStoreAction = onStoreAction || null;
  ensurePopover();

  if (!topo || !groupsOf(topo).length) {
    lastSignature = "";
    root.innerHTML = '<p class="topo-hint">暂无拓扑数据</p>';
    return;
  }

  currentLinks = topo.links || [];
  const sig = topoSignature(topo);
  const board = root.querySelector<HTMLElement>("#topo-board");

  // 节点集合未变：就地刷新，避免整页重绘闪烁 & 重置筛选
  if (board && sig === lastSignature && root.querySelector(".arch")) {
    softUpdateTopology(root, topo);
    return;
  }

  lastSignature = sig;
  mountTopology(root, topo);
}

function softUpdateTopology(root: HTMLElement, topo: Topology): void {
  const gw = groupByRole(topo, "gateway");
  const meta = groupByRole(topo, "meta");
  const store = groupByRole(topo, "storage");
  const allNodes = [...(gw?.nodes || []), ...(store?.nodes || []), ...(meta?.nodes || [])];

  for (const n of allNodes) {
    const el = root.querySelector<HTMLElement>(`.topo-node[data-name="${CSS.escape(n.name)}"]`);
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
      flowPill(flowById(topo, "meta_raft")),
    ].join("");
  }

  const panels = root.querySelector(".topo-side-panels");
  if (panels) {
    const links = currentLinks;
    const storePeerLanes = peerLanes(
      links.filter(
        (l) =>
          (l.kind === "raft" || l.kind === "sync") &&
          l.from.startsWith("storage") &&
          l.to.startsWith("storage"),
      ),
    );
    const metaPeerLanes = peerLanes(
      links.filter(
        (l) =>
          (l.kind === "raft" || l.kind === "sync") &&
          l.from.startsWith("meta") &&
          l.to.startsWith("meta"),
      ),
    );
    panels.innerHTML =
      laneBlock("Store 副本间", storePeerLanes, "仅同一 Region 的副本有 Raft/同步（默认 0/1/2）") +
      laneBlock("Meta 副本间", metaPeerLanes, "");
  }

  const head = root.querySelector(".link-table-head > span");
  if (head) head.textContent = `节点对明细（${currentLinks.length}）`;
  renderLinkTableBody(root, currentLinks, linkFilter);
  redrawWiresRef?.();
}

function mountTopology(root: HTMLElement, topo: Topology): void {
  const gw = groupByRole(topo, "gateway");
  const meta = groupByRole(topo, "meta");
  const store = groupByRole(topo, "storage");

  const links = currentLinks;
  const storePeerLanes = peerLanes(
    links.filter(
      (l) =>
        (l.kind === "raft" || l.kind === "sync") &&
        l.from.startsWith("storage") &&
        l.to.startsWith("storage"),
    ),
  );
  const metaPeerLanes = peerLanes(
    links.filter(
      (l) =>
        (l.kind === "raft" || l.kind === "sync") &&
        l.from.startsWith("meta") &&
        l.to.startsWith("meta"),
    ),
  );

  const boardNodes = boardNodeCards(gw?.nodes || [], store?.nodes || [], meta?.nodes || []);
  const wireBtns = (["all", "gateway", "meta", "raft", "sync", "store"] as const)
    .map((id) => {
      const label =
        id === "all" ? "全部线" : id === "gateway" ? "网关" : id === "meta" ? "Meta" : id === "raft" ? "Raft" : id === "sync" ? "同步" : "Store 间";
      return `<button type="button" class="link-filter${wireFilter === id ? " on" : ""}" data-wire="${id}">${label}</button>`;
    })
    .join("");

  root.innerHTML = `
    <div class="arch">
      <div class="arch-legend">
        <span><i class="lg gateway"></i>网关</span>
        <span><i class="lg meta"></i>Meta</span>
        <span><i class="lg raft"></i>Raft</span>
        <span><i class="lg sync"></i>同步</span>
      </div>

      <div class="topo-board-toolbar">
        <span class="topo-board-hint">拖拽节点 · 单击操作 · 线上为节点对 QPS</span>
        <div class="wire-filters" id="wire-filters">${wireBtns}</div>
        <button type="button" class="link-filter" id="topo-reset-layout">重置布局</button>
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
        ${laneBlock("Store 副本间", storePeerLanes, "仅同一 Region 的副本有 Raft/同步（默认 0/1/2）")}
        ${laneBlock("Meta 副本间", metaPeerLanes, "")}
      </div>

      ${linkTable(links)}
    </div>
  `;

  const redrawWires = () => {
    const b = root.querySelector<HTMLElement>("#topo-board");
    if (b) drawBoardWires(b, currentLinks, wireFilter);
  };
  redrawWiresRef = redrawWires;

  const board = root.querySelector<HTMLElement>("#topo-board");
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
      redrawWires,
    );
    window.setTimeout(() => board.classList.remove("enter"), 450);
  }

  root.querySelectorAll<HTMLButtonElement>("#wire-filters .link-filter").forEach((btn) => {
    btn.addEventListener("click", () => {
      root.querySelectorAll("#wire-filters .link-filter").forEach((b) => b.classList.remove("on"));
      btn.classList.add("on");
      wireFilter = (btn.dataset.wire as WireFilter) || "all";
      saveWireFilter(wireFilter);
      redrawWires();
    });
  });

  root.querySelector("#topo-reset-layout")?.addEventListener("click", () => {
    clearPositions();
    lastSignature = "";
    if (lastTopo && lastOnAction) {
      renderTopology(root, lastTopo, lastOnAction || (() => {}), lastOnStoreAction || undefined);
    }
  });

  bindLinkFilters(root, links);
}

function patchNodeCard(el: HTMLElement, n: TopoNode): void {
  el.classList.toggle("up", !!n.up);
  el.classList.toggle("down", !n.up);
  el.classList.toggle("leader", !!n.is_leader);
  el.classList.add("has-disk");
  applyClusterStateClass(el, n.cluster_state);

  const statusText = n.up ? "在线" : n.alive ? "进程在/端口无" : "离线";
  const badge = n.is_leader ? '<span class="topo-badge">L</span>' : "";
  const dragHandle = el.classList.contains("draggable")
    ? '<span class="topo-drag-handle" aria-hidden="true">⋮⋮</span>'
    : "";

  el.innerHTML = `
      <div class="topo-node-head">
        <span class="dot ${n.up ? "on" : "off"}"></span>
        <span class="topo-node-name">${escapeHtml(n.name)}</span>
        ${badge}
        ${clusterStateBadge(n)}
        ${dragHandle}
      </div>
      <div class="topo-node-addr">${escapeHtml(n.addr || "—")}</div>
      ${rateBlock(n)}
      ${diskBlock(n)}
      <div class="topo-node-meta">${escapeHtml(statusText)}${n.pid ? ` · pid ${n.pid}` : ""}${clusterStateMeta(n)}</div>`;
}

function boardNodeCards(gws: TopoNode[], stores: TopoNode[], metas: TopoNode[]): string {
  const parts: string[] = [];
  parts.push(clientCard());
  gws.forEach((n, i) => parts.push(nodeCard(n, i, true)));
  stores.forEach((n, i) => parts.push(nodeCard(n, i, true)));
  metas.forEach((n, i) => parts.push(nodeCard(n, i, true)));
  return parts.join("");
}

function clientCard(): string {
  return `
    <article class="topo-node soft-node draggable" data-name="client" data-role="client" title="拖拽移动">
      <div class="topo-node-head"><span class="topo-node-name">Client / skv</span></div>
      <div class="topo-node-meta">外部客户端</div>
    </article>`;
}

function applyBoardPositions(board: HTMLElement): void {
  const saved = loadPositions();
  const roleIdx: Record<string, number> = {};
  board.querySelectorAll<HTMLElement>(".topo-node.draggable").forEach((el) => {
    const name = el.dataset.name || "";
    const role = el.dataset.role || "";
    const idx = roleIdx[role] || 0;
    roleIdx[role] = idx + 1;
    const pos = saved[name] || defaultPosition(name, role, idx);
    placeNode(el, pos);
  });
}

function flowPill(flow: FlowEdge | undefined): string {
  if (!flow) return "";
  return `<span class="flow-pill ${escapeHtml(flow.kind)}">${escapeHtml(flow.label)} ${formatQps(flow.qps)}/s</span>`;
}

/** 合并双向边为 undirected lane，减少条数 */
function peerLanes(links: NodeLink[]): Array<{ a: string; b: string; kind: string; qps: number; count: number }> {
  const map = new Map<string, { a: string; b: string; kind: string; qps: number; count: number }>();
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

function laneBlock(
  title: string,
  lanes: Array<{ a: string; b: string; kind: string; qps: number; count: number }>,
  note: string,
): string {
  if (!lanes.length) {
    return `
      <div class="lane-block">
        <div class="lane-title">${escapeHtml(title)}</div>
        <div class="lane-empty">暂无互相同步</div>
        ${note ? `<p class="arch-store-note">${escapeHtml(note)}</p>` : ""}
      </div>`;
  }
  const rows = lanes
    .slice(0, 12)
    .map(
      (l) => `
      <div class="lane-row ${escapeHtml(l.kind)}">
        <span class="lane-end mono">${escapeHtml(l.a)}</span>
        <span class="lane-mid">
          <span class="lane-line"></span>
          <span class="lane-badge">${escapeHtml(l.kind === "sync" ? "同步" : "Raft")} ${formatQps(l.qps)}/s</span>
          <span class="lane-line"></span>
        </span>
        <span class="lane-end mono">${escapeHtml(l.b)}</span>
      </div>`,
    )
    .join("");
  return `
    <div class="lane-block">
      <div class="lane-title">${escapeHtml(title)} · ${lanes.length} 对</div>
      ${rows}
      ${note ? `<p class="arch-store-note">${escapeHtml(note)}</p>` : ""}
    </div>`;
}

function linkTable(links: NodeLink[]): string {
  if (!links.length) {
    return `<div class="link-table-wrap" id="link-table-wrap"><p class="topo-hint">暂无节点间流量</p></div>`;
  }
  const filters = [
    ["all", "全部"],
    ["store", "Store 间"],
    ["gateway", "网关"],
    ["meta", "Meta"],
    ["raft", "Raft"],
    ["sync", "同步"],
  ];
  return `
    <div class="link-table-wrap" id="link-table-wrap">
      <div class="link-table-head">
        <span>节点对明细（${links.length}）</span>
        <div class="link-filters">
          ${filters
            .map(
              ([id, label]) =>
                `<button type="button" class="link-filter${linkFilter === id ? " on" : ""}" data-filter="${id}">${label}</button>`,
            )
            .join("")}
        </div>
      </div>
      <table class="link-table">
        <thead><tr><th>类型</th><th>源</th><th></th><th>目标</th><th>QPS</th><th>累计</th></tr></thead>
        <tbody id="link-table-body"></tbody>
      </table>
    </div>`;
}

function filterLinks(links: NodeLink[], filter: string): NodeLink[] {
  return links.filter((l) => {
    if (filter === "all") return true;
    if (filter === "store") {
      return l.from.startsWith("storage") && l.to.startsWith("storage");
    }
    return l.kind === filter;
  });
}

function renderLinkTableBody(root: HTMLElement, links: NodeLink[], filter: string): void {
  const body = root.querySelector("#link-table-body");
  if (!body) return;
  const list = filterLinks(links, filter);
  body.innerHTML = list.length
    ? list
        .slice(0, 60)
        .map(
          (l) => `
        <tr class="kind-${escapeHtml(l.kind)}">
          <td><span class="kind-dot ${escapeHtml(l.kind)}"></span>${escapeHtml(l.label || l.kind)}</td>
          <td class="mono">${escapeHtml(l.from)}</td>
          <td class="arrow">→</td>
          <td class="mono">${escapeHtml(l.to)}</td>
          <td class="num">${formatQps(l.qps)}/s</td>
          <td class="num muted">${l.count ?? 0}</td>
        </tr>`,
        )
        .join("")
    : `<tr class="link-empty"><td colspan="6">该分类下暂无流量</td></tr>`;
}

function bindLinkFilters(root: HTMLElement, links: NodeLink[]): void {
  const wrap = root.querySelector("#link-table-wrap");
  if (!wrap) return;
  const btns = wrap.querySelectorAll<HTMLButtonElement>(".link-filter[data-filter]");
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

function findNode(name: string): TopoNode | undefined {
  if (!lastTopo) return undefined;
  for (const g of groupsOf(lastTopo)) {
    const n = g.nodes.find((x) => x.name === name);
    if (n) return n;
  }
  return undefined;
}

function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return "—";
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

function formatQps(n: number | undefined): string {
  if (n == null || !Number.isFinite(n) || n < 0) return "—";
  if (n < 10) return n.toFixed(1);
  if (n < 1000) return n.toFixed(0);
  return `${(n / 1000).toFixed(1)}k`;
}

function rateBlock(n: TopoNode): string {
  if (!n.up) {
    return `<div class="topo-rate muted">QPS —</div>`;
  }
  const r = n.rates || {};
  const chips: string[] = [];
  if (n.role === "gateway") {
    chips.push(chip("gateway", "入", r.gw));
    chips.push(chip("gateway", "→Store", r.to_store));
    chips.push(chip("meta", "→Meta", r.to_meta));
  } else if (n.role === "storage") {
    chips.push(chip("gateway", "网关", r.gw));
    chips.push(chip("meta", "Meta", r.meta_out));
    chips.push(chip("raft", "Raft", r.raft));
    chips.push(chip("sync", "同步", r.sync));
  } else if (n.role === "meta") {
    chips.push(chip("meta", "网关", r.gw));
    chips.push(chip("meta", "Store", r.store));
    chips.push(chip("raft", "Raft", (r.raft || 0) + (r.sync || 0)));
  }
  return `
    <div class="topo-rate">
      <span class="topo-rate-main">${formatQps(n.qps)}<small>/s</small></span>
    </div>
    <div class="topo-chips">${chips.join("")}</div>`;
}

function chip(kind: string, label: string, qps: number | undefined): string {
  return `<span class="topo-chip ${escapeHtml(kind)}">${escapeHtml(label)} ${formatQps(qps)}</span>`;
}

function diskBlock(n: TopoNode): string {
  const d = n.disk;
  if (!d?.ok || !d.total_bytes) return "";
  const pct = Math.min(100, Math.round((d.used_bytes / d.total_bytes) * 100));
  const level = pct >= 90 ? "crit" : pct >= 75 ? "warn" : "ok";
  return `
    <div class="topo-disk" title="${escapeHtml(d.path || "")}">
      <div class="topo-disk-row">
        <span>主机</span>
        <span>${formatBytes(d.used_bytes)} / ${formatBytes(d.total_bytes)} · ${pct}%</span>
      </div>
      <div class="topo-disk-bar"><i class="${level}" style="width:${pct}%"></i></div>
      <div class="topo-disk-row muted">
        <span>本节点</span>
        <span>${formatBytes(d.data_bytes)}</span>
      </div>
    </div>`;
}

const CLUSTER_STATE_CLS = ["st-joining", "st-active", "st-draining", "st-offline", "st-tombstone"] as const;

function applyClusterStateClass(el: HTMLElement, state?: string): void {
  for (const c of CLUSTER_STATE_CLS) el.classList.remove(c);
  const key = (state || "").toLowerCase();
  if (key) el.classList.add(`st-${key}`);
}

function clusterStateBadge(n: TopoNode): string {
  if (!n.cluster_state && !n.state_label) return "";
  const key = (n.cluster_state || "").toLowerCase();
  const label = n.state_label || n.cluster_state || "";
  return `<span class="state-pill ${key}" title="${escapeHtml(n.cluster_state || "")}">${escapeHtml(label)}</span>`;
}

function clusterStateMeta(n: TopoNode): string {
  if (!n.cluster_state) return "";
  const label = n.state_label || n.cluster_state;
  return ` · ${escapeHtml(label)}`;
}

function nodeCard(n: TopoNode, delayIdx: number, onBoard = false): string {
  const state = n.up ? "up" : "down";
  const leader = n.is_leader ? "leader" : "";
  const stCls = n.cluster_state ? `st-${n.cluster_state.toLowerCase()}` : "";
  const badge = n.is_leader ? '<span class="topo-badge">L</span>' : "";
  const statusDot = `<span class="dot ${n.up ? "on" : "off"}"></span>`;
  const style = `animation-delay:${Math.min(delayIdx, 8) * 40}ms`;
  const statusText = n.up ? "在线" : n.alive ? "进程在/端口无" : "离线";
  const dragCls = onBoard ? "draggable" : "";
  const title = onBoard ? "拖拽移动 · 单击操作" : "点击操作";

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
        ${onBoard ? '<span class="topo-drag-handle" aria-hidden="true">⋮⋮</span>' : ""}
      </div>
      <div class="topo-node-addr">${escapeHtml(n.addr || "—")}</div>
      ${rateBlock(n)}
      ${diskBlock(n)}
      <div class="topo-node-meta">${escapeHtml(statusText)}${n.pid ? ` · pid ${n.pid}` : ""}${clusterStateMeta(n)}</div>
    </article>`;
}

function ensurePopover(): void {
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

function openPopover(anchor: HTMLElement, node: TopoNode, controllable: boolean): void {
  const pop = document.getElementById("topo-popover");
  const backdrop = document.getElementById("topo-popover-backdrop");
  if (!pop || !backdrop) return;

  const stopDis = !controllable || !node.alive;
  const startDis = !controllable || node.up;
  const allDis = !controllable;
  const hint = controllable ? "选择要对节点执行的操作" : "未配置 -run-dir，无法上下线";
  const r = node.rates || {};
  const rate = `<div class="topo-popover-disk">
      <div>总速率 ${formatQps(node.qps)}/s</div>
      ${node.role === "gateway" ? `<div class="flow-line gateway">客户端 ${formatQps(r.gw)} · →Store ${formatQps(r.to_store)} · →Meta ${formatQps(r.to_meta)}</div>` : ""}
      ${node.role === "storage" ? `<div class="flow-line gateway">网关 ${formatQps(r.gw)}</div>
        <div class="flow-line meta">Meta 出站 ${formatQps(r.meta_out)}</div>
        <div class="flow-line raft">Raft ${formatQps(r.raft)}</div>
        <div class="flow-line sync">数据同步 ${formatQps(r.sync)}</div>` : ""}
      ${node.role === "meta" ? `<div class="flow-line meta">网关 ${formatQps(r.gw)} · Store ${formatQps(r.store)}</div>
        <div class="flow-line raft">Raft ${formatQps((r.raft || 0) + (r.sync || 0))}</div>` : ""}
      <div class="muted">累计 ${node.req_total ?? 0}</div>
    </div>`;
  const disk = node.disk?.ok
    ? `<div class="topo-popover-disk">
        <div>主机磁盘 ${formatBytes(node.disk.used_bytes)} / ${formatBytes(node.disk.total_bytes)}</div>
        <div>本节点数据 ${formatBytes(node.disk.data_bytes)}</div>
        <div class="muted">${escapeHtml(node.disk.path || "")}</div>
      </div>`
    : "";

  pop.innerHTML = `
    <div class="topo-popover-head">
      <div>
        <div class="topo-popover-title">
          <span class="dot ${node.up ? "on" : "off"}"></span>
          ${escapeHtml(node.name)}
          ${node.is_leader ? '<span class="topo-badge">L</span>' : ""}
          ${clusterStateBadge(node)}
        </div>
        <div class="topo-popover-sub">${escapeHtml(node.role)} · ${escapeHtml(node.addr || "—")}${
          node.cluster_state
            ? ` · 状态 ${escapeHtml(node.state_label || node.cluster_state)} (${escapeHtml(node.cluster_state)})`
            : ""
        }</div>
      </div>
      <button type="button" class="topo-popover-close" aria-label="关闭">×</button>
    </div>
    ${rate}
    ${disk}
    <p class="topo-popover-hint">${escapeHtml(hint)}</p>
    <div class="topo-popover-actions">
      ${
        node.role === "storage"
          ? (() => {
              const st = (node.cluster_state || "").toLowerCase();
              const bits: string[] = [];
              if (st === "joining" || st === "offline" || !node.up || st === "tombstone") {
                bits.push(`<button type="button" class="topo-act ok" data-store-action="online">上线</button>`);
              }
              if (st === "active" || st === "offline" || (st === "joining" && node.alive)) {
                bits.push(`<button type="button" class="topo-act danger" data-store-action="offline">下线</button>`);
              }
              if (st === "draining") {
                bits.push(`<span class="muted">缩容中…</span>`);
              }
              if (st === "tombstone" || st === "draining" || (st === "joining" && !node.alive) || st === "offline") {
                bits.push(`<button type="button" class="topo-act danger" data-store-action="kick" title="须已下线且无副本">踢出</button>`);
              }
              // Meta / Gateway：保留进程上下线
              if (bits.length) return bits.join("");
              return "";
            })()
          : `<button type="button" class="topo-act ok" data-action="start" ${startDis || allDis ? "disabled" : ""}>上线</button>
      <button type="button" class="topo-act danger" data-action="stop" ${stopDis || allDis ? "disabled" : ""}>下线</button>
      <button type="button" class="topo-act" data-action="restart" ${allDis ? "disabled" : ""}>重启</button>`
      }
    </div>
  `;

  pop.querySelector(".topo-popover-close")?.addEventListener("click", closePopover);
  pop.querySelectorAll<HTMLButtonElement>("[data-store-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const action = btn.dataset.storeAction;
      closePopover();
      if (action && lastOnStoreAction) lastOnStoreAction(action, node.name);
    });
  });
  pop.querySelectorAll<HTMLButtonElement>("[data-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const action = btn.dataset.action as NodeAction | undefined;
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

export function closePopover(): void {
  const pop = document.getElementById("topo-popover");
  const backdrop = document.getElementById("topo-popover-backdrop");
  if (pop) pop.hidden = true;
  if (backdrop) backdrop.hidden = true;
}
