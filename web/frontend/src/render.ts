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

import { $, escapeHtml } from "./dom";
import { renderTopology, type NodeAction } from "./topology";
import type {
  NodeInfo,
  NodesResponse,
  Overview,
  Region,
  RouteTable,
  Topology,
} from "./types";

import { listPendingReplicaKicks, regionActionButtons } from "./regions";
export { bindRegionStrategy, renderRegions } from "./regions";

export function renderOverview(
  ov: Overview,
  onNodeAction: (name: string, action: NodeAction) => void,
  onStoreAction?: (action: string, nodeId: string) => void,
): void {
  const metaOk = ov.meta_up === ov.meta_total && ov.meta_total > 0;
  const stOk = ov.storage_up >= Math.ceil((ov.storage_total * 2) / 3);
  const stats: Array<[string, string, string]> = [
    ["Meta", `${ov.meta_up}/${ov.meta_total}`, metaOk ? "good" : "bad"],
    ["Storage", `${ov.storage_up}/${ov.storage_total}`, stOk ? "good" : "warn"],
    ["Gateway", ov.gateway_up ? "UP" : "DOWN", ov.gateway_up ? "good" : "bad"],
    ["Regions", String(ov.region_count), ""],
    ["Route ver", String(ov.route_version), ""],
    ["next_ts", String(ov.next_ts ?? "—"), ""],
  ];
  $("#stat-row").innerHTML = stats
    .map(
      ([label, value, cls]) =>
        `<div class="stat"><div class="label">${label}</div><div class="value ${cls}">${value}</div></div>`,
    )
    .join("");

  $("#tso-dl").innerHTML = `
    <dt>gc_safe_ts</dt><dd>${ov.gc_safe_ts ?? "—"}</dd>
    <dt>next_ts</dt><dd>${ov.next_ts ?? "—"}</dd>
    <dt>min_start_hint</dt><dd>${ov.min_start_hint ?? "—"}</dd>
    <dt>meta leader</dt><dd>${escapeHtml(ov.who_leader || "—")}</dd>
  `;
  $("#fetched-at").textContent = ov.fetched_at || "";

  const hint = $("#topo-hint");
  const topo = ov.topology;
  if (topo?.controllable) {
    hint.textContent = "拖拽节点改位置 · 单击打开上线/下线/重启";
  } else {
    hint.textContent = "拖拽节点改位置 · 配置 -run-dir 后可上下线";
  }

  renderTopology($("#topology"), topo, onNodeAction, onStoreAction);
}

export function applyTopology(
  topo: Topology,
  onNodeAction: (name: string, action: NodeAction) => void,
  onStoreAction?: (action: string, nodeId: string) => void,
): void {
  renderTopology($("#topology"), topo, onNodeAction, onStoreAction);
}

function stateKey(state: string): string {
  return state.toLowerCase();
}

function stateLabel(state: string): string {
  switch (stateKey(state)) {
    case "joining":
      return "待加入";
    case "active":
      return "在用";
    case "draining":
      return "缩容中";
    case "offline":
      return "心跳超时";
    case "tombstone":
      return "已腾空";
    default:
      return state;
  }
}

function statePill(state: string): string {
  const key = stateKey(state);
  const label = stateLabel(state);
  return `<span class="state-pill ${escapeHtml(key)}" title="${escapeHtml(state)}">${escapeHtml(label)} <small>${escapeHtml(state)}</small></span>`;
}

function isPending(n: NodeInfo): boolean {
  return stateKey(n.state) === "joining";
}

/** 展开中的 Region（刷新后保持） */
const expandedRegions = new Set<number>();

/** 节点仅三操作：上线 / 下线 / 踢出（踢出须已下线且无副本） */
function storeLifecycleActions(n: NodeInfo): string {
  if (n.role !== "storage") return "—";
  const st = stateKey(n.state);
  const regions = n.regions ?? 0;
  const bits: string[] = [];

  const canOnline = st === "joining" || st === "offline" || (!n.up && st !== "tombstone" && st !== "draining");
  // Active / Offline → Draining；Joining 且进程在跑 → 停进程
  const canOffline =
    st === "active" || st === "offline" || (st === "joining" && !!n.up);
  // 踢出须已下线：Tombstone，或 Draining/Offline 且无副本，或 Joining 已停进程
  const canKick =
    st === "tombstone" ||
    ((st === "draining" || st === "offline") && regions === 0) ||
    (st === "joining" && !n.up);

  if (canOnline && st !== "draining") {
    bits.push(
      `<button type="button" class="ghost sm" data-store-action="online" data-node-id="${escapeHtml(n.id)}">上线</button>`,
    );
  }
  if (canOffline) {
    bits.push(
      `<button type="button" class="ghost sm danger-text" data-store-action="offline" data-node-id="${escapeHtml(n.id)}" title="Active→Draining 迁副本；Joining→停进程">下线</button>`,
    );
  }
  if (st === "draining") {
    bits.push(
      `<span class="muted" title="regions=${regions}">缩容中…</span>`,
    );
  }
  if (canKick) {
    bits.push(
      `<button type="button" class="ghost sm danger-text" data-store-action="kick" data-node-id="${escapeHtml(n.id)}" title="须已下线且无副本">踢出</button>`,
    );
  } else if (st === "draining" && regions > 0) {
    bits.push(`<span class="muted">迁空后可踢出 (${regions})</span>`);
  }

  return bits.length ? `<div class="row-actions">${bits.join("")}</div>` : "—";
}

function formatRange(r: Region): string {
  const a = r.start_key || "−∞";
  const b = r.end_key || "+∞";
  return `${escapeHtml(a)} … ${escapeHtml(b)}`;
}

/** Region 副本操作：只动本 Region 的 peer，不整机下线 */
function regionReplicaActions(rid: number, nodeId: string, addr: string, pendingKick: boolean): string {
  if (pendingKick) {
    return `<div class="row-actions">
      <span class="muted">已下线</span>
      <button type="button" class="ghost sm danger-text" data-replica-act="kick" data-rid="${rid}" data-node-id="${escapeHtml(nodeId)}" data-address="${escapeHtml(addr)}">踢出</button>
    </div>`;
  }
  return `<div class="row-actions">
    <button type="button" class="ghost sm danger-text" data-replica-act="offline" data-rid="${rid}" data-node-id="${escapeHtml(nodeId)}" title="从本 Region 摘除该副本（非整机）">下线</button>
  </div>`;
}

function regionReplicaSubtable(r: Region, nodeById: Map<string, NodeInfo>): string {
  const pending = listPendingReplicaKicks(r.id);
  const liveIds = new Set((r.replicas || []).map((x) => x.node_id));
  const liveRows = (r.replicas || []).map((rep) => {
    const n = nodeById.get(rep.node_id);
    const state = n?.state || "—";
    const up = n?.up;
    const role = rep.is_leader
      ? '<span class="region-chip leader">Leader</span>'
      : '<span class="region-chip">Follower</span>';
    const addr = rep.address || n?.address || "";
    return `<tr>
        <td>${escapeHtml(rep.node_id)}</td>
        <td>${escapeHtml(addr || "—")}</td>
        <td>${n ? statePill(state) : escapeHtml(state)}</td>
        <td>${role}</td>
        <td><span class="dot ${up ? "on" : "off"}"></span>${up ? "UP" : up === false ? "DOWN" : "—"}</td>
        <td>${regionReplicaActions(r.id, rep.node_id, addr, false)}</td>
      </tr>`;
  });
  const pendingRows = pending
    .filter((p) => !liveIds.has(p.node_id))
    .map(
      (p) => `<tr class="replica-pending-kick">
        <td>${escapeHtml(p.node_id)}</td>
        <td>${escapeHtml(p.address || "—")}</td>
        <td><span class="state-pill offline">已下线 <small>待踢出</small></span></td>
        <td>—</td>
        <td>—</td>
        <td>${regionReplicaActions(r.id, p.node_id, p.address, true)}</td>
      </tr>`,
    );
  const rows = [...liveRows, ...pendingRows].join("");
  return `<table class="region-child">
    <thead><tr><th>节点</th><th>地址</th><th>状态</th><th>角色</th><th>可达</th><th>副本操作</th></tr></thead>
    <tbody>${rows || emptyRow(6, "无副本")}</tbody>
  </table>`;
}

function renderRegionMasterRows(
  regions: Region[],
  nodeById: Map<string, NodeInfo>,
): string {
  if (!regions.length) return emptyRow(7, "暂无 Region");
  return regions
    .map((r) => {
      const open = expandedRegions.has(r.id);
      const leader = (r.replicas || []).find((x) => x.is_leader)?.node_id || "—";
      const nPeer = (r.replicas || []).length;
      return `
      <tr class="region-row ${open ? "open" : ""}" data-region-toggle="${r.id}" role="button" tabindex="0">
        <td class="expand-cell">${open ? "▼" : "▶"}</td>
        <td><span class="region-chip">R${r.id}</span></td>
        <td class="mono">${formatRange(r)}</td>
        <td>${r.epoch ?? "—"}</td>
        <td>${nPeer}</td>
        <td class="mono">${escapeHtml(leader)}</td>
        <td>${regionActionButtons(r.id)}</td>
      </tr>
      <tr class="region-detail ${open ? "" : "hidden"}" data-region-detail="${r.id}">
        <td colspan="7">${regionReplicaSubtable(r, nodeById)}</td>
      </tr>`;
    })
    .join("");
}

function pendingStoreActions(n: NodeInfo): string {
  return storeLifecycleActions(n);
}

function bindStoreActions(
  root: HTMLElement,
  onStoreAction?: (action: string, nodeId: string) => void,
): void {
  if (!onStoreAction) return;
  root.querySelectorAll<HTMLButtonElement>("[data-store-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const action = btn.dataset.storeAction;
      const nodeId = btn.dataset.nodeId;
      if (action && nodeId) onStoreAction(action, nodeId);
    });
  });
}

function emptyRow(cols: number, msg: string): string {
  return `<tr><td colspan="${cols}" class="muted">${escapeHtml(msg)}</td></tr>`;
}

export function renderNodes(
  data: NodesResponse,
  onStoreAction?: (action: string, nodeId: string) => void,
  route?: RouteTable | null,
): void {
  const nodes = data.nodes || [];
  const pending = nodes.filter(isPending);
  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  const regions = route?.regions || [];

  const legend = $("#node-state-legend");
  if (legend) {
    legend.innerHTML = [
      ["joining", "待加入"],
      ["active", "在用"],
      ["draining", "缩容中"],
      ["offline", "心跳超时"],
      ["tombstone", "已腾空"],
    ]
      .map(([k, lab]) => `<span class="state-pill ${k}">${lab} <small>${k}</small></span>`)
      .join("");
  }

  const activeBody = $("#node-table-active tbody");
  activeBody.innerHTML = renderRegionMasterRows(regions, nodeById);

  activeBody.querySelectorAll<HTMLElement>("[data-region-toggle]").forEach((row) => {
    const toggle = () => {
      const id = Number(row.dataset.regionToggle);
      if (Number.isNaN(id)) return;
      if (expandedRegions.has(id)) expandedRegions.delete(id);
      else expandedRegions.add(id);
      renderNodes(data, onStoreAction, route);
    };
    row.addEventListener("click", (e) => {
      // 行内操作按钮 / 操作区：不触发展开，交给 #panel-ops 委托处理
      const t = e.target as HTMLElement;
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
  pendingBody.innerHTML = pending.length
    ? pending
        .map(
          (n) => `<tr class="row-state-joining" data-state="${escapeHtml(n.state)}">
      <td>${escapeHtml(n.id)}</td>
      <td>${escapeHtml(n.role)}</td>
      <td>${escapeHtml(n.address)}</td>
      <td>${statePill(n.state)}</td>
      <td><span class="dot ${n.up ? "on" : "off"}"></span>${n.up ? "UP" : "DOWN"}</td>
      <td>${pendingStoreActions(n)}</td>
    </tr>`,
        )
        .join("")
    : emptyRow(6, "暂无待加入节点（新 Store REGISTER 后出现在此）");

  const dl = document.getElementById("pending-node-ids");
  if (dl) {
    dl.innerHTML = pending.map((n) => `<option value="${escapeHtml(n.id)}"></option>`).join("");
  }
  const joinInput = document.querySelector<HTMLInputElement>('#form-nodes-join input[name="node_id"]');
  if (joinInput && !joinInput.value && pending[0]) {
    joinInput.placeholder = pending[0].id;
  }

  // Region 表内是副本操作（regions.ts 委托）；Store 三操作只在待加入表
  bindStoreActions(pendingBody, onStoreAction);
}
