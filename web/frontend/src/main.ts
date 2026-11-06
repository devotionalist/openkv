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

import { api, postJSON } from "./api";
import { $, bindForm, setTab } from "./dom";
import { applyTopology, bindRegionStrategy, renderNodes, renderOverview, renderRegions } from "./render";
import { refreshScaleTasks } from "./regions";
import type { NodeAction } from "./topology";
import type {
  NodesResponse,
  OpResult,
  Overview,
  RouteTable,
} from "./types";

let busy = false;

async function refresh(): Promise<void> {
  const errors: string[] = [];
  let route: RouteTable | null = null;
  try {
    const ov = await api<Overview>("/api/overview");
    renderOverview(ov, handleNodeAction, handleStoreAction);
  } catch (e) {
    errors.push(`overview: ${e instanceof Error ? e.message : String(e)}`);
  }
  try {
    route = await api<RouteTable>("/api/route");
    renderRegions(route);
  } catch (e) {
    errors.push(`route: ${e instanceof Error ? e.message : String(e)}`);
  }
  try {
    const nodes = await api<NodesResponse>("/api/nodes");
    renderNodes(nodes, handleStoreAction, route);
  } catch (e) {
    errors.push(`nodes: ${e instanceof Error ? e.message : String(e)}`);
  }
  await refreshScaleTasks();
  if (errors.length) {
    console.error(errors);
    $("#fetched-at").textContent = `部分刷新失败: ${errors.join(" · ")}`;
  }
}

async function handleNodeAction(name: string, action: NodeAction): Promise<void> {
  if (busy) return;
  const label = action === "start" ? "上线" : action === "stop" ? "下线" : "重启";
  if (!confirm(`${label}节点 ${name}？`)) return;
  busy = true;
  const log = $("#topo-log");
  log.textContent = `${label} ${name}…`;
  try {
    const r = await postJSON<OpResult>(`/api/node/${action}`, { name });
    log.textContent = JSON.stringify(r, null, 2);
    if (r.topology) {
      applyTopology(r.topology, handleNodeAction, handleStoreAction);
    }
    await refresh();
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    log.textContent = `失败: ${msg}`;
    alert(msg);
  } finally {
    busy = false;
  }
}

function opsLog(): HTMLElement {
  return document.getElementById("ops-log") || $("#nodes-log");
}

async function handleStoreAction(action: string, nodeId: string): Promise<void> {
  if (busy) return;
  const log = opsLog();
  const labels: Record<string, string> = {
    online: "上线",
    offline: "下线",
    kick: "踢出",
  };
  const label = labels[action];
  if (!label) return;
  const hints: Record<string, string> = {
    online: `上线 ${nodeId}？拉起进程；若为待加入/Offline 则 JOIN → Active。`,
    offline: `下线 ${nodeId}？在用节点会进入 Draining 并迁出副本；待加入仅停进程。迁空后才能踢出。`,
    kick: `踢出 ${nodeId}？须已下线且无 Region，将擦除 Meta 元数据并停进程。`,
  };
  if (!confirm(hints[action] || `${label} ${nodeId}？`)) return;
  busy = true;
  try {
    const r = await postJSON<OpResult>(`/api/node-${action}`, { node_id: nodeId });
    log.textContent = (r.logs || [JSON.stringify(r, null, 2)]).join("\n");
    if (r.ok === false) {
      alert(r.hint || r.response || r.error || `${label}失败`);
    }
    if (r.topology) {
      applyTopology(r.topology, handleNodeAction, handleStoreAction);
    }
    await refresh();
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    log.textContent = `失败: ${msg}`;
    alert(msg);
  } finally {
    busy = false;
  }
}

function boot(): void {
  $("#tabs").addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLButtonElement>("button[data-tab]");
    if (btn?.dataset.tab) setTab(btn.dataset.tab);
  });

  $("#btn-refresh").addEventListener("click", () => {
    void refresh();
  });

  $("#btn-advance").addEventListener("click", async () => {
    try {
      const r = await postJSON<OpResult>("/api/advance-safe", {});
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
      alert("请填写 Store ID");
      return;
    }
    if (!confirm(`加入 Store ${nodeId}？将检查可达性与心跳后升为 Active。`)) return;
    const r = await postJSON<OpResult>("/api/join-node", { node_id: nodeId });
    opsLog().textContent = JSON.stringify(r, null, 2);
    if (!r.ok) alert(r.hint || r.response || "加入失败（节点不可用）");
    await refresh();
  });

  bindForm("#form-balance", async (fd) => {
    const r = await postJSON<OpResult>("/api/balance", {
      count: Number(fd.get("count")) || 1,
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
      value: String(fd.get("value")),
    });
    $("#console-log").textContent = JSON.stringify(r, null, 2);
  });

  bindForm("#form-locate", async (fd) => {
    const key = encodeURIComponent(String(fd.get("key")));
    const r = await api(`/api/locate?key=${key}`);
    $("#console-log").textContent = JSON.stringify(r, null, 2);
  });

  const runDS = async (body: Record<string, unknown>) => {
    const r = await postJSON("/api/ds", body);
    $("#console-log").textContent = JSON.stringify(r, null, 2);
  };

  bindForm("#form-ds-hash", async (fd) => {
    await runDS({
      op: String(fd.get("op") || "hgetall"),
      key: String(fd.get("key")),
      field: String(fd.get("field") || ""),
      value: String(fd.get("value") || ""),
    });
  });
  bindForm("#form-ds-set", async (fd) => {
    await runDS({
      op: String(fd.get("op") || "smembers"),
      key: String(fd.get("key")),
      member: String(fd.get("member") || ""),
    });
  });
  bindForm("#form-ds-list", async (fd) => {
    await runDS({
      op: String(fd.get("op") || "lrange"),
      key: String(fd.get("key")),
      value: String(fd.get("value") || ""),
      start: Number(fd.get("start") || 0),
      stop: Number(fd.get("stop") ?? -1),
    });
  });
  bindForm("#form-ds-zset", async (fd) => {
    await runDS({
      op: String(fd.get("op") || "zrange"),
      key: String(fd.get("key")),
      member: String(fd.get("member") || ""),
      score: Number(fd.get("score") || 0),
      start: Number(fd.get("start") || 0),
      stop: Number(fd.get("stop") ?? -1),
    });
  });

  void refresh();
  window.setInterval(() => void refresh(), 8000);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
