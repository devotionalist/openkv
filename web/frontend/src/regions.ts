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
import { $, escapeHtml } from "./dom";
import { openConfirm, openModal } from "./modal";
import type { OpResult, Region, RouteTable, ScaleTaskView } from "./types";

const COLORS = ["#1f6f6a", "#2a5f8a", "#4a6b3c", "#6b4e2e", "#3d4f7a", "#2d6b55"];

let lastRegions: Region[] = [];
let onAfterOp: (() => Promise<void>) | null = null;
let bound = false;
let scalePollTimer: number | null = null;

/** 副本已从 Region 成员摘除、待 DESTROY 的本地清盘队列（会话内） */
type PendingKick = { region_id: number; node_id: string; address: string };
const pendingReplicaKicks = new Map<string, PendingKick>();

function pendingKey(rid: number, nodeId: string): string {
  return `${rid}:${nodeId}`;
}

export function listPendingReplicaKicks(regionId: number): PendingKick[] {
  return [...pendingReplicaKicks.values()].filter((p) => p.region_id === regionId);
}

export function rememberPendingKick(p: PendingKick): void {
  pendingReplicaKicks.set(pendingKey(p.region_id, p.node_id), p);
}

export function forgetPendingKick(regionId: number, nodeId: string): void {
  pendingReplicaKicks.delete(pendingKey(regionId, nodeId));
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => window.setTimeout(r, ms));
}

function formatScaleLine(t: ScaleTaskView): string {
  const pct =
    t.catchup_pct != null
      ? t.catchup_pct
      : t.progress_pct != null
        ? t.progress_pct
        : Math.round((t.progress || 0) * 100);
  const shards = (t.shards || []).map((x) => `R${x}`).join(",") || "—";
  const bits = [
    `${t.task_id || "?"} · ${t.state_zh || t.state_name || t.state}`,
    `node=${t.node_id || "?"}`,
    `shards=${shards}`,
    `${pct}%`,
  ];
  if (t.match != null && t.commit != null) {
    bits.push(`match=${t.match}/${t.commit}${t.caught ? " 已追平" : ""}`);
  }
  if (t.error) bits.push(`err=${t.error}`);
  return bits.join(" · ");
}

export function renderScaleTasks(tasks: ScaleTaskView[]): void {
  const pane = document.getElementById("scale-progress-pane");
  const list = document.getElementById("scale-progress-list");
  if (!pane || !list) return;
  const show = tasks.filter((t) => t.active || (!t.done && !t.failed) || t.catching_up);
  // 也保留最近刚完成的，便于看到「已追平」
  const recentDone = tasks.filter((t) => t.done || t.failed).slice(-3);
  const view = [...show, ...recentDone.filter((d) => !show.some((s) => s.task_id === d.task_id))];
  if (!view.length) {
    pane.hidden = true;
    list.innerHTML = "";
    return;
  }
  pane.hidden = false;
  list.innerHTML = view
    .map((t) => {
      const pct =
        t.catchup_pct != null
          ? t.catchup_pct
          : t.progress_pct != null
            ? t.progress_pct
            : Math.round((t.progress || 0) * 100);
      const cls = t.failed ? "failed" : t.done ? "done" : "";
      const stateCls = t.failed ? "failed" : t.done ? "done" : "";
      const meta =
        t.match != null && t.commit != null
          ? `Raft 追平 match=${t.match} / commit=${t.commit}${t.caught ? " · 已追平" : ""}`
          : t.error
            ? `错误: ${escapeHtml(t.error)}`
            : t.catching_up
              ? "等待 Learner 日志追平…"
              : "";
      return `<div class="scale-card ${cls}">
        <div class="scale-card-head">
          <span class="tid">${escapeHtml(t.task_id || "")}</span>
          <span class="state ${stateCls}">${escapeHtml(t.state_zh || t.state_name || "")}</span>
          <span>${escapeHtml(t.node_id || "")} → ${(t.shards || []).map((x) => `R${x}`).join(", ") || "—"}</span>
          <span>${pct}%</span>
        </div>
        <div class="scale-bar" role="progressbar" aria-valuenow="${pct}" aria-valuemin="0" aria-valuemax="100"><i style="width:${pct}%"></i></div>
        ${meta ? `<div class="scale-meta">${meta}</div>` : ""}
      </div>`;
    })
    .join("");
}

export async function refreshScaleTasks(): Promise<void> {
  try {
    const data = await api<{ tasks?: ScaleTaskView[] }>("/api/scale-tasks");
    const tasks = data.tasks || [];
    renderScaleTasks(tasks);
    const active = tasks.some((t) => t.active || t.catching_up || (t.state != null && t.state < 5));
    if (active && scalePollTimer == null) {
      scalePollTimer = window.setInterval(() => {
        void refreshScaleTasks();
      }, 800);
    } else if (!active && scalePollTimer != null) {
      window.clearInterval(scalePollTimer);
      scalePollTimer = null;
    }
  } catch {
    /* Meta 不可达时忽略 */
  }
}

async function pollScaleTask(taskId: string, regionId: number, nodeId: string): Promise<void> {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    try {
      const t = await api<ScaleTaskView>(`/api/scale-task?task_id=${encodeURIComponent(taskId)}`);
      setLog([`加副本 R${regionId} → ${nodeId}`, formatScaleLine(t)].join("\n"));
      await refreshScaleTasks();
      if (t.done || t.failed) {
        if (t.failed) {
          setLog(`加副本失败: ${t.error || t.raw || formatScaleLine(t)}`);
        } else {
          setLog(`加副本完成（已追平并 Promote）\n${formatScaleLine(t)}`);
        }
        await onAfterOp?.();
        return;
      }
    } catch (e) {
      setLog(`查进度失败: ${e instanceof Error ? e.message : String(e)}`);
    }
    await sleep(600);
  }
  setLog(`加副本超时（仍可在上方进度面板查看 ${taskId}）`);
  await refreshScaleTasks();
}

export function bindRegionStrategy(refresh: () => Promise<void>): void {
  onAfterOp = refresh;
  if (bound) return;
  bound = true;

  const panel = $("#panel-ops");
  panel.addEventListener("click", (e) => {
    const t = e.target as HTMLElement;
    const replicaBtn = t.closest<HTMLButtonElement>("button[data-replica-act]");
    if (replicaBtn) {
      e.stopPropagation();
      const rid = Number(replicaBtn.dataset.rid);
      const nodeId = replicaBtn.dataset.nodeId || "";
      const address = replicaBtn.dataset.address || "";
      const action = replicaBtn.dataset.replicaAct || "";
      void runReplicaAction(rid, nodeId, address, action);
      return;
    }
    const act = t.closest<HTMLButtonElement>("button[data-region-act]");
    if (!act) return;
    e.stopPropagation();
    const rid = Number(act.dataset.rid);
    const action = act.dataset.regionAct || "";
    void runRegionAction(rid, action);
  });
}

async function runReplicaAction(
  rid: number,
  nodeId: string,
  address: string,
  action: string,
): Promise<void> {
  if (!nodeId) return;
  if (action === "offline") {
    const ok = await openConfirm({
      title: `下线副本 · R${rid}`,
      body: `从 Region ${rid} 成员中摘除 ${nodeId}（非整机下线）。摘除后请再「踢出」销毁本地数据。`,
      confirmLabel: "下线副本",
    });
    if (!ok) return;
    setLog(`下线副本 R${rid} / ${nodeId}…`);
    try {
      const resp = await postJSON<OpResult & { pending_kick?: boolean; address?: string }>(
        "/api/replica-offline",
        { region_id: rid, node_id: nodeId },
      );
      setLog((resp.logs || [JSON.stringify(resp)]).join("\n"));
      if (resp.ok && resp.pending_kick) {
        rememberPendingKick({
          region_id: rid,
          node_id: nodeId,
          address: resp.address || address,
        });
      }
      if (!resp.ok) alert(resp.hint || "下线副本失败");
      await onAfterOp?.();
    } catch (e) {
      setLog(`失败: ${e instanceof Error ? e.message : String(e)}`);
    }
    return;
  }
  if (action === "kick") {
    const ok = await openConfirm({
      title: `踢出副本 · R${rid}`,
      body: `销毁 ${nodeId} 上 Region ${rid} 的本地数据（Store 进程仍在集群）。须已先下线。`,
      confirmLabel: "踢出",
    });
    if (!ok) return;
    setLog(`踢出副本 R${rid} / ${nodeId}…`);
    try {
      const resp = await postJSON<OpResult>("/api/replica-kick", {
        region_id: rid,
        node_id: nodeId,
        address,
      });
      setLog((resp.logs || [JSON.stringify(resp)]).join("\n"));
      if (resp.ok) forgetPendingKick(rid, nodeId);
      if (!resp.ok) alert(resp.hint || "踢出失败");
      await onAfterOp?.();
    } catch (e) {
      setLog(`失败: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
}

/** 仅更新 key 空间条带；Region 主表由 renderNodes 负责 */
export function renderRegions(data: RouteTable): void {
  lastRegions = data.regions || [];
  const map = document.getElementById("range-map");
  if (!map) return;
  if (!lastRegions.length) {
    map.innerHTML = '<div class="range-seg" style="flex:1;opacity:.5">无 Region</div>';
    return;
  }
  map.innerHTML = lastRegions
    .map((r, i) => {
      const bg = COLORS[i % COLORS.length];
      return `<div class="range-seg" data-region-id="${r.id}" style="background:${bg}" title="${escapeHtml(
        r.start_key,
      )} → ${escapeHtml(r.end_key)}">R${r.id}</div>`;
    })
    .join("");
}

export function regionActionButtons(rid: number): string {
  return `<div class="region-acts">
    <button type="button" class="ghost btn-xs" data-region-act="split" data-rid="${rid}">分裂</button>
    <button type="button" class="ghost btn-xs" data-region-act="merge-right" data-rid="${rid}">合并右邻</button>
    <button type="button" class="ghost btn-xs" data-region-act="add-replica" data-rid="${rid}">增加副本</button>
    <button type="button" class="ghost btn-xs" data-region-act="place" data-rid="${rid}">放置</button>
    <button type="button" class="ghost btn-xs" data-region-act="suggest-place" data-rid="${rid}">建议放置</button>
  </div>`;
}

function findRegion(id: number): Region | undefined {
  return lastRegions.find((r) => r.id === id);
}

function suggestSplitKey(r: Region): string {
  const s = r.start_key;
  const e = r.end_key;
  if (s && s !== "−∞" && !s.startsWith("0x") && e && e !== "+∞" && !e.startsWith("0x")) {
    if (s.length === 1 && e.length >= 1) {
      const mid = String.fromCharCode(Math.floor((s.charCodeAt(0) + e.charCodeAt(0)) / 2));
      if (mid > s && (e === "+∞" || mid < e)) return mid;
    }
  }
  if (!s || s === "−∞") return "m";
  return s + "m";
}

function rangeLabel(r: Region): string {
  return `[${r.start_key || "−∞"}, ${r.end_key || "+∞"})`;
}

function setLog(text: string): void {
  const el = document.getElementById("ops-log") || document.getElementById("nodes-log");
  if (el) el.textContent = text;
}

async function runRegionAction(rid: number, action: string): Promise<void> {
  const r = findRegion(rid);
  if (!r) {
    setLog(`未知 Region ${rid}`);
    return;
  }

  if (action === "split") {
    const modal = await openModal({
      title: `分裂 Region ${rid}`,
      subtitle: `区间 ${rangeLabel(r)} · 仅切元数据，不迁副本（无感扩容走 join + balance）`,
      confirmLabel: "执行分裂",
      fields: [
        {
          name: "split_key",
          label: "Split key",
          value: suggestSplitKey(r),
          placeholder: "m",
          required: true,
        },
      ],
    });
    if (!modal.ok) return;
    const key = String(modal.values.split_key || "").trim();
    setLog(`分裂 R${rid}…`);
    try {
      const resp = await postJSON<OpResult>("/api/split", {
        region_id: rid,
        split_key: key,
      });
      setLog((resp.logs || [JSON.stringify(resp)]).join("\n"));
      await onAfterOp?.();
    } catch (e) {
      setLog(`失败: ${e instanceof Error ? e.message : String(e)}`);
    }
    return;
  }

  if (action === "merge-right") {
    setLog(`查找 R${rid} 右邻…`);
    try {
      const found = await api<{ ok?: boolean; right_id?: number; raw?: string }>(
        `/api/find-merge-right?left_id=${rid}`,
      );
      const right_id = found.right_id;
      if (right_id == null) {
        setLog(found.raw || "无右邻可合并");
        return;
      }
      const right = findRegion(right_id);
      const ok = await openConfirm({
        title: `合并 Region`,
        body: `将 R${rid} ${rangeLabel(r)} 与 R${right_id} ${right ? rangeLabel(right) : ""} 合并为左 Region。`,
        confirmLabel: "执行合并",
        danger: true,
      });
      if (!ok) return;
      setLog(`合并 R${rid}+R${right_id}…`);
      const resp = await postJSON<OpResult>("/api/merge", { left_id: rid, right_id });
      setLog((resp.logs || [JSON.stringify(resp)]).join("\n"));
      await onAfterOp?.();
    } catch (e) {
      setLog(`失败: ${e instanceof Error ? e.message : String(e)}`);
    }
    return;
  }

  if (action === "add-replica") {
    const cur = (r.replicas || []).map((x) => x.node_id).join(", ") || "—";
    setLog(`查询可加副本的 Store（R${rid}）…`);
    let suggested = "";
    let candidates: Array<{ id: string; address?: string }> = [];
    try {
      const sug = await api<{
        suggested?: string;
        candidates?: Array<{ id: string; address?: string }>;
        have_peers?: string[];
      }>(`/api/suggest-replica?region_id=${rid}`);
      suggested = sug.suggested || "";
      candidates = sug.candidates || [];
      setLog(
        [
          `已有副本: ${(sug.have_peers || []).join(",") || "—"}`,
          candidates.length
            ? `候选: ${candidates.map((c) => c.id).join(", ")}`
            : "无可用 Active Store（可先加入待加入节点）",
        ].join("\n"),
      );
    } catch (e) {
      setLog(`建议失败: ${e instanceof Error ? e.message : String(e)}`);
    }
    const modal = await openModal({
      title: `增加副本 · R${rid}`,
      subtitle: candidates.length
        ? `当前副本：${cur} · 候选 ${candidates.map((c) => c.id).join(", ")}（会抬 RF）`
        : `当前副本：${cur} · 暂无空闲 Active Store，可先在下方「待加入」加入节点`,
      confirmLabel: "增加副本",
      fields: [
        {
          name: "node_id",
          label: "目标 Store ID（须 Active，且不在当前副本中）",
          value: suggested,
          placeholder: candidates[0]?.id || "storage-3",
          required: true,
        },
      ],
    });
    if (!modal.ok) return;
    const node_id = String(modal.values.node_id || "").trim();
    if (!node_id) return;
    setLog(`SCALE_OUT ${node_id} → R${rid}…`);
    try {
      const resp = await postJSON<OpResult>("/api/add-replica", { region_id: rid, node_id });
      if (!resp.ok) {
        setLog(resp.hint || resp.response || "加副本失败");
        alert(resp.hint || resp.response || "加副本失败");
        return;
      }
      const taskId = resp.task_id || "";
      if (!taskId) {
        setLog(JSON.stringify(resp, null, 2));
        await onAfterOp?.();
        return;
      }
      setLog(`已入队 ${taskId}（${resp.state_zh || "排队中"}）· 轮询追平进度…`);
      await refreshScaleTasks();
      await pollScaleTask(taskId, rid, node_id);
    } catch (e) {
      setLog(`失败: ${e instanceof Error ? e.message : String(e)}`);
    }
    return;
  }

  if (action === "suggest-place") {
    setLog(`建议放置 R${rid}…`);
    try {
      const resp = await api<OpResult & { avoid?: string[] }>(`/api/suggest-place?region_id=${rid}`);
      const peers = resp.peers?.join(",") || "";
      setLog(
        [resp.raw || "", resp.avoid?.length ? `avoid: ${resp.avoid.join(",")}` : "", peers ? `peers: ${peers}` : "无建议"]
          .filter(Boolean)
          .join("\n"),
      );
      if (!peers) return;
      const ok = await openConfirm({
        title: `建议放置 R${rid}`,
        body: `目标 peers：${peers}`,
        confirmLabel: "采用并 Place",
      });
      if (!ok) return;
      const placeResp = await postJSON<OpResult>("/api/place", {
        region_id: rid,
        peers: peers.split(","),
      });
      setLog(JSON.stringify(placeResp, null, 2));
      await onAfterOp?.();
    } catch (e) {
      setLog(`失败: ${e instanceof Error ? e.message : String(e)}`);
    }
    return;
  }

  if (action === "place") {
    const cur = (r.replicas || []).map((x) => x.node_id).join(",");
    const modal = await openModal({
      title: `放置 Region ${rid}`,
      subtitle: `当前副本：${cur || "—"}`,
      confirmLabel: "执行 Place",
      fields: [
        {
          name: "peers",
          label: "Peers（逗号分隔 Store ID）",
          value: cur,
          placeholder: "storage-0,storage-1,storage-2",
          required: true,
        },
      ],
    });
    if (!modal.ok) return;
    const peers = String(modal.values.peers || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (!peers.length) return;
    setLog(`Place R${rid}…`);
    try {
      const resp = await postJSON<OpResult>("/api/place", { region_id: rid, peers });
      setLog(JSON.stringify(resp, null, 2));
      await onAfterOp?.();
    } catch (e) {
      setLog(`失败: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
}
