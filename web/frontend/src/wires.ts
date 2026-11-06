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

import type { NodeLink } from "./types";

const STROKE: Record<string, string> = {
  gateway: "rgba(47, 214, 192, 0.85)",
  meta: "rgba(232, 176, 90, 0.9)",
  raft: "rgba(90, 160, 255, 0.85)",
  sync: "rgba(186, 120, 255, 0.85)",
};

function formatQps(n: number | undefined): string {
  if (n == null || !Number.isFinite(n) || n < 0) return "—";
  if (n < 10) return n.toFixed(1);
  if (n < 1000) return n.toFixed(0);
  return `${(n / 1000).toFixed(1)}k`;
}

function nodeCenter(board: HTMLElement, name: string): { x: number; y: number } | null {
  let el: HTMLElement | null = null;
  board.querySelectorAll<HTMLElement>(".topo-node[data-name]").forEach((n) => {
    if (!el && n.dataset.name === name) el = n;
  });
  if (!el) return null;
  const x = (parseFloat(el.style.left || "0") || 0) + el.offsetWidth / 2;
  const y = (parseFloat(el.style.top || "0") || 0) + el.offsetHeight / 2;
  return { x, y };
}

function ensureSvg(board: HTMLElement): SVGSVGElement {
  let svg = board.querySelector<SVGSVGElement>(".topo-wires");
  if (svg) return svg;
  svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.classList.add("topo-wires");
  svg.setAttribute("aria-hidden", "true");
  board.insertBefore(svg, board.firstChild);
  return svg;
}

export type WireFilter = "all" | "gateway" | "meta" | "raft" | "sync" | "store";

function matchFilter(l: NodeLink, filter: WireFilter): boolean {
  if (filter === "all") return true;
  if (filter === "store") {
    return l.from.startsWith("storage") && l.to.startsWith("storage");
  }
  return l.kind === filter;
}

/** 在画板上绘制节点间流量连线；拖拽时重复调用即可。 */
export function drawBoardWires(
  board: HTMLElement,
  links: NodeLink[],
  filter: WireFilter = "all",
): void {
  const svg = ensureSvg(board);
  const w = Math.max(board.scrollWidth, board.offsetWidth, 980);
  const h = Math.max(board.scrollHeight, board.offsetHeight, 520);
  svg.setAttribute("width", String(w));
  svg.setAttribute("height", String(h));
  svg.style.width = `${w}px`;
  svg.style.height = `${h}px`;

  const visible = links
    .filter((l) => (l.count || 0) > 0 || (l.qps || 0) > 0)
    .filter((l) => matchFilter(l, filter))
    .sort((a, b) => (b.qps || 0) - (a.qps || 0));

  // 同 pair 多条线错开
  const pairCount = new Map<string, number>();
  const parts: string[] = [];

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
    // 缩进端点，避免箭头扎进卡片中心
    const inset = 36;
    const ax = a.x + (dx / len) * inset;
    const ay = a.y + (dy / len) * inset;
    const bx = b.x - (dx / len) * inset;
    const by = b.y - (dy / len) * inset;

    const bend = (idx - (pairCount.get(pk)! - 1) / 2) * 22;
    const cx = (ax + bx) / 2 - (dy / len) * bend;
    const cy = (ay + by) / 2 + (dx / len) * bend;

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
      ${(["gateway", "meta", "raft", "sync"] as const)
        .map(
          (k) => `
        <marker id="wire-arrow-${k}" viewBox="0 0 10 10" refX="8" refY="5"
          markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0 1.5 L9 5 L0 8.5 Z" fill="${STROKE[k]}"/>
        </marker>`,
        )
        .join("")}
    </defs>
    ${parts.join("")}
  `;
}
