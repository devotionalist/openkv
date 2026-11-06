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

/** 拓扑节点拖拽：绝对定位 + localStorage 持久化。 */

export type Pos = { x: number; y: number };
export type PosMap = Record<string, Pos>;

const POS_KEY = "skv-topo-pos-v1";
const DRAG_THRESHOLD = 5;

export function loadPositions(): PosMap {
  try {
    const raw = localStorage.getItem(POS_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as PosMap;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

export function savePositions(map: PosMap): void {
  try {
    localStorage.setItem(POS_KEY, JSON.stringify(map));
  } catch {
    /* ignore quota */
  }
}

export function clearPositions(): void {
  try {
    localStorage.removeItem(POS_KEY);
  } catch {
    /* ignore */
  }
}

export function defaultPosition(name: string, role: string, indexInRole: number): Pos {
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

export function placeNode(el: HTMLElement, pos: Pos): void {
  el.style.left = `${Math.max(0, pos.x)}px`;
  el.style.top = `${Math.max(0, pos.y)}px`;
}

export function expandBoard(board: HTMLElement): void {
  let maxX = 900;
  let maxY = 480;
  board.querySelectorAll<HTMLElement>(".topo-node.draggable").forEach((el) => {
    const x = parseFloat(el.style.left || "0") || 0;
    const y = parseFloat(el.style.top || "0") || 0;
    maxX = Math.max(maxX, x + el.offsetWidth + 40);
    maxY = Math.max(maxY, y + el.offsetHeight + 40);
  });
  board.style.minWidth = `${maxX}px`;
  board.style.minHeight = `${maxY}px`;
}

/**
 * 启用拖拽。moved=false 时调用 onTap（点击打开菜单）；拖动中/结束调用 onMove 重绘连线。
 */
export function enableNodeDrag(
  board: HTMLElement,
  onTap: (el: HTMLElement) => void,
  onMove?: () => void,
): void {
  let posMap = loadPositions();

  board.querySelectorAll<HTMLElement>(".topo-node.draggable").forEach((el) => {
    const name = el.dataset.name;
    if (!name || name.startsWith("_empty")) return;

    let dragging = false;
    let moved = false;
    let startX = 0;
    let startY = 0;
    let origX = 0;
    let origY = 0;
    let pointerId = -1;

    const onPointerDown = (e: PointerEvent) => {
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

    const onPointerMove = (e: PointerEvent) => {
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

    const onPointerUp = (e: PointerEvent) => {
      if (!dragging || e.pointerId !== pointerId) return;
      dragging = false;
      el.classList.remove("dragging");
      el.style.zIndex = "";
      try {
        el.releasePointerCapture(pointerId);
      } catch {
        /* already released */
      }
      if (moved) {
        posMap = loadPositions();
        posMap[name] = {
          x: parseFloat(el.style.left || "0") || 0,
          y: parseFloat(el.style.top || "0") || 0,
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
