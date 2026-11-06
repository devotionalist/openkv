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

import { escapeHtml } from "./dom";

export type ModalField =
  | { name: string; label: string; type?: "text" | "number"; value?: string; placeholder?: string; required?: boolean }
  | { name: string; label: string; type: "checkbox"; checked?: boolean; hint?: string };

export interface ModalResult {
  ok: boolean;
  values: Record<string, string | boolean>;
}

function ensureModal(): { backdrop: HTMLElement; box: HTMLElement } {
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

/** 自定义表单弹层，替代 prompt/confirm */
export function openModal(opts: {
  title: string;
  subtitle?: string;
  fields?: ModalField[];
  confirmLabel?: string;
  cancelLabel?: string;
  danger?: boolean;
}): Promise<ModalResult> {
  const { backdrop, box } = ensureModal();
  const fields = opts.fields || [];

  const fieldsHtml = fields
    .map((f) => {
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
    })
    .join("");

  box.innerHTML = `
    <div class="skv-modal-head">
      <div>
        <h3 class="skv-modal-title">${escapeHtml(opts.title)}</h3>
        ${opts.subtitle ? `<p class="skv-modal-sub">${escapeHtml(opts.subtitle)}</p>` : ""}
      </div>
      <button type="button" class="skv-modal-x" data-modal="cancel" aria-label="关闭">×</button>
    </div>
    <form class="skv-modal-body" id="skv-modal-form">
      ${fieldsHtml}
      <div class="skv-modal-actions">
        <button type="button" class="ghost" data-modal="cancel">${escapeHtml(opts.cancelLabel || "取消")}</button>
        <button type="submit" class="${opts.danger ? "danger" : "primary"}">${escapeHtml(opts.confirmLabel || "确定")}</button>
      </div>
    </form>
  `;

  backdrop.hidden = false;
  box.hidden = false;
  requestAnimationFrame(() => {
    backdrop.classList.add("show");
    box.classList.add("show");
  });

  const firstInput = box.querySelector<HTMLInputElement>("input:not([type=checkbox])");
  firstInput?.focus();
  firstInput?.select();

  return new Promise((resolve) => {
    const finish = (ok: boolean, values: Record<string, string | boolean> = {}) => {
      backdrop.classList.remove("show");
      box.classList.remove("show");
      backdrop.hidden = true;
      box.hidden = true;
      resolve({ ok, values });
    };

    const onKey = (e: KeyboardEvent) => {
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

    box.querySelectorAll<HTMLElement>("[data-modal=cancel]").forEach((el) => {
      el.addEventListener("click", () => {
        cleanup();
        finish(false);
      });
    });

    const form = box.querySelector<HTMLFormElement>("#skv-modal-form");
    form?.addEventListener("submit", (e) => {
      e.preventDefault();
      const values: Record<string, string | boolean> = {};
      for (const f of fields) {
        const input = form.querySelector<HTMLInputElement>(`[name="${f.name}"]`);
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

function inputShake(el: Element | null): void {
  if (!el) return;
  el.classList.add("shake");
  (el as HTMLInputElement).focus();
  setTimeout(() => el.classList.remove("shake"), 400);
}

export function openConfirm(opts: {
  title: string;
  body: string;
  confirmLabel?: string;
  danger?: boolean;
}): Promise<boolean> {
  return openModal({
    title: opts.title,
    subtitle: opts.body,
    confirmLabel: opts.confirmLabel || "确定",
    danger: opts.danger,
  }).then((r) => r.ok);
}
