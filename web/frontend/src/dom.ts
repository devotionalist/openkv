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

export function $(sel: string): HTMLElement {
  const el = document.querySelector(sel);
  if (!el) throw new Error(`missing element: ${sel}`);
  return el as HTMLElement;
}

export function $optional(sel: string): HTMLElement | null {
  return document.querySelector(sel);
}

export function escapeHtml(s: unknown): string {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function setTab(name: string): void {
  document.querySelectorAll<HTMLButtonElement>(".tabs button").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === name);
  });
  document.querySelectorAll<HTMLElement>(".panel").forEach((p) => {
    p.classList.toggle("active", p.id === `panel-${name}`);
  });
}

export function bindForm(
  id: string,
  handler: (fd: FormData, form: HTMLFormElement) => Promise<void>,
): void {
  $(id).addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = e.target as HTMLFormElement;
    const fd = new FormData(form);
    const sub = (e as SubmitEvent).submitter as HTMLButtonElement | null;
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
