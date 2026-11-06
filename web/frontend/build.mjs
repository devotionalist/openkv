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

import * as esbuild from "esbuild";
import { cpSync, mkdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const outDir = join(__dirname, "..", "static");
const watch = process.argv.includes("--watch");

mkdirSync(outDir, { recursive: true });

const ctx = await esbuild.context({
  entryPoints: [join(__dirname, "src", "main.ts")],
  bundle: true,
  outfile: join(outDir, "app.js"),
  format: "iife",
  platform: "browser",
  target: ["es2022"],
  sourcemap: true,
  minify: false,
  logLevel: "info",
});

function copyAssets() {
  cpSync(join(__dirname, "index.html"), join(outDir, "index.html"));
  cpSync(join(__dirname, "style.css"), join(outDir, "style.css"));
}

copyAssets();

if (watch) {
  await ctx.watch();
  console.log("watching frontend…");
} else {
  await ctx.rebuild();
  await ctx.dispose();
  console.log(`built → ${outDir}`);
}
