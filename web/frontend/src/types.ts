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

export interface Overview {
  meta_up: number;
  meta_total: number;
  storage_up: number;
  storage_total: number;
  gateway_up: boolean;
  gateway: string;
  route_version: number;
  region_count: number;
  gc_safe_ts?: number;
  next_ts?: number;
  min_start_hint?: number;
  who_leader?: string;
  fetched_at?: string;
  route_error?: string;
  safe_error?: string;
  topology?: Topology;
}

export interface DiskInfo {
  path: string;
  total_bytes: number;
  used_bytes: number;
  free_bytes: number;
  data_bytes: number;
  ok: boolean;
}

export interface TrafficRates {
  gw?: number;
  store?: number;
  raft?: number;
  sync?: number;
  admin?: number;
  meta_out?: number;
  to_store?: number;
  to_meta?: number;
}

export interface TopoNode {
  name: string;
  role: string;
  addr: string;
  up: boolean;
  alive: boolean;
  port: boolean;
  pid: number;
  is_leader: boolean;
  /** Meta 注册态：Joining / Active / Draining / Offline / Tombstone */
  cluster_state?: string;
  /** 中文短标签：待加入 / 在用 / … */
  state_label?: string;
  disk?: DiskInfo;
  qps?: number;
  client_qps?: number;
  req_total?: number;
  req_client?: number;
  rates?: TrafficRates;
}

export interface TopoGroup {
  role: string;
  label: string;
  nodes: TopoNode[];
}

export interface FlowEdge {
  id: string;
  from: string;
  to: string;
  kind: "gateway" | "meta" | "raft" | "sync" | string;
  label: string;
  qps: number;
  count?: number;
}

export interface NodeLink {
  from: string;
  to: string;
  kind: "gateway" | "meta" | "raft" | "sync" | string;
  label: string;
  qps: number;
  count?: number;
}

export interface Topology {
  controllable: boolean;
  groups?: TopoGroup[];
  /** @deprecated 使用 groups；保留兼容 */
  layers?: TopoGroup[];
  flows?: FlowEdge[];
  edges?: FlowEdge[];
  /** 节点对级流量边 */
  links?: NodeLink[];
}

export interface Replica {
  node_id: string;
  address: string;
  is_leader: boolean;
}

export interface Region {
  id: number;
  start_key: string;
  end_key: string;
  epoch: number;
  replicas: Replica[];
}

export interface RouteTable {
  version: number;
  regions: Region[];
}

export interface NodeInfo {
  id: string;
  role: string;
  address: string;
  state: string;
  state_code?: number;
  up: boolean;
  regions?: number;
  leaders?: number;
  /** 本节点承载的 Region ID 列表 */
  region_ids?: number[];
  /** 本节点任 Leader 的 Region ID */
  leader_region_ids?: number[];
}

export type StoreAction = "online" | "offline" | "kick";

export interface NodesResponse {
  nodes: NodeInfo[];
  raw?: string;
}

export interface PingItem {
  role: string;
  address: string;
  ok: boolean;
  detail: string;
}

export interface PingResponse {
  items: PingItem[];
}

export interface ProcessInfo {
  name: string;
  pid: string;
  alive: boolean;
}

export interface ProcessesResponse {
  available: boolean;
  run_dir?: string;
  processes: ProcessInfo[];
}

export interface OpResult {
  ok?: boolean;
  logs?: string[];
  response?: string;
  detail?: string;
  peers?: string[];
  raw?: string;
  error?: string;
  action?: string;
  hint?: string;
  node_id?: string;
  state?: string | number;
  state_code?: number;
  state_zh?: string;
  task_id?: string;
  regions?: number;
  leaders?: number;
  status?: TopoNode;
  topology?: Topology;
}

/** Meta SCALE_OUT / GET_SCALE 任务视图 */
export interface ScaleTaskView {
  task_id?: string;
  node_id?: string;
  state?: number;
  state_name?: string;
  state_zh?: string;
  progress?: number;
  progress_pct?: number;
  catchup_pct?: number;
  match?: number;
  commit?: number;
  caught?: boolean;
  shards?: number[];
  done?: boolean;
  failed?: boolean;
  catching_up?: boolean;
  active?: boolean;
  error?: string;
  raw?: string;
  ok?: boolean;
}
