# Copyright 2026 devotionalist
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# =============================================================================
# OpsKV Makefile — 常用开发入口
# -----------------------------------------------------------------------------
# 默认使用系统 clang++/ar，避免 Homebrew binutils 的 ar 与 Apple ld 不兼容。
# 变量可覆盖，例如：
#   make BUILD_DIR=build-asan memcheck
# =============================================================================

BUILD_DIR ?= build

# RocksDB 由 CMake FetchContent 固定版本随工程编译，无需系统预装
# gRPC 强制（系统包优先，否则 FetchContent）；勿再传 -DOPSKV_USE_GRPC=OFF
CMAKE_FLAGS ?= -DOPSKV_USE_SEASTAR=OFF \
	-DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_AR=/usr/bin/ar -DCMAKE_RANLIB=/usr/bin/ranlib

.PHONY: all configure build test gtest run-meta run-storage run-compute proto go-mod clean \
	cluster-up cluster-down cluster-test e2e memcheck asan fault-drill integ-workload release-gate \
	admin admin-run admin-frontend \
	it-web it-region it-txn it-disk-fault it-partition integ-extra fault-drill-extra

# 默认：编译 + 跑全部单元测试
all: build test

# 仅配置（生成 build/ 与 compile_commands.json）
configure:
	cmake -B $(BUILD_DIR) $(CMAKE_FLAGS)

# 增量编译
build: configure
	cmake --build $(BUILD_DIR) -j

# 通过 ctest 跑 gtest_discover_tests 注册的全部 TEST()
test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

# 直接跑 gtest 二进制（可用 --gtest_filter=HashTest.* 过滤）
gtest: build
	./$(BUILD_DIR)/tests/opskv_unit_tests --gtest_color=yes

# 内存泄漏 / 非法访问 / UB 检查（独立 build-asan，不污染普通构建）
memcheck asan:
	bash scripts/memcheck.sh

# ---- 单进程调试启动 --------------------------------------------------------
run-meta: build
	./$(BUILD_DIR)/meta/meta_server conf/bootstrap.conf

run-storage: build
	./$(BUILD_DIR)/storage/storage_node conf/storage.conf

run-compute: build
	./$(BUILD_DIR)/compute/compute_node conf/compute.conf

# ---- 本地集群（daemon 保活）与客户端 --------------------------------------
cluster-up:
	bash scripts/start-cluster.sh

cluster-down:
	bash scripts/stop-cluster.sh

cluster-test:
	bash scripts/test-client.sh

# 同进程树内：启集群 → 测客户端 → 自动清理
e2e:
	bash scripts/run-e2e.sh

# 故障演练：杀节点 / 恢复 / 滚动重启；全程后台持续 Put/Get
fault-drill:
	bash scripts/fault-drill.sh

# 集成测试：后台多用户读写 + e2e/txn/watch + 杀 1 Store
integ-workload:
	bash scripts/integ-with-workload.sh

# 发版门禁：单元 + e2e + it-web/region/txn + fault-drill（含 F2）
release-gate:
	bash scripts/release-gate.sh

# Web / Region scatter / Txn 硬门禁（集群须已 up）
it-web:
	bash scripts/it-web.sh
it-region:
	bash scripts/it-region.sh
it-txn:
	bash scripts/it-txn.sh

# 周跑：磁盘只读 / 分区近似（破坏性更强，不默认进 release-gate）
it-disk-fault:
	bash scripts/it-disk-fault.sh
it-partition:
	bash scripts/it-partition.sh
integ-extra: it-disk-fault it-partition
fault-drill-extra: fault-drill integ-extra

# 独立 Web 管控模块（HTTP :19100）；前端 TypeScript → static/
admin-frontend:
	cd web/frontend && npm install --no-fund --no-audit && npm run build

admin: admin-frontend
	mkdir -p .run
	cd web && go build -o ../.run/skv-web .
	@echo "run: make admin-run   →  http://127.0.0.1:19100"

admin-run: admin
	SKV_RUN="$(CURDIR)/.run" SKV_ROOT="$(CURDIR)" ./.run/skv-web \
		-listen 0.0.0.0:19100 \
		-metas 127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
		-storages 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 \
		-gateway 127.0.0.1:19000 \
		-run-dir "$(CURDIR)/.run" \
		-root "$(CURDIR)"

# 重新生成 Go protobuf 消息代码
proto:
	bash scripts/gen_proto.sh

go-mod:
	cd client/go && go mod tidy

# 清理构建产物与运行时目录
clean:
	rm -rf $(BUILD_DIR) data .run
