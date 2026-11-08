> 语言: [English](../en/contributing.md) · [中文](./contributing.md)

# 代码提交与协作规则

本仓库公开。外部贡献者**只能通过 Pull Request** 提交改动。直接推送到 `master`，以及创建 tag / 发布 Release，仅仓库所有者（`@devotionalist`）可以操作。

## 权限一览

| 操作 | 贡献者 | 所有者（`@devotionalist`） |
|------|--------|---------------------------|
| 直接 push 到 `master` | 否 | 是（admin bypass） |
| 发起 Pull Request | 是（从 fork） | 是 |
| 批准 / 合并 PR 到 `master` | 否（需所有者批准） | 是 |
| 创建 / 更新 / 删除 tag | 否 | 是 |
| 发布 GitHub Release | 否 | 是 |

由 GitHub [rulesets](https://github.com/devotionalist/openkv/rules) 与 [`.github/CODEOWNERS`](../../.github/CODEOWNERS) 强制执行。

## `master` 分支规则

1. **必须走 PR** — 非 admin 不能直接往 `master` 提交。
2. **至少 1 个批准** — 且必须包含 **CODEOWNERS** 审查（`@devotionalist`）。
3. **新推送会使旧批准失效** — 批准后若再 push，需重新审批。
4. **最后一次推送也要批准** — 合并前，最后推送者仍需获得新的批准。
5. **禁止 force-push / 禁止删除 `master`**。

## Tag 与 Release

- 所有 tag（`refs/tags/*`）的创建、移动、删除 **仅仓库 admin** 可做。
- GitHub Release 只能由所有者发布（通常基于其创建的 tag）。
- 不要为了发版去申请 write 权限；提 PR，由所有者打 tag / 发 Release。

## 贡献者流程

```bash
# 1. 在 GitHub fork，再克隆自己的 fork
git clone https://github.com/<you>/openkv.git
cd openkv
git remote add upstream https://github.com/devotionalist/openkv.git

# 2. 基于最新 master 开分支
git fetch upstream
git checkout -b my-fix upstream/master

# 3. 开发、测试、提交
make
# 可选: make e2e
# 功能 DoD：docs/ 设计文档 + 测试 + 验收（见 .cursor/rules）

# 4. 推到 fork，并向 devotionalist/openkv:master 开 PR
git push -u origin my-fix
gh pr create --repo devotionalist/openkv --base master
```

之后等待**所有者批准并合并**。贡献者没有自行合并权限。

### PR 检查清单

- [ ] 尽量一个 PR 只做一件事
- [ ] 本地 `make`（编译 + 单测）通过；涉及集群路径时跑 E2E
- [ ] 文档同步：`docs/en/`（需要时更新中文深度文档 / `docs/zh/`）
- [ ] 不提交密钥、凭证或超大二进制
- [ ] Commit message 讲清 **为什么**，而不只是改了什么

## 所有者说明

- 分支 ruleset 的 admin bypass 允许所有者在必要时直接 push / hotfix `master`。
- 即便是所有者改动，也建议尽量走 PR，便于留痕与审查。
- 发版前确认 `master` 已是目标状态；自行打 tag，再发布 Release。

## 相关文档

- [快速开始（英文）](../en/quickstart.md)
- [中文文档索引](README.md)
- [架构](../architecture/ARCHITECTURE.md)
