> Language: [English](./contributing.md) · [中文](../zh/contributing.md)

# Contribution & commit rules

This repository is public. External contributors submit changes **only via pull requests**. Direct pushes to `master`, and creating tags / releases, are reserved for the repository owner (`@devotionalist`).

## Who can do what

| Action | Contributors | Owner (`@devotionalist`) |
|--------|--------------|--------------------------|
| Push directly to `master` | No | Yes (admin bypass) |
| Open a pull request | Yes (from a fork) | Yes |
| Approve / merge a PR into `master` | No (must wait for owner) | Yes |
| Create / update / delete tags | No | Yes |
| Publish GitHub Releases | No | Yes |

Enforced by GitHub [rulesets](https://github.com/devotionalist/openkv/rules) plus [`.github/CODEOWNERS`](../../.github/CODEOWNERS).

## Rules on `master`

1. **Pull request required** — no direct commits to `master` for non-admins.
2. **At least one approving review** — must include a **CODEOWNERS** review (`@devotionalist`).
3. **Stale reviews dismissed** — new pushes after approval clear previous approvals.
4. **Last push must be approved** — the person who pushes last still needs a fresh approval before merge.
5. **No force-push / no deleting `master`**.

## Tags and releases

- All tags (`refs/tags/*`) may be created, moved, or deleted **only by repository admins**.
- GitHub Releases should be cut by the owner only (typically from a tag they created).
- Do not ask collaborators for write access just to ship a release; open a PR and let the owner tag/release.

## Contributor workflow

```bash
# 1. Fork on GitHub, then clone your fork
git clone https://github.com/<you>/openkv.git
cd openkv
git remote add upstream https://github.com/devotionalist/openkv.git

# 2. Branch from up-to-date master
git fetch upstream
git checkout -b my-fix upstream/master

# 3. Develop, test, commit
make
# optional: make e2e
# Feature DoD: design docs under docs/ + tests + acceptance (see .cursor/rules)

# 4. Push to your fork and open a PR against devotionalist/openkv:master
git push -u origin my-fix
gh pr create --repo devotionalist/openkv --base master
```

Then wait for **owner approval** and merge. Do not expect self-merge rights.

### PR checklist

- [ ] One logical change per PR when practical
- [ ] `make` (build + unit tests) green locally; E2E when touching cluster paths
- [ ] Docs updated under `docs/en/` (and Chinese deep docs / `docs/zh/` when applicable)
- [ ] No secrets, credentials, or large binaries
- [ ] Commit messages describe **why**, not only what

## Owner notes

- Admin bypass on the branch ruleset allows the owner to push/hotfix `master` when needed.
- Prefer still using PRs for reviewable history, even for owner changes.
- Cut releases only after `master` is in the intended state; create the tag yourself, then publish the Release.

## Related docs

- [Quick start](quickstart.md)
- [Architecture](architecture.md)
- [Chinese documentation index](../zh/README.md)
