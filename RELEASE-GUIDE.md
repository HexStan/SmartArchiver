# 提交与发布指南

本指南供 Agent 在本仓库执行「提交变更」与「发布版本」。两个流程均**仅在维护者主动要求时执行**（触发条件见 AGENTS.md「提交与发布」一节），不要自行触发。

## 通用约定

- 提交信息遵循 Conventional Commits + 中文描述，可带 scope，如 `fix: 修复规则匹配对于目录的逻辑缺陷`、`feat(config): 将 conflict_policy 改为选填并默认 "skip"`。常用类型：`feat` / `fix` / `docs` / `refactor` / `test` / `chore` / `ci`；拿不准时参考 `git log` 近期风格。
- 版本号只写在根目录 `VERSION` 文件（单行）：`[x].[y].[z]` 为正式版，带 `-dev` 后缀为开发版。
- `gh` CLI 已认证（HexStan），创建 Release 与触发工作流均通过 `gh` 完成。
- 流程开始前先同步与验证：

  ```bash
  git pull --ff-only        # 分叉或失败时停止并报告维护者
  python -m pytest          # 涉及代码/测试的变更必须先通过
  ```

## 提交流程

前置条件：`git status` 存在未提交的变更；否则告知维护者无需提交，流程结束。

1. 用 `git status` 和 `git diff`（未跟踪文件直接阅读）审查全部待提交内容。
   - 疑似临时产物、密钥等不应入库的文件：不提交，向维护者报告。
2. 判断变更类型。**若存在多种不同类型的变更，按类型拆分为多次提交**（如功能代码与 README 更新拆为 `feat(...)` 与 `docs(...)` 两次）；同一类型内的关联变更合并为一次提交即可。
3. 逐次 `git add <文件>` + `git commit`，直至工作区干净。

本流程仅提交到本地，不推送；推送仅出现在发布流程中，或由维护者明确要求时执行。

> 作为发布流程第 1 步调用时，排除 `VERSION` 与 `CHANGELOG.md`——这两个文件固定由发布流程第 3 步合并提交。

## 发布流程

前置条件：`VERSION` 文件存在未提交的变更。若无，先与维护者确认目标版本号（例如将当前开发版号去掉 `-dev` 后缀），修改 `VERSION` 后继续（该修改即成为未提交变更）。

- 当前分支为 `dev` → 开发版路径：第 1–3、5 步。
- 当前分支为 `master` → 正式版路径：第 1–5 步。
- 其他分支：发布流程仅为 master/dev 定义，向维护者确认后再继续。

### 第 1 步：提交变更

执行[提交流程](#提交流程)，但排除 `VERSION` 与 `CHANGELOG.md`。

### 第 2 步：更新 CHANGELOG.md

只收录**用户可感知的变更**（影响行为、配置、日志输出、使用方式等）；纯重构、测试调整、CI 改动等不改变程序行为的变更不写入。分类子标题顺序固定：`### 破坏性变更` → `### 新增` → `### 修复` → `### 变更`，仅保留有内容的分类。

#### dev 分支（开发版）

1. 确认 `VERSION` 带 `-dev` 后缀；否则向维护者确认意图。
2. 界定增量范围：
   - 用 `git log -p -- VERSION` 找到当前**已提交**的 `-dev` 版本号（即上个 DEV 版；工作区未提交的新值不算），以及将其写入 `VERSION` 的提交（diff 中出现 `+<上个DEV版>` 的提交），记为分界提交。
   - 分析范围 = `分界提交^..HEAD`（含分界提交自身——本仓库版本号递增常与功能变更在同一提交）。
   - 不存在更早的 `-dev` 版本时，以上个正式版 tag 为分界（`git describe --abbrev=0 --match='v*'`），范围 = `<tag>..HEAD`。
3. 分析 `git log --oneline <范围>` 及相关提交内容，将结果**合并**进 `## 未发布` 段落：与现有条目去重；发现此前遗漏的用户可感知变更一并补充。最终以「`未发布` 段落完整覆盖自上个正式版以来的全部用户可感知变更、且无重复条目」为准。

#### master 分支（正式版）

1. 确认 `VERSION` 不带 `-dev` 后缀；否则向维护者确认意图。
2. 确定上个正式版 tag（`git describe --abbrev=0 --match='v*'`），分析 `git log --oneline <tag>..HEAD`，以 `## 未发布` 段落为底稿核对、补全。
3. 将发布内容写入新段落 `## [x.y.z] - YYYY-MM-DD`（日期为发布当日），置于 `## 未发布` 之后、上一个正式版段落之前；仅包含用户可感知的变更。
4. 清空 `## 未发布` 段落的条目（保留标题），供下个开发周期累积。
5. 在文件底部链接区最上方追加一行：`[x.y.z]: https://github.com/HexStan/SmartArchiver/releases/tag/vx.y.z`。

### 第 3 步：合并提交 VERSION 与 CHANGELOG.md

两个文件合并为**一次**提交，信息沿用既有风格：`docs: 提升版本号，更新日志`。

### 第 4 步（仅 master）：打 tag 并创建 Release

1. `git tag -a vX.Y.Z -m "Release vX.Y.Z"`（指向第 3 步的提交）。tag 已存在时停止并报告维护者。
2. `git push origin master vX.Y.Z`（分支与 tag 一起推送）。
3. 创建 Release：标题为 `vX.Y.Z`，注记为 CHANGELOG 对应小节去掉 `## [x.y.z] - 日期` 标题行后的正文（与已发布的 v0.24.0 / v0.24.2 格式一致）。

   ```bash
   gh release create vX.Y.Z --title "vX.Y.Z" --notes-file release_notes.md
   ```

   - 注记可先写入仓库根的临时文件 `release_notes.md`（勿提交，用后删除），提取命令见附录 B。
   - **必须先推送 tag 再创建 Release**：否则 `gh` 会在默认分支（dev）的最新提交上另建同名 tag。

dev 分支不打 tag、不创建 Release，直接进入第 5 步。

### 第 5 步：触发 Docker 构建（两个分支）

Docker 镜像的构建与推送由 CI（`.github/workflows/version-release.yml`）完成，Agent 只需保证提交已推送：

- dev：`git push`（master 已在第 4 步连同 tag 推送）。
- CI 触发条件：推送到 master/dev 且 `VERSION` 有变更。若本次推送未变更 `VERSION`（补发场景），手动触发：`gh workflow run version-release.yml --ref <分支名>`。
- 确认构建成功：`gh run list --workflow=version-release.yml --limit 1`（或 `gh run watch`）；失败时报告维护者。

构建以仓库根为 context（`docker build -f docker/app/Dockerfile .`），产物推送至 GHCR（镜像名 = 仓库名全小写）：

| VERSION | 镜像标签 |
| --- | --- |
| `x.y.z-dev` | `x.y.z-dev`、`dev` |
| `x.y.z`（正式版） | `x.y.z`、`latest` |

## 附录 A：CHANGELOG.md 格式

```markdown
# 更新日志

---

## 未发布

### 破坏性变更

- …

---

## [x.y.z] - YYYY-MM-DD

### 新增

- …

### 修复

- …

### 变更

- …

---

[x.y.z]: https://github.com/HexStan/SmartArchiver/releases/tag/vx.y.z
```

- 条目为一句话中文描述，面向用户；段落之间以 `---` 分隔。
- 正式版段落日期为发布当日；文件底部链接区新版本置于最上方。

## 附录 B：提取发布注记（可选）

```bash
VERSION=x.y.z
python - "$VERSION" <<'PY' > release_notes.md
import re, sys
version = sys.argv[1]
content = open('CHANGELOG.md', encoding='utf-8').read()
m = re.search(rf'## \[{re.escape(version)}\].*?(?=\n---|\n## \[|\n\[\d|\Z)', content, re.DOTALL)
notes = m.group(0).strip().split('\n', 1)[1] if m and '\n' in m.group(0) else ''
print(notes.strip())
PY
```

## 异常与中止

遇以下情况停止流程并向维护者报告，不要自行变通：

- `python -m pytest` 未通过；
- 待提交内容混有疑似不应入库的文件；
- 目标 tag 已存在；
- `VERSION` 与分支不匹配（dev 上无 `-dev` 后缀，或 master 上带 `-dev` 后缀）；
- 当前分支既非 master 也非 dev；
- `gh` 认证或网络异常导致 Release 创建失败（此时提交与 tag 已推送可保留，修复后仅重跑第 4 步第 3 点）。
