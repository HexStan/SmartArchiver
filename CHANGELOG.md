# 更新日志

---

## 未发布

### 破坏性变更

- 合并 `move` 和 `whitelist_move` 为 `move` 模式，合并 `copy` 和 `whitelist_copy` 为 `copy` 模式。`whitelist_move`/`whitelist_copy` 不再支持，工作方式现统一为原 whitelist 系列的行为。
- 配置项 `keep_rules` 重命名为 `exclude_rules`，`whitelist_rules` 重命名为 `include_rules`。
- 删去 `preferred_rule` 配置项。规则引擎现采用顺序流水线：`include_rules` → `exclude_rules` → `delete_rules` → TRANSFER，命中即停止，不再有并行检查和冲突消解。
- `include_rules` 为选填：未配置时所有文件均视为"已纳入"（等价于旧 `move`/`copy` 的默认行为）；配置后仅匹配的文件被处理（等价于旧 whitelist 模式）。

### 修复

- `keep_rules` 和 `delete_rules` 在 rotate 模式下对目录不生效的问题，以及同源的逻辑缺陷。

### 变更

- `conflict_policy` 配置项现为选填，默认值为 `"skip"`（跳过已存在的目标文件）。
- 非 `sync` 模式下允许省略 `dest`（目标目录），未配置时文件传输操作自动跳过。
- `sync` 模式现强制要求 `dest` 字段必填。

---

## [0.24.2] - 2026-06-28

### 修复

- 日志文件名前缀丢失的问题。

---

## [0.24.0] - 2026-06-26

### 新增

- 支持配置文件热重载：在定时（cron/interval）运行模式下，修改配置文件后无需重启服务即可自动生效。
- `log_dir` 配置项现为可选，未设置时默认使用 `./logs` 目录。

### 修复

- 修复部分任务模式未校验 `source` 字段的问题，避免无效任务静默跳过。

### 变更

- 删除空目录与清理旧备份的日志级别从 `debug` 提升至 `info`，使操作记录在默认日志级别下即可见。
- 日志中的 "CRUCIAL" 信息改为中文 "致命错误"，提升可读性。

---

## [0.23.0] - 2026-06-15

- 初次发布 Release

[0.24.2]: https://github.com/HexStan/SmartArchiver/releases/tag/v0.24.2
[0.24.0]: https://github.com/HexStan/SmartArchiver/releases/tag/v0.24.0
[0.23.0]: https://github.com/HexStan/SmartArchiver/releases/tag/v0.23.0
