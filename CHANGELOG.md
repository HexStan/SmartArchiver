# 更新日志

---

## 未发布

### 修复

- `keep_rules` 和 `delete_rules` 在 rotate 模式下对目录不生效的问题，以及同源的逻辑缺陷

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
