# AGENTS.md

SmartArchiver 是文件归档/清理工具：目录树 + 配置 → 新目录树。仓库文档、用户可见的日志与错误信息一律使用中文；提交信息用 Conventional Commits + 中文描述（如 `fix: 修复规则匹配对于目录的逻辑缺陷`）。

## 代码规范性要求

- 如果现有代码结构无法规范、优雅地实现维护者的需求：

  - 大胆增/删/改写模块，以保持低耦合、高内聚，避免出现补丁式代码。
  - 允许引入新库，不要为了“不增加依赖”而频繁自己造轮子。
  
- 如果：

  - 不确定某个功能应该使用何种方案来实现
  - 应该如何处理某种维护者未提及的情况
  - 维护者的需求描述或功能设计中有无法自洽的内容，或 Agent 认为有更好的方案
  - 有其他任何不确定的问题

  请向维护者询问。可在开发过程中多次询问，直至所有细节清晰。

## 禁令

- 除非被明确要求，否则禁止修改 `ROADMAP.md`。
- 除非进入发布流程或被明确要求，否则禁止修改 `CHANGELOG.md`。

## 常用命令

```bash
uv pip install --group dev                              # dev 组 = 运行依赖 + ruff / pyrefly / pytest / 类型存根
python -m pytest                                        # 全部测试（pyproject.toml 已设 pythonpath=. 和 testpaths=tests）
python -m pytest tests/test_greybox_rotate.py -v        # 单个测试文件
ruff check .                                            # Ruff 静态检查
ruff format .                                           # Ruff 格式化
pyrefly check                                           # Pyrefly 类型检查（strict 门禁，配置见 pyproject.toml 的 [tool.pyrefly]）
python main.py                                          # 客户端模式，读取 config/config.toml，缺失即退出
python main.py --server                                 # 服务器模式，读取 config/config.server.toml
python scripts/build_executable.py                      # 构建当前平台的可执行文件（PyInstaller onedir + 内置 rclone）
python scripts/smoke_test.py "dist/SmartArchiver-*.zip" # 对发行 zip 做冒烟检查（包结构 + 入口可运行）
```

- 运行前需 `cp config/config.example.toml config/config.toml`；`config.toml` 已 gitignore，只提交 `config/*.example.toml`。
- 验证手段 = pytest + ruff check + pyrefly check；formatter 用 Ruff Format（`pyproject.toml` 的 `[tool.ruff.format]`）。

## 类型检查（权威配置：pyproject.toml 的 [tool.pyrefly]）

- 详见 `pyproject.toml` 中 `[tool.pyrefly]` 的注释。

## 架构速览

- 入口 `main.py`：仅 CLI 解析与分发。运行路径在 `src/app/runtime.py`（`run_client()` 无 `[schedule]` 时一次性运行，否则 cron/interval 常驻；`run_server()` 为 HTTP 远端 API），控制子命令 start/log/stop 在 `src/app/control.py`。
- 配置加载与校验：`src/app/config.py` 的 `ConfigManager` 在加载期完成所有"仅凭配置文件即可判定"的合法性校验（Pydantic 模型）——全局段 `GlobalConfig`（max_retries、`[schedule]`、服务器数值段，致命错误为 `ConfigError`：启动期干净退出、热重载回退备份）、bootstrap 段 `BootstrapConfig`（log_dir / max_log_files / log_level，日志系统就位前消费，print + exit 通道）、远端条目（`remote.factory` / `ssh.remotes`）与任务条目；单任务/单远端条目错误记日志跳过该条目，不致命。
- 任务分派：`src/core/registry.py` 用 `@register_handler` 装饰器维护 mode → handler 映射，`process_task()` 接收 `TaskSpec` 实例化并执行，返回 `TaskOutcome`（COMPLETED / FAILED / SKIPPED）。新增模式在此注册（同时要在 `task_config.TASK_MODELS` 声明对应配置模型）。
- 任务配置模型与加载期解析：`src/core/task_config.py` 按 mode 判别的任务模型（`MoveCopyTaskConfig` / `RotateTaskConfig` / `SyncTaskConfig`，`extra="forbid"` + `frozen`）与共享类型（`SizeValue` / `RuleSetConfig` / `RotateRulesConfig`）；`src/core/task_plan.py` 的 `resolve_tasks()` 在加载期完成逐任务模型校验、dest 装配校验（`dest.validate_dest`）与 id/depends_on 静态校验（错误日志 + `TaskSpec.config=None` 跳过；id 注册只看字符集与唯一性，与任务自身合法性解耦）；"前置任务是否完成"的运行期判定在 `app/runtime.py` 的任务循环内（finished 集合每轮重置）。
- 处理器在 `src/core/handlers/`：`StandardHandler`（move/copy）、`RotateHandler`、`SyncHandler`，继承 `base.py` 的 `BaseTaskHandler`（构造签名收 `TaskSpec`，只做外部条件校验——目录存在性、远端可达、工具可用、静默期）。
- 目标侧 I/O 转发层 `src/core/dest.py`：dest 串协议（`{http:别名}?` / `{ssh:别名}?` / 本地路径）的唯一解析处与目标读写的唯一入口，按前缀分派至 `fs` 或 `RemoteClient`；加载期 `validate_dest` 做模式兼容性与别名存在性校验（纯函数，由 task_plan 调用）。约定：目标目录的读写必须经 `core.dest`，`fs` 仅用于本地路径。SSH 远端目录操作与 rsync 连接参数在 `src/ssh/ops.py`。
- 规则引擎 `src/core/filters.py` 的 `FileFilterPolicy`：顺序流水线 include → exclude → delete → TRANSFER，命中即停；消费加载期解析完成的 `RuleSetConfig`（容量解析与目录/文件拆分已在加载期完成），运行期只做匹配。模式匹配与容量解析原语在 `src/core/patterns.py`。
- 本地文件 I/O 原语在 `src/fs/`（errors / abort / scan / atomic / transfer / cleanup），不依赖任何上层包；`fs.abort` 的中止检查由 `app/runtime.py` 组装时注入。
- 共享校验基建 `src/validation.py`：远端别名类型（`RemoteAlias`）、别名/任务 id 共用正则（`ALIAS_PATTERN`）与校验错误中文格式化（`format_validation_error`）；与 `fs/` 平级的叶子模块，仅依赖 pydantic，所有包可导入。
- `AppContext`（`src/app/context.py`）是全局单例，logger / history 均经 `AppContext.get()` 获取；测试 fixture 用完会重置单例。
- 包分层依赖方向：`app/` 装配层可依赖全部；`core/` → `fs/`、`output/`、`remote/`、`ssh/`；`remote/`、`ssh/` → `fs/`；`output/` → `fs/`、`core/patterns`；`fs/` 仅标准库；`src/validation.py` 为仅依赖 pydantic 的共享叶子模块（所有包可导入）。领域包不得依赖 `app/` 的装配模块（runtime/control），需要 app 层能力时由组合根注入参数，依赖必须无环；唯一例外是文档化的 `AppContext` 服务定位与 `app.paths` 等叶子助手。`TYPE_CHECKING` 下的类型级 import（注解专用，运行时零导入）不构成违规，如 `remote/`、`app/context.py` 引用 `output.logger.Logger` 协议。

## 测试约定（权威文档：tests/README.md）

详见 `tests/README.md`。

## 平台与版本约束

- Python 3.14+。CI 矩阵为 ubuntu + windows × Python 3.14–3.15。
- sync 模式调用外部进程：Linux 用 rsync，Windows 用 rclone（需在 PATH）。同步语义本身不测，测试只断言传给工具的命令行参数。

## 提交与发布（权威文档：RELEASE-GUIDE.md）

- 仅当维护者主动要求时执行对应流程，不得自行提交、推送或发布：
  - 要求提交变更 → 执行 RELEASE-GUIDE.md 的提交流程（前置：仓库存在未提交的变更）。
  - 要求发布版本 → 执行 RELEASE-GUIDE.md 的发布流程（前置：`pyproject.toml` 的 version 字段存在未提交的变更）。
