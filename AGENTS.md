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

- 运行入口：`main.py` 仅 CLI 解析与分发；运行路径在 `src/app/runtime.py`，控制子命令 start/log/stop 在 `src/app/control.py`。
- 加载期：`src/app/config.py` 的 `ConfigManager`（Pydantic）完成所有仅凭配置即可判定的校验，单任务/单远端条目错误记日志跳过、不致命；`src/core/task_config.py` 定义按 mode 判别的任务配置模型，`src/core/task_plan.py` 的 `resolve_tasks()` 完成逐任务校验与 id/depends_on 静态校验。
- 运行期：`src/core/registry.py` 维护 mode → handler 映射（新增模式在此注册，并在 `task_config.TASK_MODELS` 声明模型）；处理器在 `src/core/handlers/`，基类 `BaseTaskHandler` 只做外部条件校验；"前置任务是否完成"由 `app/runtime.py` 的任务循环判定。
- 目标 I/O：dest 串解析与目标读写的唯一入口是 `src/core/dest.py`；约定：目标目录读写必须经 `core.dest`，`fs` 仅用于本地路径；SSH 远端操作在 `src/ssh/ops.py`。
- 规则引擎：`src/core/filters.py` 顺序流水线 include → exclude → delete → TRANSFER，命中即停，运行期只做匹配；原语在 `src/core/patterns.py`。
- 底层：本地文件 I/O 原语在 `src/fs/`（仅标准库）；共享校验基建 `src/validation.py`（仅 pydantic，所有包可导入）。
- `AppContext`（`src/app/context.py`）是全局单例，logger / history 经 `AppContext.get()` 获取；测试 fixture 用完会重置单例。
- 依赖方向（必须无环）：`app/` 可依赖全部；`core/` → `fs/`、`output/`、`remote/`、`ssh/`；`remote/`、`ssh/` → `fs/`；`output/` → `fs/`、`core/patterns`。领域包不得依赖 `app/` 的装配模块（runtime/control），需要 app 层能力时由组合根注入；唯一例外是文档化的 `AppContext` 与 `app.paths` 等叶子助手；`TYPE_CHECKING` 下的类型级 import 不构成违规。

## 测试约定（权威文档：tests/README.md）

详见 `tests/README.md`。

## 平台与版本约束

- Python 3.14+。CI 测试矩阵为 ubuntu + macos + windows × Python 3.14–3.15。
- sync 模式调用外部进程：Linux 用 rsync，Windows 用 rclone（需在 PATH）。同步语义本身不测，测试只断言传给工具的命令行参数。

## 提交与发布（权威文档：RELEASE-GUIDE.md）

- 仅当维护者主动要求时执行对应流程，不得自行提交、推送或发布：
  - 要求提交变更 → 执行 RELEASE-GUIDE.md 的提交流程（前置：仓库存在未提交的变更）。
  - 要求发布版本 → 执行 RELEASE-GUIDE.md 的发布流程（前置：`pyproject.toml` 的 version 字段存在未提交的变更）。
