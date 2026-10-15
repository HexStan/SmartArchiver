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

## 常用命令

```bash
pip install -r requirements.txt && pip install pytest   # pytest 不在 requirements.txt，需单独装
python -m pytest                                        # 全部测试（pytest.ini 已设 pythonpath=. 和 testpaths=tests）
python -m pytest tests/test_greybox_rotate.py -v        # 单个测试文件
python main.py                                          # 客户端模式，读取 config/config.toml，缺失即退出
python main.py --server                                 # 服务器模式，读取 config/config.server.toml
python scripts/build_executable.py                      # 构建当前平台的可执行文件（PyInstaller onedir + 内置 rclone）
python scripts/smoke_test.py "dist/SmartArchiver-*.zip" # 对发行 zip 做冒烟检查（包结构 + 入口可运行）
```

- 运行前需 `cp config/config.example.toml config/config.toml`；`config.toml` 已 gitignore，只提交 `config/*.example.toml`。
- 仓库没有 lint / formatter / typecheck 配置，验证手段就是 pytest。

## 架构速览

- 入口 `main.py`：`run_client()`（无 `[schedule]` 时一次性运行，否则 cron/interval 常驻）与 `run_server()`（HTTP 远端 API）。
- 任务分派：`src/core/registry.py` 用 `@register_handler` 装饰器维护 mode → handler 映射，`process_task()` 负责创建后端、实例化并执行。新增模式在此注册。
- 处理器在 `src/core/handlers/`：`StandardHandler`（move/copy）、`RotateHandler`、`SyncHandler`，继承 `base.py` 的 `BaseTaskHandler`。
- 目标后端抽象 `src/core/backend.py`：`LocalDestBackend` / `RemoteDestBackend`（HTTP）/ `SshDestBackend`，依据 dest 字符串前缀 `{http:别名}?` 或 `{ssh:别名}?` 选择。
- 规则引擎 `src/core/filters.py` 的 `FileFilterPolicy`：顺序流水线 include → exclude → delete → TRANSFER，命中即停。
- `AppContext`（`src/app_context.py`）是全局单例，logger / history 均经 `AppContext.get()` 获取；测试 fixture 用完会重置单例。

## 测试约定（权威文档：tests/README.md）

详见 `tests/README.md`。

## 平台与版本约束

- Python 3.14+（依赖内置 `tomllib`）。CI 矩阵为 ubuntu + windows × Python 3.14–3.15。
- sync 模式调用外部进程：Linux 用 rsync，Windows 用 rclone（需在 PATH）。同步语义本身不测，测试只断言传给工具的命令行参数。

## 提交与发布（权威文档：RELEASE-GUIDE.md）

- 仅当维护者主动要求时执行对应流程，不得自行提交、推送或发布：
  - 要求提交变更 → 执行 RELEASE-GUIDE.md 的提交流程（前置：仓库存在未提交的变更）。
  - 要求发布版本 → 执行 RELEASE-GUIDE.md 的发布流程（前置：`VERSION` 文件存在未提交的变更）。
