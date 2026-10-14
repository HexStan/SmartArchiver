# 测试指南

本目录的测试体系遵循一个明确的分层策略：**灰盒测试为主体框架，白盒单元测试补外部不可见的盲区**。

## 为什么是"灰盒为主、白盒补盲"

SmartArchiver 的本质是一个"目录树 + 配置 → 新目录树"的变换工具，它**会删除和搬移用户的文件**。因此：

1. **正确性有一个天然的外部判定标准**——最终哪些文件被移动、删除、保留在原地。断言真实文件系统的最终状态，比断言内部某个分支被走过信号强得多。
2. **大部分代码是 I/O 编排**（文件系统、subprocess、网络）。对它们做白盒测试意味着大量 mock，测的其实是 mock 本身；rsync/rclone 的同步语义、文件锁、路径分隔符差异都在白盒边界之外。
3. **少数行为从外部根本观察不到**（惰性求值、遍历顺序状态、多分组扣减），只能靠白盒单元测试兜住。

## 分层结构

| 层 | 文件 | 被测对象 | 驱动方式 |
|----|------|---------|---------|
| 灰盒·模式处理器 | `test_greybox_move_copy.py` | StandardHandler（move/copy） | `run_task()` 入口 + 临时目录树 |
| 灰盒·模式处理器 | `test_greybox_rotate.py` | RotateHandler | 同上 |
| 灰盒·模式处理器 | `test_greybox_sync.py` | SyncHandler | 入口驱动，进程边界打桩 |
| 灰盒·持久化 | `test_history_persistence.py` | HistoryManager | 真实文件读写 |
| 白盒·规则引擎 | `test_rule_engine.py` | FileFilterPolicy | 直接调用 `decide()` |
| 白盒·轮转分组 | `test_rotate_groups.py` | RotateGroupManager | 直接调用分组统计接口 |
| 白盒·纯函数 | `test_size_and_pattern.py` | parse_size_string / match_pattern | 直接调用 |
| 白盒·路径与分派 | `test_dest_backend.py` | DestBackend 路径拼接、create_dest_backend | 直接调用（get_unique_dest 用真实文件） |

## 编写原则

**原则一：灰盒优先——能从入口测的，不从内部测。**
新测试先问："能不能通过 `run_task()`（即 handler 的 `execute()` 入口）加一个临时目录树来驱动？"能，就写成灰盒测试：输入 = 目录树 + 任务配置，断言 = 输出目录树 + 日志。被测代码内部**不打桩**——Logger 用捕获型替身，HistoryManager 用真的，LocalDestBackend 用真的。任务的配置校验失败、源目录不存在等错误路径，也在这里以"错误日志 + 目录树无变化"的形式验证，不单独拆成校验函数的单测。

**原则二：白盒只补外部不可见的契约。**
只有当行为从 `execute()` 入口无法观察（或观察成本畸高）时，才写白盒单元测试。当前属于这一类的契约：
- **惰性求值**：`ge."模式" = -1` 命中时不计算大小——性能契约，只能靠传入计数 callable 验证（`test_rule_engine.py`）；
- **include 继承的遍历顺序**：`included_dirs` 在遍历中累积，目录必须先于子文件被评估（`test_rule_engine.py`）；
- **多分组扣减联动**：一个文件从所有关联分组中扣除统计值（`test_rotate_groups.py`）；
- **阈值边界**：lt 严格小于、ge 大于等于（两处都有，白盒侧钉死语义）。
不在此列的内部细节（比如某个私有方法的返回结构）不要测——那会把测试焊死在实现上。

**原则三：打桩只发生在外部边界。**
桩只能放在**进程或网络边界**（`subprocess.Popen`、`shutil.which`、SSH 远端探测），因为这些是"不在我们控制之内的外部世界"。业务函数之间不许互相打桩——那会掩盖真实的集成缺陷。`test_greybox_sync.py` 的 `stub_procs` fixture 是范式：拦截 `Popen` 记录构造出的命令行，断言我们传给外部工具的参数正确，而不假装自己测试了 rsync。

**原则四：确定性——禁止依赖真实时钟。**
- 所有 `now` 通过 `run_task(task, now=NOW)` 显式注入；
- 所有文件 mtime 在 `make_tree()` 里显式设定（`NOW`、`OLD`、`RECENT` 常量，或逐文件元组）；
- 不 sleep、不依赖"现在几点"、不依赖文件系统时间戳默认值。

**原则五：断言优先落在目录树快照上。**
`snapshot()` 返回 `{相对路径: 大小}`，是主断言对象；`handler.stats` 和捕获的日志只作补充信号（如 `conflict_skipped == 1`、"致命错误"日志）。文件内容需要区分时（冲突覆盖测试），单独读文件断言。

**原则六：一个测试只陈述一个行为。**
测试名就是行为描述（`test_exclude_shields_file_from_delete`），读测试名应当能看懂它守护的契约。组合行为的验证交给灰盒场景测试，白盒测试保持单点。

**原则七：真实组件优先于替身。**
优先级：真实组件 > 捕获型替身（记录调用但不改变行为）> 行为型 mock（伪造返回值）。Logger 用捕获型（不改行为只记录）；HistoryManager、文件系统、LocalDestBackend 全部用真的。每引入一个行为型 mock，都要能回答"我在替谁做决定，依据是什么"。

## 基础设施

**`conftest.py`**
- `app_context`：初始化全局 AppContext——捕获型 `CaptureLogger` + **真实** `HistoryManager`（指向 tmp 目录）+ 默认配置；测试结束清理单例。
- `run_task(task, now=None, remote_clients=None, ssh_remotes=None)`：灰盒测试统一入口。按任务的 `mode` 从注册表取处理器、创建目标后端、执行并返回 handler 实例（供补充断言）。

**`helpers.py`**
- `NOW / OLD / RECENT`：确定性时间常量（`OLD` 距 `NOW` 30 天，`RECENT` 距 60 秒）。
- `make_tree(root, files, default_mtime=OLD)`：按 `{相对路径: bytes}` 声明式建树；值也可以是 `(bytes, mtime)` 元组单独指定时间。
- `snapshot(root)`：`{相对路径: 大小}` 快照，目录树断言的基准。

## 运行

```bash
python -m pytest            # 全部测试
python -m pytest tests/test_greybox_move_copy.py -v
```

## 新增测试的决策路径

1. 行为能从 `execute()` 入口观察？→ 写进对应灰盒文件（`test_greybox_*.py`）。
2. 不能观察，但属于对外契约（性能、顺序、联动）？→ 写进对应白盒文件，并在模块 docstring 里注明"为什么这里必须白盒"。
3. 涉及外部进程/网络？→ 只在边界打桩，参考 `test_greybox_sync.py` 的 `stub_procs`。
4. 三条都不满足 → 先怀疑这个测试有没有必要存在。
