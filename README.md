# SmartArchiver

SmartArchiver 是一个灵活的文件归档与清理工具，帮助你按规则自动管理磁盘上的文件——将旧文件移动到归档目录、清理过期日志、按条件轮转文件、或保持两个目录的镜像同步。

📋 [TODO 看板](./ROADMAP.md)

---

## 适用场景

SmartArchiver 解决的核心问题是：**"磁盘上有一堆文件，我想根据名字、大小、时间等条件，把它们搬运、删除或同步到别处"**。它既可作为一次性脚本，也可作为常驻服务定时运行。以下是几个典型场景：

- **冷热数据分层**：将 SSD 上超过 N 天未修改的文件自动迁移到 HDD 大容量存储，同时保留最近文件在高速盘上。
- **日志归档与清理**：将 `logs/` 目录下超过一定天数的大日志文件移动到归档目录；或当日志总体积超过阈值时，从最旧的开始轮转删除。
- **备份目录瘦身**：在备份目录中保留最近 N 份全量备份或限制总大小，超出部分自动清除。
- **选择性同步**：将源目录镜像同步到目标，但排除临时文件、缓存目录等不需要同步的内容。
- **白名单模式**：仅处理符合特定命名规则的文件（如只搬运 `*.mp4`），其他一律不动。
- **定时自动运行**：通过 cron 表达式或固定间隔，让程序在后台持续执行上述任务。

---

## 安装与部署

### 本地运行

需要 Python 3.11+（内置 `tomllib`，无需额外安装 TOML 解析库）。

```bash
# 1. 克隆项目
git clone https://github.com/hexstan/smart-archiver.git
cd smart-archiver

# 2. 安装依赖
pip install -r requirements.txt

# 3. 创建并编辑配置文件
cp config/config.example.toml config/config.toml
# 按需编辑 config/config.toml，参考示例文件中的注释

# 4. 一次性运行
python main.py
```

如需使用同步（sync）模式：
- **Linux**：需安装 `rsync`（`apt install rsync` / `yum install rsync`）
- **Windows**：需安装 [`rclone`](https://rclone.org/downloads/) 并将其加入 PATH

### Docker 部署（推荐）

```bash
# 1. 创建并编辑配置文件（同上）
cp config/config.example.toml config/config.toml

# 2. 按需编辑 docker-compose.yml 中的目录挂载映射

# 3. 启动（会持续运行，容器退出后自动重启）
docker-compose up -d

# 查看日志
docker-compose logs -f
```

也可直接构建镜像：

```bash
docker build -t smart-archiver .
docker run -d \
  --name smart-archiver \
  --restart unless-stopped \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/logs:/app/logs \
  -v /your/source:/app/source \
  -v /your/dest:/app/dest \
  smart-archiver
```

Docker 镜像基于 `python:3.14-slim-trixie`，已内置 `rsync`，同步模式开箱即用。GitHub Actions 会自动构建 `linux/amd64` 和 `linux/arm64` 多架构镜像并推送到 GHCR。

---

## 项目模块结构

```
SmartArchiver/
├── main.py                         # 程序入口：一次性 / cron / interval 三种运行模式
├── config/
│   └── config.example.toml         # 带中文注释的示例配置文件
├── src/
│   ├── app_context.py              # 单例上下文，解耦 Logger、HistoryManager、Config 的传递
│   ├── logger.py                   # 日志系统：自定义级别、双格式输出、按天滚动
│   ├── history.py                  # 失败历史管理：记录失败次数，超阈值自动跳过
│   ├── presentation.py             # 表现层：统一格式化调用，任务输出头/统计信息
│   ├── utils.py                    # 工具函数：文件锁、路径匹配、大小解析、空目录清理等
│   └── core/
│       ├── __init__.py             # 包入口，触发 handler 注册，导出 process_task
│       ├── types.py                # FileAction 枚举 + MoverStats 统计类
│       ├── filters.py              # 规则引擎：keep/delete/whitelist 规则的解析与决策
│       ├── actions.py              # 动作函数：任务校验、文件删除、文件传输（含冲突策略）
│       ├── registry.py             # 处理器注册中心：装饰器注册 + process_task 调度
│       └── handlers/
│           ├── base.py             # 抽象基类 + FileChecker + ActionExecutor
│           ├── standard.py         # StandardHandler：move / copy / whitelist_move / whitelist_copy
│           ├── rotate.py           # RotateHandler：轮转模式 + RotateGroupManager
│           └── sync.py             # SyncHandler：同步模式（rsync / rclone）
├── Dockerfile                      # Docker 镜像定义
├── docker-compose.yml              # Docker Compose 编排
└── requirements.txt                # Python 依赖
```

---

## 各模式工作原理

SmartArchiver 支持 6 种任务模式，对应 3 个处理器。

### 1. 标准模式（move / copy / whitelist_move / whitelist_copy）

**处理器**：[`StandardHandler`](src/core/handlers/standard.py)

**工作流程**：

1. 使用 `os.walk` 自顶向下遍历源目录。
2. **先处理目录**：对每个子目录执行 `FileFilterPolicy.decide()`，判断是删除、保留还是继续遍历。如果目录被判定为 DELETE 且其 `mtime` 超过时间阈值，则递归删除整个目录。
3. **再处理文件**：对每个文件先通过 `FileChecker` 三重检查（失败历史、mtime 阈值、文件锁），再通过规则引擎决策，最后执行传输或删除。
4. 如果启用了 `remove_empty_dirs`，任务结束后自底向上清理空目录。

**move 与 copy 的区别**：仅在于传输时调用 `shutil.move` 还是 `shutil.copy2`。

**whitelist 变体的区别**：在执行 keep/delete 规则判断之前，先引入白名单过滤——只有命中白名单的文件/目录才会进入后续流程，未命中的直接跳过。

### 2. 轮转模式（rotate）

**处理器**：[`RotateHandler`](src/core/handlers/rotate.py)

**工作流程**：

1. 扫描源目录下所有文件，构建「分组统计」（`RotateGroupManager`）。
2. 分组统计支持两个层级：
   - **全局限制**：`size_limit`（总体积上限）和 `count_limit`（总数量上限）
   - **规则级限制**：`rotate_rules.size` 和 `rotate_rules.count`，按命名模式匹配分组，每组有独立的大小或数量上限
3. 将所有文件按 `mtime` 升序排列（最旧的在前）。
4. 从最旧的文件开始迭代，依次检查该文件所属的分组是否超出限制。如果任一关联分组已超限，则处理该文件（移动或删除）。
5. 每处理完一个文件，更新该文件所属的所有分组的统计值。
6. 循环直到所有分组均不超限为止。
7. 如果文件已全部处理完毕但仍有分组超限，则记录警告日志。

**重要细节**：轮转模式下**不使用** `mtime_threshold_minutes`，文件的「新旧」仅由 `mtime` 决定——最旧的最先被处理。如果同时配置了 `dest`，超限文件会被移动到目标目录；如果未配置 `dest`，文件将被留在原地（除非被 delete_rules 匹配删除）。

### 3. 同步模式（sync）

**处理器**：[`SyncHandler`](src/core/handlers/sync.py)

**工作流程**：

1. 在 Linux 上调用 `rsync -av --delete` 进行镜像同步（源目录和目标目录完全一致，目标中多余的文件会被删除）。
2. 在 Windows 上调用 `rclone sync` 实现同等效果。
3. 支持通过 `exclude` 列表排除不需要同步的文件/目录（通配符由底层工具处理）。
4. 可选开启备份功能（`create_backups`），在替换/删除文件前将其备份到 `.smart-archiver.backups/<时间戳>/` 目录，并通过 `max_backups` 限制保留的备份份数。

**重要**：同步模式**不支持** SmartArchiver 的 keep/delete/whitelist 规则系统，所有过滤通过 rsync/rclone 的 `--exclude` 参数实现。

---

## 规则系统工作原理

**核心模块**：[`FileFilterPolicy`](src/core/filters.py)

规则系统决定了每个文件或目录的最终命运——**传输**、**删除**或**跳过（保留）**。

### 规则类型

| 规则 | 作用 | 适用模式 |
|------|------|----------|
| `keep_rules` | 命中后保留文件/目录，不传输不删除 | move / copy / whitelist / rotate |
| `delete_rules` | 命中后直接删除文件/目录 | move / copy / whitelist / rotate |
| `whitelist_rules` | 白名单过滤，仅命中项进入后续流程 | whitelist_move / whitelist_copy |
| `rotate_rules` | 轮转限制条件，触发文件处理 | rotate |

### 规则结构

每组规则（keep/delete/whitelist）内部包含两类阈值匹配：

- **`lt`（less than）**：当目标**小于**阈值时命中
- **`ge`（greater or equal）**：当目标**大于等于**阈值时命中

每个模式（pattern）的值是一个大小字符串（如 `"10MB"`、`"1GB"`）或特殊值 `-1`（匹配所有大小）。模式支持通配符 `*` 和 `?`。

### 文件与目录的区分

规则系统通过模式末尾是否带 `/` 来区分匹配目标：

- **`"*.log"`**（无尾部 `/`）→ 匹配**文件**
- **`"logs/"`**（有尾部 `/`）→ 匹配**目录**

例如 `lt."*.log" = "10MB"` 表示"匹配小于 10MB 的 .log 文件"；`ge."backups/" = "1GB"` 表示"匹配大于等于 1GB 的 backups 目录"。

### 多级路径匹配

模式支持多级路径，通过 `/` 分隔：

- `"logs/*.log"` — 匹配 `logs/` 目录下所有 `.log` 后缀的文件
- `"data/*/cache/"` — 匹配 `data/` 下一级子目录中的 `cache/` 目录
- `"alpha/beta/charlie.txt"` — 精确匹配路径 `alpha/beta/charlie.txt`

### 惰性求值（Lazy Evaluation）

目录大小的获取需要递归扫描，开销较大。规则引擎仅在必要时才触发大小计算：

1. 先检查名称是否匹配模式（字符串比较，开销极低）。
2. 如果存在 `ge:"*" = -1` 这样的无条件命中规则，直接返回结果，**不计算大小**。
3. 只有名称匹配且规则有实际大小阈值时，才调用 `get_dir_size_and_mtime()` 逐个文件扫描。

这一设计使得大部分目录能在步骤 1 或 2 就完成判断，避免不必要的磁盘 I/O。

### 规则优先级（Ambiguity Resolution）

当一个文件同时命中 keep_rules 和 delete_rules 时，由 `preferred_rule` 决定最终行为：

- `preferred_rule = "keep"`（默认）：**保留**该文件（遵循 keep_rules）
- `preferred_rule = "delete"`：**删除**该文件（遵循 delete_rules）

### 白名单的父目录继承

在白名单模式下，如果一个**目录**命中了 whitelist_rules，该目录会被记录到 `whitelisted_dirs` 集合中。后续遍历时，该目录下的**所有子文件和子目录**会自动继承白名单状态——即便它们自身的名称不匹配任何白名单规则。

这个设计解决了"我只配置了 `whitelist_rules.ge."videos/" = -1`，`videos/` 下的 `subdir/` 目录是否会被处理？"这类问题——答案是**会**，因为 `subdir/` 的父目录 `videos/` 已在白名单中。

---

## 容易产生歧义的功能逻辑

### 1. `mtime_threshold_minutes` 的适用范围

**仅在 move/copy/whitelist_move/whitelist_copy 模式下生效**。轮转模式和同步模式不使用此参数。

在标准模式下，它同时作用于**传输**和**删除**——即便是 delete_rules 匹配的文件，也必须先满足 mtime 阈值才会被删除。这意味着你无法通过 delete_rules 删除最近才修改过的文件。

### 2. 轮转模式的「最旧优先」不等于「全部删除」

轮转模式的逻辑是：**从最旧的文件开始处理，只要所有分组限制都满足就立即停止**。它不是"把所有旧文件都删掉"，而是"刚好处理到满足条件为止"。

例如：配置 `count_limit = 10`，目录有 15 个文件。轮转只会处理最旧的 5 个文件（移动或删除），保留最新的 10 个。

### 3. `rotate_rules` 不支持匹配目录

`rotate_rules` 的键（pattern）**不支持以 `/` 结尾**来匹配目录，仅支持匹配文件路径。如果需要约束某个目录下的文件数量或体积，应使用 `"目录名/*"` 的形式（如 `"logs/*"`），而非 `"logs/"`。

### 4. 轮转中的 keep_rules / delete_rules 作用

轮转模式先通过分组统计确定**哪些文件需要处理**（因超限而需要被移除），然后对每个待处理文件再执行 keep_rules / delete_rules 判断：

- 如果文件命中 `keep_rules`，则**跳过不处理**（该文件的统计值也会被保留）
- 如果文件命中 `delete_rules`，则**直接删除**而不是移动到目标目录
- 如果都不命中，且配置了 `dest`，则**移动到目标目录**

一个典型用法：`delete_rules.ge."*" = -1` — 让所有轮转产生的文件都直接删除而不是移动，实现纯清理效果。

### 5. 规则只匹配当前目录下的子项名称

在 `os.walk` 遍历过程中，规则引擎接收的是**相对于源目录的路径**。例如源目录是 `./source`，文件是 `./source/a/b/file.txt`，则传递给规则的是 `a/b/file.txt`。因此在配置规则时，应以源目录为根的相对路径来编写模式。

### 6. 空目录清理不会删除符号链接和挂载点

`clean_empty_dirs()` 在遍历时会跳过符号链接和挂载点（`os.path.islink` / `os.path.ismount`），避免意外卸载外部存储或删除特殊目录。

### 7. 冲突策略 `copy` 的含义

`conflict_policy = "copy"` 并非指任务模式为复制的意思。它的含义是：**当目标位置已存在同名文件时，为源文件创建一个带编号的副本**（如 `file.txt` → `file-1.txt`）。"copy" 在这里表示"保留两份"，而非"覆盖"或"跳过"。

---

## 设计考量

### 1. 为什么没有几十种模式和规则？

如果为每种场景都创建一个专属模式，SmartArchiver 的配置将变成一份冗长的菜单——`move_logs_by_age`、`copy_videos_above_size`、`rotate_backups_by_count`、`sync_except_temp`……每种组合都需要一个名字，用户只能在预设的选项中挑选，稍有偏差就无从下手。SmartArchiver 选择了一条不同的路径：**提供少量正交的"动词"（move / copy / rotate / sync）和一套可组合的"条件表达式"（keep_rules / delete_rules / whitelist_rules / rotate_rules）**，让用户像搭积木一样，通过排列组合来表达任意文件管理逻辑。move 配上 `mtime_threshold_minutes` 就是"按时间归档"；rotate 配上 `delete_rules` 就是"纯清理"；whitelist_move 配上 `keep_rules.lt` 就是"只搬运某个目录下超过特定大小的文件"。模式负责"要做什么"（传输/删除/同步），规则负责"对谁做"（命名、大小、时间的筛选条件），两者解耦后，6 种模式 + 4 类规则所能表达的组合远远超过为每种组合单独设计一个模式。工程上，这也意味着规则引擎只需实现一次，所有处理器共享同一套决策逻辑，新增模式时也无需在配置层引入破坏性变更——因为规则语法对任何模式都是统一的。

### 2. 为什么 `rotate_rules` 不支持匹配目录

在 `keep_rules` / `delete_rules` / `whitelist_rules` 中，规则系统通过模式末尾是否带 `/` 来区分匹配目标——`"xxx/"` 匹配目录自身，`"xxx/*"` 匹配目录下的文件。虽然两个模式的匹配对象不同（一个匹配目录、一个匹配文件），但 `delete_rules` 和 `whitelist_rules` 的行为简单直接、歧义小，用户按直觉配置通常不会出问题。

但是，给 `rotate_rules` 设计目录匹配逻辑极为困难。轮转模式的操作单元是**文件**：遍历文件列表、按 `mtime` 排序、逐个移除旧文件以腾出空间。如果引入目录级别的分组，会引发一系列难以自洽的问题：

- **`"xxx/"` 的歧义问题在 `rotate_rules` 中会被严重放大，比如**：

   - `rotate_rules.count."xxx/*" = 5` → 限制 `xxx/` 下的**文件数量**不超过 5 个（合理、直观）
   - `rotate_rules.count."xxx/" = 5` → 名为 xxx 的目录数量不超过 5 个？每个 xxx 目录下的文件数量不超过 5 个？所有名为 xxx 的目录下的总文件数量不超过 5 个？
   - `rotate_rules.size."xxx/" = "1GB"` → 每个 xxx 目录下的文件体积不超过 1GB？所有名为 xxx 的目录下的总体积不超过 1GB？

- **计数单位不统一**：目录和文件如何统一计入同一个 count 限制？一个目录算一个文件还是算 N 个？
- **嵌套目录的归属**：`"a/"` 匹配了目录 `a/`，那么 `a/b/` 是否需要独立分组？还是两个目录共享同一个配额？
- **mtime 排序失效**：轮转从最旧的文件开始处理，但如果一个「旧目录」下有一个「新文件」，应该如何处理？取目录 mtime 还是文件 mtime？
- **移除语义模糊**：轮转一个文件是指移动或删除它，那轮转一个目录是针对整个目录，还是目录中的部分旧文件？

这些问题没有唯一正确的答案，强行设计只会引入逻辑漏洞和配置歧义。因此 `rotate_rules` 刻意保持了**仅匹配文件**的简单语义，将复杂度留给更自然的 `keep_rules` / `delete_rules` 组合去解决（例如用 `delete_rules` 删除整个目录，或用 `keep_rules` 保护目录内的部分文件不被轮转处理）。

---

## 通过组合规则满足特殊需求（示例）

以下示例展示如何通过**模式选择 + 规则组合**实现一些项目没有显式设计但实际可完成的复杂需求：

### 示例 1：只保留最近 7 天的日志，归档其余部分

```toml
mode = "move"
mtime_threshold_minutes = 10080   # 7 天
source = "./logs"
dest = "./log_archive"
keep_rules = {}   # 不保留任何文件，全部移动
delete_rules = {}
```

通过增大 mtime 阈值，只有超过 7 天未修改的文件才会被移动，最近 7 天的文件自动留在原地。

### 示例 2：删除所有 .tmp 和 .cache 文件，但保护 .important.cache

```toml
mode = "move"
mtime_threshold_minutes = 0
source = "./data"
dest = "./backup"
keep_rules.ge."*.important.cache" = -1
delete_rules.ge."*.tmp" = -1
delete_rules.ge."*.cache" = -1
preferred_rule = "delete"
```

设置 `preferred_rule = "delete"` 确保 `*.important.cache`（同时命中 keep 和 delete）被保留；其余 `.tmp` 和 `.cache` 被删除。注意 `mtime_threshold_minutes` 设为 0 使所有文件立即参与处理。

### 示例 3：目录超过 500MB 时仅删除超过 30 天的备份，不删除近期备份

```toml
mode = "rotate"
source = "./backups"
size_limit = "500MB"
keep_rules.ge."*recent*" = -1
delete_rules = {}
```

轮转从最旧文件开始处理（移动或删除），但标记了 `*recent*` 的文件永远不会被处理（命中 keep_rules）。旋转只处理那些非 recent 且最旧的文件。

### 示例 4：只搬运 videos/ 和 music/ 下超过 100MB 的大文件

```toml
mode = "whitelist_move"
source = "./media"
dest = "./archive"
mtime_threshold_minutes = 0
whitelist_rules.ge."videos/*" = -1
whitelist_rules.ge."music/*" = -1
keep_rules.lt."*.mp4" = "100MB"
keep_rules.lt."*.flac" = "100MB"
```

白名单限制了只有 `videos/` 和 `music/` 下的文件参与处理。再通过 keep_rules 将其中小于 100MB 的文件保留在原处。最终效果：只有 `videos/` 和 `music/` 下大于等于 100MB 的文件被移动。

### 示例 5：源目录即归档 — 纯清理旧备份，保留最近 N 份

```toml
mode = "rotate"
source = "./backups"
size_limit = "10GB"
count_limit = 5
rotate_rules.count."*.tar.gz" = 3
delete_rules.ge."*" = -1   # 命中轮转后直接删除，不移动
remove_empty_dirs = true
```

全局最多保留 5 个文件且不超过 10GB，`.tar.gz` 备份最多保留 3 份。由于 `delete_rules.ge."*" = -1`，所有轮转处理都会直接删除，无需目标目录。

### 示例 6：同步媒体库但排除缓存和临时文件

```toml
mode = "sync"
source = "./media_library"
dest = "/mnt/backup/media_library"
exclude = ["*.tmp", "*.partial", "Thumbs.db", ".cache/", "@eaDir/"]
```

通过 rsync 的 exclude 排除临时文件和系统生成的文件，保持媒体库的干净镜像。
