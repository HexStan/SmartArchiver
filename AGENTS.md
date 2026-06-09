# AGENTS.md — SmartArchiver

## Setup & Run

- Requires **Python 3.11+** (uses `tomllib`, no `toml` pip package needed).
- `pip install -r requirements.txt`
- Config: copy `config/config.example.toml` → `config/config.toml` (client) or `config/config.server.example.toml` → `config/config.server.toml` (server).
- Run: `python main.py` (client) or `python main.py --server` (server).
- Docker build context: repo root, Dockerfile at `docker/app/Dockerfile`. CI publishes to `ghcr.io/hexstan/smart-archiver` on tag push, multi-arch (`linux/amd64`, `linux/arm64`).

## Architecture

- **Flat package**: `main.py` is the single entry point. `src/` is the library.
- **Handler registry**: handlers self-register via `@register_handler("move", "copy", …)` decorator. `src/core/__init__.py` `import src.core.handlers` triggers this as a side effect — removing that import breaks the registry.
- **AppContext** (`src/app_context.py`): module-level singleton. All code reads config/logger/history through `AppContext.get()`. Must call `AppContext.init()` once before any task processing.
- **DestBackend** (`src/core/backend.py`): abstracts local, HTTP remote, and SSH remote destinations. SSH backend **only supports sync mode**; per-file ops (`exists`, `copy_file`, etc.) raise `NotImplementedError`.
- **fs_ops** (`src/fs_ops.py`): all local filesystem primitives. Does **not** depend on AppContext; logs via standard `logging` (wired to same handlers in `setup_logger()`). Use this for any new local file I/O instead of raw `os`/`shutil`.

### 6 task modes → 3 handlers

| Modes | Handler |
|---|---|
| `move`, `copy`, `whitelist_move`, `whitelist_copy` | `StandardHandler` |
| `rotate` | `RotateHandler` |
| `sync` | `SyncHandler` |

### Remote dest URL syntax

Dest paths use `{type:alias}?path` format:
- `{http:my_nas}?/vol/backup` → HTTP remote
- `{ssh:my_vps}?/var/data` → SSH remote (sync mode only)

HTTP and SSH remote namespaces are completely isolated — same alias can exist in both.

## Testing

- No test suite exists. `tests/` is in `.gitignore`. There is no lint/format/typecheck configuration (no `ruff`, `black`, `mypy`).
- `pytest.ini` exists but points to a non-existent `tests` directory.

## Gotchas

- **File locking on Windows**: `SingleInstance` lock and `is_file_locked()` are no-ops on Windows (`fcntl` unavailable). No multi-instance protection on Windows.
- **Sync mode requires external tools**: `rsync` on Linux, `rclone` on Windows (and in PATH). Auto-detection via `os.name == "nt"`.
- **`mtime_threshold_minutes` only applies to Standard modes** (move/copy/whitelist). Not used by rotate or sync.
- **Rotate mode** does NOT support directory patterns (no trailing `/` in `rotate_rules` keys). Uses "oldest-first" strategy — stops as soon as all limits are satisfied.
- **Config is TOML**: `[[tasks]]`, `[[http_remotes]]`, `[[ssh_remotes]]` are arrays of tables. Rules inside tasks use dotted sub-tables like `[tasks.keep_rules.lt]`.

## Conventions

- Git commit messages: **Chinese** (漢语). See `.trae/rules/git-commit-message.md`.
- Logs: custom `SUCCESS` level (25), `DualFormatter` supports `raw=True` extra for unformatted output.
- `LoggerWrapper` re-encodes messages via `surrogateescape` then `replace` to survive undecodable filenames.
