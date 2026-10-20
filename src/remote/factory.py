"""Remote client factory: creates RemoteClient instances from configuration."""

import re
from typing import Any

from src.remote.client import RemoteClient

_ALIAS_PATTERN = re.compile(r"^[a-zA-Z0-9\-_]+$")


def validate_remote_alias(alias: str) -> bool:
    """Return True if alias contains only alphanumeric chars, hyphens, and underscores."""
    return bool(_ALIAS_PATTERN.match(alias))


def parse_remote_config(config: dict[str, Any]) -> dict[str, RemoteClient]:
    """Parse http_remotes from config dict into {alias: RemoteClient} dict."""
    remotes: dict[str, RemoteClient] = {}
    http_remotes = config.get("http_remotes", [])
    if not http_remotes:
        return remotes

    for entry in http_remotes:
        alias = entry.get("alias", "")
        address = entry.get("address", "").strip()
        key = entry.get("key", "")
        timeout = entry.get("timeout")
        queue_time = entry.get("queue_time", 0)

        if not alias or not address or not key:
            continue

        if not validate_remote_alias(alias):
            continue

        if timeout is not None:
            try:
                timeout = float(timeout)
            except TypeError, ValueError:
                timeout = None

        if queue_time is not None:
            try:
                queue_time = float(queue_time)
            except TypeError, ValueError:
                queue_time = 0.0

        remotes[alias] = RemoteClient(
            address=address,
            api_key=key,
            alias=alias,
            timeout=timeout,
            queue_time=queue_time,
        )

    return remotes
