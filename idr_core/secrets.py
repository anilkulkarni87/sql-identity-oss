"""
Utilities for loading runtime config values and file-backed secrets.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Return an environment value, treating empty strings as unset."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    value = raw_value.strip()
    return value if value else default


def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    """Resolve secret from <NAME>_FILE first, then <NAME>.

    This pattern is compatible with Docker/Kubernetes secret mounts and allows
    runtime token rotation when callers fetch per use.
    """
    file_var = f"{name}_FILE"
    secret_file = get_env(file_var)
    if secret_file:
        try:
            value = Path(secret_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise RuntimeError(f"Unable to read secret file {secret_file} for {file_var}") from exc
        return value if value else default
    return get_env(name, default=default)
