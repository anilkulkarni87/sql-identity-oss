"""
Connection session store abstraction for adapter lifecycle management.

OSS default is an in-memory implementation. Enterprise distributions can
provide a custom backend by setting IDR_SESSION_STORE_CLASS to a dotted class
path (e.g. "idr_enterprise.session_store.RedisConnectionSessionStore").
"""

from __future__ import annotations

import importlib
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Dict, Optional

from idr_core.adapters.base import IDRAdapter


@dataclass
class ConnectionState:
    adapter: IDRAdapter
    config: Dict[str, Any] = field(default_factory=dict)
    last_used_at: float = field(default_factory=time.time)


class ConnectionSessionStore(ABC):
    @abstractmethod
    def set_adapter(self, user_key: str, adapter: IDRAdapter, config: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def get_adapter(self, user_key: str) -> Optional[IDRAdapter]:
        pass

    @abstractmethod
    def get_config(self, user_key: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def disconnect_user(self, user_key: str) -> None:
        pass

    @abstractmethod
    def disconnect_all(self) -> None:
        pass

    @abstractmethod
    def get_any_adapter(self) -> Optional[IDRAdapter]:
        pass

    @abstractmethod
    def connection_count(self) -> int:
        pass


class InMemoryConnectionSessionStore(ConnectionSessionStore):
    def __init__(self, ttl_seconds: int = 3600):
        self._states: Dict[str, ConnectionState] = {}
        self._ttl_seconds = max(0, int(ttl_seconds))
        self._lock = RLock()

    def _close_state(self, state: Optional[ConnectionState]) -> None:
        if not state:
            return
        try:
            state.adapter.close()
        except Exception:
            pass

    def _cleanup_idle_locked(self) -> None:
        if self._ttl_seconds <= 0:
            return
        now = time.time()
        expired = [
            key
            for key, state in self._states.items()
            if (now - state.last_used_at) > self._ttl_seconds
        ]
        for key in expired:
            self._close_state(self._states.pop(key, None))

    def set_adapter(self, user_key: str, adapter: IDRAdapter, config: Dict[str, Any]) -> None:
        with self._lock:
            self._cleanup_idle_locked()
            previous = self._states.get(user_key)
            if previous and previous.adapter is not adapter:
                self._close_state(previous)
            self._states[user_key] = ConnectionState(
                adapter=adapter,
                config=dict(config),
                last_used_at=time.time(),
            )

    def get_adapter(self, user_key: str) -> Optional[IDRAdapter]:
        with self._lock:
            self._cleanup_idle_locked()
            state = self._states.get(user_key)
            if not state:
                return None
            state.last_used_at = time.time()
            return state.adapter

    def get_config(self, user_key: str) -> Dict[str, Any]:
        with self._lock:
            self._cleanup_idle_locked()
            state = self._states.get(user_key)
            return dict(state.config) if state else {}

    def disconnect_user(self, user_key: str) -> None:
        with self._lock:
            self._close_state(self._states.pop(user_key, None))

    def disconnect_all(self) -> None:
        with self._lock:
            for key in list(self._states.keys()):
                self._close_state(self._states.pop(key, None))

    def get_any_adapter(self) -> Optional[IDRAdapter]:
        with self._lock:
            self._cleanup_idle_locked()
            if not self._states:
                return None
            first_key = next(iter(self._states.keys()))
            state = self._states.get(first_key)
            if not state:
                return None
            state.last_used_at = time.time()
            return state.adapter

    def connection_count(self) -> int:
        with self._lock:
            self._cleanup_idle_locked()
            return len(self._states)


def _build_custom_store(class_path: str, ttl_seconds: int) -> ConnectionSessionStore:
    module_name, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)

    # Support a few constructor signatures for custom stores.
    try:
        store = cls(ttl_seconds=ttl_seconds)
    except TypeError:
        try:
            store = cls(ttl_seconds)
        except TypeError:
            store = cls()

    required_methods = (
        "set_adapter",
        "get_adapter",
        "get_config",
        "disconnect_user",
        "disconnect_all",
        "get_any_adapter",
        "connection_count",
    )
    for method in required_methods:
        if not hasattr(store, method) or not callable(getattr(store, method)):
            raise TypeError(f"Custom session store missing callable '{method}'")

    return store


def load_connection_session_store(ttl_seconds: int = 3600) -> ConnectionSessionStore:
    class_path = os.getenv("IDR_SESSION_STORE_CLASS", "").strip()
    if not class_path:
        return InMemoryConnectionSessionStore(ttl_seconds=ttl_seconds)

    return _build_custom_store(class_path, ttl_seconds)
