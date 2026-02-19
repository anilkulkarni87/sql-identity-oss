from typing import Any, Dict, Optional

from idr_core.adapters.base import IDRAdapter


class ConnectionManager:
    """
    Singleton manager for database connections.
    Handles connection lifecycle, validation, and retrieval.
    """

    _instance = None
    _adapter: Optional[IDRAdapter] = None
    _connection_params: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConnectionManager, cls).__new__(cls)
        return cls._instance

    def get_adapter(self) -> Optional[IDRAdapter]:
        """
        Get the active adapter instance.
        If connection is stale/closed, could attempt reconnect here (future improvement).
        """
        return self._adapter

    def set_adapter(self, adapter: IDRAdapter, params: Dict[str, Any]):
        """
        Set a new active adapter and store params for potential reconnection.
        Closes existing adapter if present.
        """
        if self._adapter:
            try:
                self._adapter.close()
            except Exception as e:
                print(f"Warning: Failed to close old adapter: {e}")

        self._adapter = adapter
        self._connection_params = params

    def is_connected(self) -> bool:
        return self._adapter is not None

    def disconnect(self):
        if self._adapter:
            try:
                self._adapter.close()
            except Exception:
                pass
            self._adapter = None
            self._connection_params = {}

    # Helper to get singleton instance easily
    @classmethod
    def instance(cls):
        return cls()
