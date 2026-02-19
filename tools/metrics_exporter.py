"""
IDR Metrics Export Plugin Framework

This module provides a plugin-based architecture for exporting IDR metrics
to various monitoring providers. The core framework reads from the
idr_out.metrics_export table and delegates to provider-specific plugins.

Usage:
    python metrics_exporter.py --provider prometheus --port 9090
    python metrics_exporter.py --provider webhook --url https://your-endpoint.com/metrics
    python metrics_exporter.py --provider stdout

Plugins:
    - PrometheusPlugin: Exposes metrics via HTTP endpoint for Prometheus scraping
    - WebhookPlugin: Pushes metrics to any HTTP endpoint as JSON
    - StdoutPlugin: Prints metrics to console (for debugging/logging)
    - DataDogPlugin: Push to DataDog API (requires DD_API_KEY env var)

Custom plugins can be added by extending BaseExporterPlugin.
"""

import abc
import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

# Optional imports (provider-specific)
try:
    from prometheus_client import REGISTRY, Counter, Gauge, Histogram, start_http_server

    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# ============================================
# DATA MODELS
# ============================================
class Metric:
    """Represents a single metric reading."""

    def __init__(
        self,
        name: str,
        value: float,
        metric_type: str = "gauge",
        dimensions: Optional[Dict] = None,
        run_id: Optional[str] = None,
        recorded_at: Optional[datetime] = None,
        metric_id: Optional[str] = None,
    ):
        self.metric_id = metric_id
        self.name = name
        self.value = value
        self.metric_type = metric_type
        self.dimensions = dimensions or {}
        self.run_id = run_id
        self.recorded_at = recorded_at or datetime.utcnow()

    def to_dict(self) -> Dict:
        return {
            "metric_id": self.metric_id,
            "name": self.name,
            "value": self.value,
            "type": self.metric_type,
            "dimensions": self.dimensions,
            "run_id": self.run_id,
            "recorded_at": self.recorded_at.isoformat()
            if isinstance(self.recorded_at, datetime)
            else str(self.recorded_at),
        }


# ============================================
# BASE PLUGIN CLASS
# ============================================
class BaseExporterPlugin(abc.ABC):
    """Abstract base class for metrics export plugins."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Plugin name for CLI selection."""
        pass

    @abc.abstractmethod
    def export(self, metrics: List[Metric]) -> bool:
        """Export metrics to the target system. Returns True on success."""
        pass

    def setup(self, **kwargs):
        """Optional setup method called before export loop."""
        pass

    def teardown(self):
        """Optional cleanup method called on shutdown."""
        pass


# ============================================
# BUILT-IN PLUGINS
# ============================================
class StdoutPlugin(BaseExporterPlugin):
    """Prints metrics to stdout (useful for debugging and log aggregation)."""

    @property
    def name(self) -> str:
        return "stdout"

    def export(self, metrics: List[Metric]) -> bool:
        for m in metrics:
            dims = json.dumps(m.dimensions) if m.dimensions else "{}"
            print(
                f"[{m.recorded_at}] {m.name}={m.value} type={m.metric_type} run_id={m.run_id} dims={dims}"
            )
        return True


class WebhookPlugin(BaseExporterPlugin):
    """Pushes metrics to an HTTP endpoint as JSON."""

    def __init__(self, url: str, headers: Optional[Dict] = None, timeout: int = 30):
        if not HAS_REQUESTS:
            raise ImportError("requests library required. Install with: pip install requests")
        self.url = url
        self.headers = headers or {"Content-Type": "application/json"}
        self.timeout = timeout

    @property
    def name(self) -> str:
        return "webhook"

    def export(self, metrics: List[Metric]) -> bool:
        if not metrics:
            return True

        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics_count": len(metrics),
            "metrics": [m.to_dict() for m in metrics],
        }

        try:
            response = requests.post(
                self.url, json=payload, headers=self.headers, timeout=self.timeout
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"[ERROR] Webhook export failed: {e}")
            return False


class PrometheusPlugin(BaseExporterPlugin):
    """Exposes metrics via HTTP for Prometheus scraping."""

    def __init__(self, port: int = 9090):
        if not HAS_PROMETHEUS:
            raise ImportError(
                "prometheus_client required. Install with: pip install prometheus-client"
            )
        self.port = port
        self._gauges = {}
        self._started = False

    @property
    def name(self) -> str:
        return "prometheus"

    def setup(self, **kwargs):
        if not self._started:
            start_http_server(self.port)
            self._started = True
            print(f"[INFO] Prometheus metrics server started on port {self.port}")

    def _get_or_create_gauge(self, name: str, labels: List[str]) -> Gauge:
        key = (name, tuple(sorted(labels)))
        if key not in self._gauges:
            self._gauges[key] = Gauge(name, f"IDR metric: {name}", labels)
        return self._gauges[key]

    def export(self, metrics: List[Metric]) -> bool:
        for m in metrics:
            # Convert dimensions to label names/values
            label_names = list(m.dimensions.keys()) if m.dimensions else []
            label_values = list(m.dimensions.values()) if m.dimensions else []

            # Add run_id as a label
            if m.run_id:
                label_names.append("run_id")
                label_values.append(m.run_id)

            try:
                gauge = self._get_or_create_gauge(m.name, label_names)
                if label_values:
                    gauge.labels(*label_values).set(m.value)
                else:
                    gauge.set(m.value)
            except Exception as e:
                print(f"[WARN] Failed to set metric {m.name}: {e}")

        return True


class DataDogPlugin(BaseExporterPlugin):
    """Pushes metrics to DataDog API."""

    def __init__(self, api_key: Optional[str] = None):
        if not HAS_REQUESTS:
            raise ImportError("requests library required. Install with: pip install requests")
        self.api_key = api_key or os.environ.get("DD_API_KEY")
        if not self.api_key:
            raise ValueError("DataDog API key required. Set DD_API_KEY env var or pass api_key.")
        self.url = "https://api.datadoghq.com/api/v1/series"

    @property
    def name(self) -> str:
        return "datadog"

    def export(self, metrics: List[Metric]) -> bool:
        if not metrics:
            return True

        series = []
        now = int(time.time())

        for m in metrics:
            tags = [f"{k}:{v}" for k, v in m.dimensions.items()] if m.dimensions else []
            if m.run_id:
                tags.append(f"run_id:{m.run_id}")

            series.append(
                {
                    "metric": m.name,
                    "points": [[now, m.value]],
                    "type": "gauge" if m.metric_type == "gauge" else "count",
                    "tags": tags,
                }
            )

        try:
            response = requests.post(
                self.url,
                json={"series": series},
                headers={"Content-Type": "application/json", "DD-API-KEY": self.api_key},
                timeout=30,
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"[ERROR] DataDog export failed: {e}")
            return False


# ============================================
# DATABASE ADAPTERS
# ============================================
class BaseDatabaseAdapter(abc.ABC):
    """Abstract base for database-specific metric readers."""

    @abc.abstractmethod
    def fetch_unexported_metrics(self, limit: int = 1000) -> List[Metric]:
        """Fetch metrics that haven't been exported yet."""
        pass

    @abc.abstractmethod
    def mark_exported(self, metric_ids: List[str]):
        """Mark metrics as exported."""
        pass

    @abc.abstractmethod
    def close(self):
        """Close database connection."""
        pass


class DuckDBAdapter(BaseDatabaseAdapter):
    """DuckDB-specific adapter."""

    def __init__(self, db_path: str):
        import duckdb

        self.con = duckdb.connect(db_path)

    def fetch_unexported_metrics(self, limit: int = 1000) -> List[Metric]:
        rows = self.con.execute(f"""
            SELECT metric_id, run_id, metric_name, metric_value, metric_type, dimensions, recorded_at
            FROM idr_out.metrics_export
            WHERE exported_at IS NULL
            ORDER BY recorded_at
            LIMIT {limit}
        """).fetchall()

        metrics = []
        for row in rows:
            dims = json.loads(row[5]) if row[5] else {}
            metrics.append(
                Metric(
                    metric_id=row[0],
                    run_id=row[1],
                    name=row[2],
                    value=row[3],
                    metric_type=row[4] or "gauge",
                    dimensions=dims,
                    recorded_at=row[6],
                )
            )
        return metrics

    def mark_exported(self, metric_ids: List[str]):
        if not metric_ids:
            return
        ids_str = ",".join([f"'{mid}'" for mid in metric_ids])
        self.con.execute(
            f"UPDATE idr_out.metrics_export SET exported_at = CURRENT_TIMESTAMP WHERE metric_id IN ({ids_str})"
        )

    def close(self):
        self.con.close()


# ============================================
# PLUGIN REGISTRY
# ============================================
PLUGIN_REGISTRY = {
    "stdout": StdoutPlugin,
    "webhook": WebhookPlugin,
    "prometheus": PrometheusPlugin,
    "datadog": DataDogPlugin,
}


def get_plugin(name: str, **kwargs) -> BaseExporterPlugin:
    """Factory function to get a plugin by name."""
    if name not in PLUGIN_REGISTRY:
        raise ValueError(f"Unknown plugin: {name}. Available: {list(PLUGIN_REGISTRY.keys())}")
    return PLUGIN_REGISTRY[name](**kwargs)


# ============================================
# MAIN EXPORTER
# ============================================
class MetricsExporter:
    """Main exporter that coordinates reading from DB and exporting via plugins."""

    def __init__(self, db_adapter: BaseDatabaseAdapter, plugin: BaseExporterPlugin):
        self.db = db_adapter
        self.plugin = plugin

    def run_once(self, batch_size: int = 1000) -> int:
        """Fetch and export one batch of metrics. Returns count exported."""
        metrics = self.db.fetch_unexported_metrics(limit=batch_size)
        if not metrics:
            return 0

        success = self.plugin.export(metrics)
        if success:
            metric_ids = [m.metric_id for m in metrics if m.metric_id]
            self.db.mark_exported(metric_ids)
            return len(metrics)
        return 0

    def run_loop(self, interval: int = 60, batch_size: int = 1000):
        """Continuously export metrics at given interval."""
        self.plugin.setup()
        print(f"[INFO] Starting metrics export loop (interval={interval}s)")

        try:
            while True:
                exported = self.run_once(batch_size)
                if exported > 0:
                    print(f"[INFO] Exported {exported} metrics")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("[INFO] Shutting down...")
        finally:
            self.plugin.teardown()
            self.db.close()


# ============================================
# CLI
# ============================================
def main():
    parser = argparse.ArgumentParser(description="IDR Metrics Exporter")
    parser.add_argument(
        "--provider",
        choices=list(PLUGIN_REGISTRY.keys()),
        default="stdout",
        help="Export provider plugin",
    )
    parser.add_argument("--db", default="idr.duckdb", help="DuckDB database path")
    parser.add_argument("--port", type=int, default=9090, help="Prometheus port")
    parser.add_argument("--url", help="Webhook URL")
    parser.add_argument("--interval", type=int, default=60, help="Export interval in seconds")
    parser.add_argument("--once", action="store_true", help="Run once and exit")

    args = parser.parse_args()

    # Build plugin with appropriate args
    if args.provider == "prometheus":
        plugin = PrometheusPlugin(port=args.port)
    elif args.provider == "webhook":
        if not args.url:
            print("Error: --url required for webhook provider")
            sys.exit(1)
        plugin = WebhookPlugin(url=args.url)
    elif args.provider == "datadog":
        plugin = DataDogPlugin()
    else:
        plugin = StdoutPlugin()

    # Build DB adapter
    db = DuckDBAdapter(args.db)

    # Run exporter
    exporter = MetricsExporter(db, plugin)

    if args.once:
        count = exporter.run_once()
        print(f"Exported {count} metrics")
    else:
        exporter.run_loop(interval=args.interval)


if __name__ == "__main__":
    main()
