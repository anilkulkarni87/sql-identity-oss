#!/usr/bin/env python3
"""
IDR Dashboard Generator

Generates a static HTML dashboard from IDR output tables.
Works with all supported platforms: DuckDB, Snowflake, BigQuery, Databricks.

Usage:
    python tools/dashboard/generator.py --platform=duckdb --connection=idr.duckdb --output=dashboard.html
"""

import argparse
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

# ============================================
# ADAPTER BASE CLASS
# ============================================


class DashboardAdapter(ABC):
    """Abstract base class for platform adapters."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the platform."""
        pass

    @abstractmethod
    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL and return list of dicts."""
        pass

    def get_run_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent run history."""
        return self.query(f"""
            SELECT
                run_id, run_mode, started_at, ended_at, status,
                entities_processed, edges_created, clusters_impacted,
                lp_iterations, duration_seconds, groups_skipped,
                values_excluded, large_clusters, warnings
            FROM idr_out.run_history
            ORDER BY started_at DESC
            LIMIT {limit}
        """)

    def get_cluster_distribution(self, buckets: int = 10) -> List[Dict[str, Any]]:
        """Get cluster size distribution."""
        return self.query("""
            SELECT
                CASE
                    WHEN cluster_size = 1 THEN '1 (singleton)'
                    WHEN cluster_size <= 5 THEN '2-5'
                    WHEN cluster_size <= 10 THEN '6-10'
                    WHEN cluster_size <= 50 THEN '11-50'
                    WHEN cluster_size <= 100 THEN '51-100'
                    WHEN cluster_size <= 500 THEN '101-500'
                    WHEN cluster_size <= 1000 THEN '501-1000'
                    ELSE '1000+'
                END as bucket,
                COUNT(*) as cluster_count,
                SUM(cluster_size) as entity_count
            FROM idr_out.identity_clusters_current
            GROUP BY bucket
            ORDER BY
                CASE bucket
                    WHEN '1 (singleton)' THEN 1
                    WHEN '2-5' THEN 2
                    WHEN '6-10' THEN 3
                    WHEN '11-50' THEN 4
                    WHEN '51-100' THEN 5
                    WHEN '101-500' THEN 6
                    WHEN '501-1000' THEN 7
                    ELSE 8
                END
        """)

    def get_largest_clusters(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get largest clusters."""
        return self.query(f"""
            SELECT resolved_id, cluster_size, updated_ts
            FROM idr_out.identity_clusters_current
            ORDER BY cluster_size DESC
            LIMIT {limit}
        """)

    def get_dry_run_summary(self, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get dry run summary."""
        where = f"WHERE run_id = '{run_id}'" if run_id else ""
        return self.query(f"""
            SELECT *
            FROM idr_out.dry_run_summary
            {where}
            ORDER BY analyzed_at DESC
            LIMIT 10
        """)

    def get_dry_run_breakdown(self, run_id: str) -> List[Dict[str, Any]]:
        """Get dry run change type breakdown."""
        return self.query(f"""
            SELECT
                change_type,
                COUNT(*) as entity_count
            FROM idr_out.dry_run_results
            WHERE run_id = '{run_id}'
            GROUP BY change_type
        """)

    def get_metrics_timeline(self, metric_name: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get metric values over time."""
        return self.query(f"""
            SELECT
                DATE(recorded_at) as date,
                AVG(metric_value) as avg_value,
                MAX(metric_value) as max_value
            FROM idr_out.metrics_export
            WHERE metric_name = '{metric_name}'
              AND recorded_at >= CURRENT_DATE - {days}
            GROUP BY DATE(recorded_at)
            ORDER BY date
        """)

    def get_skipped_groups(
        self, run_id: Optional[str] = None, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get skipped identifier groups."""
        where = f"WHERE run_id = '{run_id}'" if run_id else ""
        return self.query(f"""
            SELECT
                run_id, identifier_type, identifier_value_norm,
                group_size, max_allowed, reason, skipped_at
            FROM idr_out.skipped_identifier_groups
            {where}
            ORDER BY skipped_at DESC
            LIMIT {limit}
        """)

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get overall summary statistics."""
        result = self.query("""
            SELECT
                (SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current) as total_entities,
                (SELECT COUNT(*) FROM idr_out.identity_clusters_current) as total_clusters,
                (SELECT MAX(cluster_size) FROM idr_out.identity_clusters_current) as largest_cluster,
                (SELECT COUNT(*) FROM idr_out.run_history) as total_runs,
                (SELECT COUNT(*) FROM idr_out.run_history WHERE status LIKE 'SUCCESS%') as successful_runs
        """)
        return result[0] if result else {}


# ============================================
# DUCKDB ADAPTER
# ============================================


class DuckDBAdapter(DashboardAdapter):
    """Adapter for DuckDB databases."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    def connect(self) -> None:
        import duckdb

        self.conn = duckdb.connect(self.db_path, read_only=True)

    def query(self, sql: str) -> List[Dict[str, Any]]:
        result = self.conn.execute(sql).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in result]


# ============================================
# SNOWFLAKE ADAPTER
# ============================================


class SnowflakeAdapter(DashboardAdapter):
    """Adapter for Snowflake."""

    def __init__(
        self, account: str, user: str, password: str, database: str = None, warehouse: str = None
    ):
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.warehouse = warehouse
        self.conn = None

    def connect(self) -> None:
        import snowflake.connector

        self.conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            warehouse=self.warehouse,
        )

    def query(self, sql: str) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute(sql)
        columns = [desc[0].lower() for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ============================================
# BIGQUERY ADAPTER
# ============================================


class BigQueryAdapter(DashboardAdapter):
    """Adapter for BigQuery."""

    def __init__(self, project: str):
        self.project = project
        self.client = None

    def connect(self) -> None:
        from google.cloud import bigquery

        self.client = bigquery.Client(project=self.project)

    def query(self, sql: str) -> List[Dict[str, Any]]:
        # Replace unqualified table names with project-qualified names
        sql = sql.replace("idr_out.", f"{self.project}.idr_out.")
        sql = sql.replace("idr_meta.", f"{self.project}.idr_meta.")
        result = self.client.query(sql).result()
        return [dict(row) for row in result]


# ============================================
# HTML TEMPLATE
# ============================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IDR Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #e8e8e8;
            min-height: 100vh;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        header {
            text-align: center;
            padding: 30px 0;
            border-bottom: 1px solid rgba(255,255,255,0.1);
            margin-bottom: 30px;
        }
        header h1 {
            font-size: 2.5rem;
            background: linear-gradient(45deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 10px;
        }
        header p {
            color: #888;
            font-size: 0.9rem;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 24px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .stat-card h3 {
            font-size: 0.8rem;
            color: #888;
            text-transform: uppercase;
            margin-bottom: 8px;
        }
        .stat-card .value {
            font-size: 2rem;
            font-weight: bold;
            color: #fff;
        }
        .stat-card .value.success { color: #4ade80; }
        .stat-card .value.warning { color: #fbbf24; }
        .stat-card .value.danger { color: #f87171; }
        .section {
            background: rgba(255,255,255,0.03);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 24px;
            border: 1px solid rgba(255,255,255,0.08);
        }
        .section h2 {
            font-size: 1.2rem;
            margin-bottom: 20px;
            color: #fff;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .section h2::before {
            content: '';
            display: block;
            width: 4px;
            height: 20px;
            background: linear-gradient(45deg, #667eea, #764ba2);
            border-radius: 2px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px 16px;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        th {
            font-size: 0.75rem;
            text-transform: uppercase;
            color: #888;
        }
        td {
            font-size: 0.9rem;
        }
        tr:hover {
            background: rgba(255,255,255,0.02);
        }
        .badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .badge.success { background: rgba(74, 222, 128, 0.2); color: #4ade80; }
        .badge.warning { background: rgba(251, 191, 36, 0.2); color: #fbbf24; }
        .badge.info { background: rgba(96, 165, 250, 0.2); color: #60a5fa; }
        .badge.danger { background: rgba(248, 113, 113, 0.2); color: #f87171; }
        .chart-container {
            position: relative;
            height: 300px;
        }
        .grid-2 {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 24px;
        }
        .empty-state {
            text-align: center;
            padding: 40px;
            color: #666;
        }
        footer {
            text-align: center;
            padding: 20px;
            color: #666;
            font-size: 0.8rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔗 Identity Resolution Dashboard</h1>
            <p>Generated: {{generated_at}} | Platform: {{platform}}</p>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Entities</h3>
                <div class="value">{{total_entities}}</div>
            </div>
            <div class="stat-card">
                <h3>Total Clusters</h3>
                <div class="value">{{total_clusters}}</div>
            </div>
            <div class="stat-card">
                <h3>Largest Cluster</h3>
                <div class="value {{largest_cluster_class}}">{{largest_cluster}}</div>
            </div>
            <div class="stat-card">
                <h3>Success Rate</h3>
                <div class="value success">{{success_rate}}%</div>
            </div>
        </div>

        <div class="section">
            <h2>Run History</h2>
            {{run_history_table}}
        </div>

        <div class="grid-2">
            <div class="section">
                <h2>Cluster Size Distribution</h2>
                <div class="chart-container">
                    <canvas id="clusterChart"></canvas>
                </div>
            </div>
            <div class="section">
                <h2>Top 10 Largest Clusters</h2>
                {{largest_clusters_table}}
            </div>
        </div>

        <div class="section">
            <h2>Recent Dry Runs</h2>
            {{dry_run_table}}
        </div>

        <div class="section">
            <h2>Skipped Identifier Groups</h2>
            {{skipped_groups_table}}
        </div>

        <footer>
            SQL Identity Resolution | <a href="https://github.com/anilkulkarni87/sql-identity-resolution" style="color: #667eea;">GitHub</a>
        </footer>
    </div>

    <script>
        // Cluster distribution chart
        const ctx = document.getElementById('clusterChart').getContext('2d');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: {{cluster_labels}},
                datasets: [{
                    label: 'Cluster Count',
                    data: {{cluster_counts}},
                    backgroundColor: 'rgba(102, 126, 234, 0.6)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: { color: '#888' }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { color: '#888' }
                    }
                }
            }
        });
    </script>
</body>
</html>
"""


# ============================================
# DASHBOARD GENERATOR
# ============================================


class DashboardGenerator:
    """Generates HTML dashboard from IDR data."""

    def __init__(self, adapter: DashboardAdapter):
        self.adapter = adapter

    def generate(self) -> str:
        """Generate the complete HTML dashboard."""
        self.adapter.connect()

        # Get data
        stats = self.adapter.get_summary_stats()
        run_history = self.adapter.get_run_history(20)
        cluster_dist = self.adapter.get_cluster_distribution()
        largest_clusters = self.adapter.get_largest_clusters(10)
        dry_runs = self.adapter.get_dry_run_summary()
        skipped_groups = self.adapter.get_skipped_groups(limit=10)

        # Calculate success rate
        total_runs = stats.get("total_runs", 0) or 0
        successful_runs = stats.get("successful_runs", 0) or 0
        success_rate = round(100 * successful_runs / total_runs, 1) if total_runs > 0 else 0

        # Determine largest cluster class
        largest_cluster = stats.get("largest_cluster", 0) or 0
        largest_cluster_class = (
            "danger" if largest_cluster > 5000 else ("warning" if largest_cluster > 1000 else "")
        )

        # Build HTML
        html = HTML_TEMPLATE
        html = html.replace("{{generated_at}}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        html = html.replace("{{platform}}", type(self.adapter).__name__.replace("Adapter", ""))
        html = html.replace("{{total_entities}}", f"{stats.get('total_entities', 0):,}")
        html = html.replace("{{total_clusters}}", f"{stats.get('total_clusters', 0):,}")
        html = html.replace("{{largest_cluster}}", f"{largest_cluster:,}")
        html = html.replace("{{largest_cluster_class}}", largest_cluster_class)
        html = html.replace("{{success_rate}}", str(success_rate))

        # Run history table
        html = html.replace("{{run_history_table}}", self._build_run_history_table(run_history))

        # Cluster distribution chart data
        html = html.replace(
            "{{cluster_labels}}", json.dumps([d.get("bucket", "") for d in cluster_dist])
        )
        html = html.replace(
            "{{cluster_counts}}", json.dumps([d.get("cluster_count", 0) for d in cluster_dist])
        )

        # Largest clusters table
        html = html.replace(
            "{{largest_clusters_table}}", self._build_largest_clusters_table(largest_clusters)
        )

        # Dry run table
        html = html.replace("{{dry_run_table}}", self._build_dry_run_table(dry_runs))

        # Skipped groups table
        html = html.replace(
            "{{skipped_groups_table}}", self._build_skipped_groups_table(skipped_groups)
        )

        return html

    def _build_run_history_table(self, runs: List[Dict]) -> str:
        if not runs:
            return '<div class="empty-state">No run history available</div>'

        rows = []
        for run in runs:
            status = run.get("status", "UNKNOWN") or "UNKNOWN"
            badge_class = (
                "success"
                if "SUCCESS" in status
                else (
                    "warning" if "WARNING" in status else ("info" if "DRY" in status else "danger")
                )
            )

            # Handle None values
            entities = run.get("entities_processed") or 0
            edges = run.get("edges_created") or 0
            clusters = run.get("clusters_impacted") or 0
            duration = run.get("duration_seconds") or 0
            run_id = run.get("run_id", "") or ""
            run_mode = run.get("run_mode", "") or ""

            rows.append(f"""
                <tr>
                    <td><code>{run_id[:16]}...</code></td>
                    <td>{run_mode}</td>
                    <td><span class="badge {badge_class}">{status}</span></td>
                    <td>{entities:,}</td>
                    <td>{edges:,}</td>
                    <td>{clusters:,}</td>
                    <td>{duration}s</td>
                </tr>
            """)

        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Run ID</th>
                        <th>Mode</th>
                        <th>Status</th>
                        <th>Entities</th>
                        <th>Edges</th>
                        <th>Clusters</th>
                        <th>Duration</th>
                    </tr>
                </thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        """

    def _build_largest_clusters_table(self, clusters: List[Dict]) -> str:
        if not clusters:
            return '<div class="empty-state">No clusters available</div>'

        rows = []
        for cluster in clusters:
            size = cluster.get("cluster_size", 0)
            size_class = "danger" if size > 5000 else ("warning" if size > 1000 else "")

            rows.append(f"""
                <tr>
                    <td><code>{cluster.get("resolved_id", "")[:20]}...</code></td>
                    <td class="{size_class}">{size:,}</td>
                </tr>
            """)

        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Cluster ID</th>
                        <th>Size</th>
                    </tr>
                </thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        """

    def _build_dry_run_table(self, dry_runs: List[Dict]) -> str:
        if not dry_runs:
            return '<div class="empty-state">No dry runs available</div>'

        rows = []
        for dr in dry_runs:
            rows.append(f"""
                <tr>
                    <td><code>{dr.get("run_id", "")[:16]}...</code></td>
                    <td>{dr.get("total_entities", 0):,}</td>
                    <td style="color: #4ade80;">{dr.get("new_entities", 0):,}</td>
                    <td style="color: #fbbf24;">{dr.get("moved_entities", 0):,}</td>
                    <td>{dr.get("unchanged_entities", 0):,}</td>
                    <td>{dr.get("largest_proposed_cluster", 0):,}</td>
                </tr>
            """)

        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Run ID</th>
                        <th>Total</th>
                        <th>New</th>
                        <th>Moved</th>
                        <th>Unchanged</th>
                        <th>Largest Cluster</th>
                    </tr>
                </thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        """

    def _build_skipped_groups_table(self, groups: List[Dict]) -> str:
        if not groups:
            return '<div class="empty-state">No skipped groups (good!)</div>'

        rows = []
        for g in groups:
            rows.append(f"""
                <tr>
                    <td>{g.get("identifier_type", "")}</td>
                    <td><code>{g.get("identifier_value_norm", "")[:30]}...</code></td>
                    <td style="color: #f87171;">{g.get("group_size", 0):,}</td>
                    <td>{g.get("max_allowed", 0):,}</td>
                    <td>{g.get("reason", "")}</td>
                </tr>
            """)

        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Identifier Type</th>
                        <th>Value</th>
                        <th>Group Size</th>
                        <th>Max Allowed</th>
                        <th>Reason</th>
                    </tr>
                </thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        """


# ============================================
# CLI
# ============================================


def main():
    parser = argparse.ArgumentParser(description="Generate IDR Dashboard")
    parser.add_argument(
        "--platform",
        required=True,
        choices=["duckdb", "snowflake", "bigquery"],
        help="Database platform",
    )
    parser.add_argument("--connection", required=True, help="Connection string or path")
    parser.add_argument("--output", default="dashboard.html", help="Output file path")

    # Snowflake-specific
    parser.add_argument("--account", help="Snowflake account")
    parser.add_argument("--user", help="Snowflake user")
    parser.add_argument("--password", help="Snowflake password")
    parser.add_argument("--database", help="Snowflake database")
    parser.add_argument("--warehouse", help="Snowflake warehouse")

    # BigQuery-specific
    parser.add_argument("--project", help="GCP project ID")

    args = parser.parse_args()

    # Create adapter
    if args.platform == "duckdb":
        adapter = DuckDBAdapter(args.connection)
    elif args.platform == "snowflake":
        adapter = SnowflakeAdapter(
            account=args.account or os.environ.get("SNOWFLAKE_ACCOUNT"),
            user=args.user or os.environ.get("SNOWFLAKE_USER"),
            password=args.password or os.environ.get("SNOWFLAKE_PASSWORD"),
            database=args.database,
            warehouse=args.warehouse,
        )
    elif args.platform == "bigquery":
        adapter = BigQueryAdapter(project=args.project or args.connection)
    else:
        print(f"Unsupported platform: {args.platform}")
        return

    # Generate dashboard
    generator = DashboardGenerator(adapter)
    html = generator.generate()

    # Write output
    with open(args.output, "w") as f:
        f.write(html)

    print(f"✅ Dashboard generated: {args.output}")
    print(f"   Open in browser: file://{os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
