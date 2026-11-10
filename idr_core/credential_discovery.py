"""
Auto-discovery of data warehouse credentials from local development environments.

Inspects standard locations:
- Environment variables (DATABRICKS_*, SNOWFLAKE_*, BIGQUERY_*, DUCKDB_*)
- dbt profiles (~/.dbt/profiles.yml or ./profiles.yml)
- Databricks CLI config (~/.databrickscfg)
- Snowflake CLI config (~/.snowflake/connections.toml or config.toml)
- Google Cloud Application Default Credentials (gcloud ADC)
- Local DuckDB database files (*.duckdb, *.db)
"""

import configparser
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib  # type: ignore
    except ImportError:
        tomllib = None  # type: ignore

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore


def _mask(val: Optional[str], show_last: int = 4) -> str:
    if not val:
        return ""
    if len(val) <= show_last:
        return "****"
    return "*" * (len(val) - show_last) + val[-show_last:]


def _clean_str(val: Optional[Any]) -> str:
    """Trim whitespace and surrounding quotes from configuration values."""
    if val is None:
        return ""
    s = str(val).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _sanitize_host(raw_host: Optional[str]) -> str:
    """Sanitize warehouse hostname to domain/host only (no protocol, port, or path)."""
    cleaned = _clean_str(raw_host)
    if not cleaned:
        return ""
    if cleaned.startswith("https://"):
        cleaned = cleaned[len("https://"):]
    elif cleaned.startswith("http://"):
        cleaned = cleaned[len("http://"):]
    # Strip any path or query parameters (e.g. /sql/1.0/... or ?o=...)
    cleaned = cleaned.split("/")[0].split("?")[0].strip()
    return cleaned


def discover_from_env() -> List[Dict[str, Any]]:
    """Discover credentials defined in active environment variables or local .env files."""
    try:
        from dotenv import load_dotenv

        load_dotenv()
        home_env = Path.home() / ".env"
        if home_env.exists():
            load_dotenv(dotenv_path=home_env)
    except Exception:
        pass

    results: List[Dict[str, Any]] = []

    # 1. Databricks
    db_host = (
        os.environ.get("DATABRICKS_HOST")
        or os.environ.get("DATABRICKS_SERVER_HOSTNAME")
        or os.environ.get("DATABRICKS_URL")
        or os.environ.get("DATABRICKS_WORKSPACE_URL")
    )
    if db_host:
        clean_host = _sanitize_host(db_host)
        token = _clean_str(
            os.environ.get("DATABRICKS_TOKEN")
            or os.environ.get("DATABRICKS_ACCESS_TOKEN")
            or os.environ.get("DATABRICKS_PAT")
            or os.environ.get("DATABRICKS_API_KEY")
        )
        http_path = _clean_str(
            os.environ.get("DATABRICKS_HTTP_PATH")
            or os.environ.get("DATABRICKS_SQL_HTTP_PATH")
        )
        if not http_path:
            wh_id = _clean_str(os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_CLUSTER_ID"))
            if wh_id:
                http_path = f"/sql/1.0/warehouses/{wh_id}"

        catalog = _clean_str(
            os.environ.get("DATABRICKS_CATALOG")
            or os.environ.get("DATABRICKS_DATABASE")
            or "main"
        )
        schema = _clean_str(os.environ.get("DATABRICKS_SCHEMA") or "default")

        results.append({
            "id": "env_databricks",
            "source": "env",
            "platform": "databricks",
            "name": "Databricks (Environment Variables)",
            "description": f"Host: {clean_host}" + (f" | Catalog: {catalog}" if catalog else ""),
            "params": {
                "server_hostname": clean_host,
                "http_path": http_path,
                "access_token": token,
                "catalog": catalog or "main",
                "schema": schema or "default",
            },
            "masked_summary": f"Host: {clean_host} | Token: {_mask(token)}",
        })

    # 2. Snowflake
    sf_account = os.environ.get("SNOWFLAKE_ACCOUNT")
    if sf_account:
        user = os.environ.get("SNOWFLAKE_USER", "")
        password = os.environ.get("SNOWFLAKE_PASSWORD", "")
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
        database = os.environ.get("SNOWFLAKE_DATABASE", "")
        schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
        results.append({
            "id": "env_snowflake",
            "source": "env",
            "platform": "snowflake",
            "name": "Snowflake (Environment Variables)",
            "description": f"Account: {sf_account}" + (f" | User: {user}" if user else ""),
            "params": {
                "account": sf_account,
                "user": user,
                "password": password,
                "warehouse": warehouse,
                "database": database,
                "schema": schema,
            },
            "masked_summary": f"Account: {sf_account} | User: {user} | Pass: {_mask(password)}",
        })

    # 3. BigQuery
    bq_project = (
        os.environ.get("BIGQUERY_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCP_PROJECT")
    )
    if bq_project:
        results.append({
            "id": "env_bigquery",
            "source": "env",
            "platform": "bigquery",
            "name": "Google BigQuery (Environment Variables)",
            "description": f"Project: {bq_project}",
            "params": {
                "project": bq_project,
                "location": os.environ.get("BIGQUERY_LOCATION", "US"),
            },
            "masked_summary": f"Project: {bq_project}",
        })

    # 4. DuckDB
    duckdb_path = os.environ.get("IDR_DB_PATH") or os.environ.get("DUCKDB_PATH")
    if duckdb_path:
        results.append({
            "id": "env_duckdb",
            "source": "env",
            "platform": "duckdb",
            "name": "DuckDB (Environment Variables)",
            "description": f"Path: {duckdb_path}",
            "params": {
                "path": duckdb_path,
                "database": duckdb_path,
            },
            "masked_summary": f"Path: {duckdb_path}",
        })

    return results


def discover_from_databrickscfg(config_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Discover credentials from ~/.databrickscfg with support for headless INI, profiles, and tokens."""
    results: List[Dict[str, Any]] = []
    cfg_file = config_path or (Path.home() / ".databrickscfg")
    if not cfg_file.exists():
        return results

    try:
        content = cfg_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        # Check if file has standard INI section headers [SECTION]
        has_section = any(
            line.strip().startswith("[") and line.strip().endswith("]")
            for line in lines
            if line.strip() and not line.strip().startswith(("#", ";"))
        )
        if not has_section:
            # Headless configuration: wrap content in a default section
            content = "[DEFAULT]\n" + content

        # Use RawConfigParser to prevent interpolation errors on tokens containing '%'
        parser = configparser.RawConfigParser()
        parser.read_string(content)

        sections: List[str] = list(parser.sections())
        # RawConfigParser default section handling
        if "DEFAULT" in parser and "DEFAULT" not in sections:
            sections.insert(0, "DEFAULT")

        for section in sections:
            sec_name = str(section).strip("'\" ")
            sec_dict = dict(parser[section])
            raw_host = (
                sec_dict.get("host")
                or sec_dict.get("server_hostname")
                or sec_dict.get("server-hostname")
                or sec_dict.get("workspace_url")
                or sec_dict.get("workspace-url")
                or ""
            )
            clean_host = _sanitize_host(raw_host)
            if not clean_host:
                continue

            token = _clean_str(
                sec_dict.get("token")
                or sec_dict.get("access_token")
                or sec_dict.get("access-token")
                or sec_dict.get("personal_access_token")
                or sec_dict.get("password")
                or sec_dict.get("pwd")
            )
            http_path = _clean_str(
                sec_dict.get("http_path")
                or sec_dict.get("http-path")
                or sec_dict.get("sql_endpoint_path")
            )
            if not http_path:
                wh_id = _clean_str(sec_dict.get("warehouse_id") or sec_dict.get("cluster_id"))
                if wh_id:
                    http_path = f"/sql/1.0/warehouses/{wh_id}"

            catalog = _clean_str(sec_dict.get("catalog") or sec_dict.get("database") or "main")
            schema = _clean_str(sec_dict.get("schema") or "default")

            results.append({
                "id": f"cli_databricks_{sec_name.lower()}",
                "source": "cli_config",
                "platform": "databricks",
                "name": f"Databricks CLI Profile [{sec_name}]",
                "description": f"Host: {clean_host}" + (f" | Warehouse: {http_path.split('/')[-1]}" if http_path else ""),
                "params": {
                    "server_hostname": clean_host,
                    "http_path": http_path,
                    "access_token": token,
                    "catalog": catalog or "main",
                    "schema": schema or "default",
                },
                "masked_summary": f"Host: {clean_host} | Token: {_mask(token)}",
            })
    except Exception:
        pass

    return results


def discover_from_snowflake_config(config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Discover connections from ~/.snowflake/connections.toml or config.toml."""
    results: List[Dict[str, Any]] = []
    if not tomllib:
        return results

    base_dir = config_dir or (Path.home() / ".snowflake")
    candidates = [base_dir / "connections.toml", base_dir / "config.toml"]

    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            data = tomllib.loads(candidate.read_text())
            for name, cfg in data.items():
                if isinstance(cfg, dict) and ("account" in cfg or "user" in cfg):
                    account = str(cfg.get("account", ""))
                    user = str(cfg.get("user", ""))
                    password = str(cfg.get("password", ""))
                    warehouse = str(cfg.get("warehouse", ""))
                    database = str(cfg.get("database", ""))
                    schema = str(cfg.get("schema", "PUBLIC"))
                    results.append({
                        "id": f"cli_snowflake_{name.lower().replace('.', '_')}",
                        "source": "cli_config",
                        "platform": "snowflake",
                        "name": f"Snowflake CLI Connection [{name}]",
                        "description": f"Account: {account}" + (f" | User: {user}" if user else ""),
                        "params": {
                            "account": account,
                            "user": user,
                            "password": password,
                            "warehouse": warehouse,
                            "database": database,
                            "schema": schema,
                        },
                        "masked_summary": f"Account: {account} | User: {user} | Pass: {_mask(password)}",
                    })
        except Exception:
            pass

    return results


def discover_from_dbt_profiles(profiles_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Discover targets from ~/.dbt/profiles.yml or local profiles.yml."""
    results: List[Dict[str, Any]] = []
    if not yaml:
        return results

    candidates = [
        profiles_path or (Path.home() / ".dbt" / "profiles.yml"),
        Path.cwd() / "profiles.yml",
    ]

    seen_ids = set()

    for candidate in candidates:
        if not candidate or not candidate.exists():
            continue
        try:
            data = yaml.safe_load(candidate.read_text())
            if not isinstance(data, dict):
                continue

            for profile_name, profile_cfg in data.items():
                if not isinstance(profile_cfg, dict):
                    continue
                outputs = profile_cfg.get("outputs", {})
                target_default = profile_cfg.get("target")

                if not isinstance(outputs, dict):
                    continue

                for out_name, out_cfg in outputs.items():
                    if not isinstance(out_cfg, dict):
                        continue
                    db_type = str(out_cfg.get("type", "")).lower()
                    unique_id = f"dbt_{profile_name}_{out_name}_{db_type}".lower()

                    if unique_id in seen_ids:
                        continue
                    seen_ids.add(unique_id)

                    is_default = (out_name == target_default)
                    default_suffix = " (default target)" if is_default else ""

                    if db_type == "snowflake":
                        account = str(out_cfg.get("account", ""))
                        user = str(out_cfg.get("user", ""))
                        password = str(out_cfg.get("password", ""))
                        warehouse = str(out_cfg.get("warehouse", ""))
                        database = str(out_cfg.get("database", ""))
                        schema = str(out_cfg.get("schema", "PUBLIC"))
                        results.append({
                            "id": unique_id,
                            "source": "dbt_profile",
                            "platform": "snowflake",
                            "name": f"dbt Profile: {profile_name} [{out_name}]{default_suffix}",
                            "description": f"Account: {account} | DB: {database}",
                            "params": {
                                "account": account,
                                "user": user,
                                "password": password,
                                "warehouse": warehouse,
                                "database": database,
                                "schema": schema,
                            },
                            "masked_summary": f"Account: {account} | Wh: {warehouse} | Db: {database}",
                        })

                    elif db_type == "databricks":
                        host = str(out_cfg.get("host", "")).replace("https://", "").rstrip("/")
                        http_path = str(out_cfg.get("http_path", ""))
                        token = str(out_cfg.get("token", ""))
                        catalog = str(out_cfg.get("catalog", "main"))
                        results.append({
                            "id": unique_id,
                            "source": "dbt_profile",
                            "platform": "databricks",
                            "name": f"dbt Profile: {profile_name} [{out_name}]{default_suffix}",
                            "description": f"Host: {host} | Catalog: {catalog}",
                            "params": {
                                "server_hostname": host,
                                "http_path": http_path,
                                "access_token": token,
                                "catalog": catalog,
                            },
                            "masked_summary": f"Host: {host} | Token: {_mask(token)}",
                        })

                    elif db_type in ("bigquery",):
                        project = str(out_cfg.get("project", ""))
                        results.append({
                            "id": unique_id,
                            "source": "dbt_profile",
                            "platform": "bigquery",
                            "name": f"dbt Profile: {profile_name} [{out_name}]{default_suffix}",
                            "description": f"Project: {project}",
                            "params": {
                                "project": project,
                                "location": str(out_cfg.get("location", "US")),
                            },
                            "masked_summary": f"Project: {project}",
                        })

                    elif db_type in ("duckdb",):
                        path = str(out_cfg.get("path", "retail.duckdb"))
                        results.append({
                            "id": unique_id,
                            "source": "dbt_profile",
                            "platform": "duckdb",
                            "name": f"dbt Profile: {profile_name} [{out_name}]{default_suffix}",
                            "description": f"Path: {path}",
                            "params": {
                                "path": path,
                                "database": path,
                            },
                            "masked_summary": f"Path: {path}",
                        })
        except Exception:
            pass

    return results


def discover_from_gcloud_adc(adc_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Discover Google Cloud Application Default Credentials."""
    results: List[Dict[str, Any]] = []
    candidate = adc_path or (Path.home() / ".config" / "gcloud" / "application_default_credentials.json")
    if not candidate.exists():
        return results

    try:
        data = json.loads(candidate.read_text())
        account = data.get("account") or "gcloud-adc"
        quota_project = data.get("quota_project_id") or ""
        results.append({
            "id": "gcloud_adc",
            "source": "gcloud_adc",
            "platform": "bigquery",
            "name": f"Google Cloud ADC ({account})",
            "description": "Authenticated via `gcloud auth application-default login`" + (f" (Project: {quota_project})" if quota_project else ""),
            "params": {
                "project": quota_project or os.environ.get("GOOGLE_CLOUD_PROJECT", ""),
                "location": "US",
            },
            "masked_summary": f"Account: {account}",
        })
    except Exception:
        pass

    return results


def discover_local_duckdb_files(search_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Discover local DuckDB database files in project/data directories."""
    results: List[Dict[str, Any]] = []
    base_dir = search_dir or Path.cwd()

    candidates = [
        base_dir / "retail.duckdb",
        base_dir / "data" / "identity_graph.db",
        base_dir / "identity_graph.db",
        base_dir / "data" / "retail.duckdb",
    ]

    for c in candidates:
        if c.exists() and c.is_file():
            results.append({
                "id": f"local_duckdb_{c.stem}",
                "source": "local_file",
                "platform": "duckdb",
                "name": f"Local DuckDB ({c.name})",
                "description": f"File path: {c}",
                "params": {
                    "path": str(c),
                    "database": str(c),
                },
                "masked_summary": f"Path: {c.name}",
            })

    return results


def discover_all_connections() -> List[Dict[str, Any]]:
    """
    Run all discovery providers and return deduplicated list of available warehouse configurations.
    Priority order:
    1. Active Environment Variables
    2. dbt Profiles (~/.dbt/profiles.yml)
    3. Platform CLI configs (Databricks / Snowflake)
    4. Google Cloud ADC
    5. Local database files
    """
    all_conns: List[Dict[str, Any]] = []
    all_conns.extend(discover_from_env())
    all_conns.extend(discover_from_dbt_profiles())
    all_conns.extend(discover_from_databrickscfg())
    all_conns.extend(discover_from_snowflake_config())
    all_conns.extend(discover_from_gcloud_adc())
    all_conns.extend(discover_local_duckdb_files())

    # Deduplicate by platform + core identifier
    deduped: List[Dict[str, Any]] = []
    seen = set()

    for item in all_conns:
        # Create composite fingerprint
        p = item.get("platform")
        params = item.get("params", {})
        fingerprint = (
            p,
            params.get("server_hostname")
            or params.get("account")
            or params.get("project")
            or params.get("path")
            or item.get("id"),
        )
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        deduped.append(item)

    return deduped


# Convenience alias for CLI and external consumers
discover_all = discover_all_connections

