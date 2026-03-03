#!/usr/bin/env python
"""
Validate secrets handling posture for enterprise deployment artifacts.

Checks:
1. No plaintext sensitive values in compose runtime env blocks.
2. API services expose *_FILE secret path wiring for webhook bearer token.
3. Webhook bearer token rotates at runtime without process restart.
"""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path
from typing import Dict, List

import yaml

from idr_api.job_manager import SQLiteRunJobManager


def _normalize_environment(environment) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if isinstance(environment, dict):
        for key, value in environment.items():
            result[str(key)] = "" if value is None else str(value)
        return result

    if isinstance(environment, list):
        for item in environment:
            entry = str(item)
            if "=" in entry:
                key, value = entry.split("=", 1)
                result[key] = value
            else:
                result[entry] = ""
    return result


def _is_literal_secret(value: str) -> bool:
    candidate = value.strip()
    if not candidate:
        return False
    if "${" in candidate:
        return False
    return True


def _validate_compose_secret_env(compose_files: List[Path]) -> List[str]:
    errors: List[str] = []
    sensitive_keys = {
        "IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN",
        "KEYCLOAK_ADMIN_PASSWORD",
        "GF_SECURITY_ADMIN_PASSWORD",
    }

    for compose_file in compose_files:
        payload = yaml.safe_load(compose_file.read_text(encoding="utf-8")) or {}
        services = payload.get("services", {})
        for service_name, service in services.items():
            env_map = _normalize_environment(service.get("environment", {}))
            for key in sensitive_keys:
                value = env_map.get(key)
                if value is not None and _is_literal_secret(value):
                    errors.append(
                        f"{compose_file}: service '{service_name}' has plaintext {key} value."
                    )

            is_idr_api_runtime = "IDR_RUN_JOB_DB_PATH" in env_map
            if is_idr_api_runtime:
                if "IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE" not in env_map:
                    errors.append(
                        f"{compose_file}: service '{service_name}' is missing "
                        "IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE."
                    )

        # Enterprise compose should fail fast if admin secrets are missing.
        if "enterprise" in compose_file.name:
            keycloak_env = _normalize_environment(services.get("keycloak", {}).get("environment", {}))
            grafana_env = _normalize_environment(services.get("grafana", {}).get("environment", {}))
            keycloak_password = keycloak_env.get("KEYCLOAK_ADMIN_PASSWORD", "")
            grafana_password = grafana_env.get("GF_SECURITY_ADMIN_PASSWORD", "")
            if ":?" not in keycloak_password:
                errors.append(
                    f"{compose_file}: keycloak KEYCLOAK_ADMIN_PASSWORD should be required "
                    "via ${VAR:?message} syntax."
                )
            if ":?" not in grafana_password:
                errors.append(
                    f"{compose_file}: grafana GF_SECURITY_ADMIN_PASSWORD should be required "
                    "via ${VAR:?message} syntax."
                )

    return errors


def _validate_webhook_rotation() -> None:
    captured_headers: List[str] = []

    class _DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_urlopen(req, timeout):
        del timeout
        captured_headers.append(req.get_header("Authorization"))
        return _DummyResponse()

    import idr_api.job_manager as job_manager_module

    original_urlopen = job_manager_module.urllib_request.urlopen
    original_env = dict(os.environ)
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            token_file = tmp_path / "run_job_webhook.token"
            token_file.write_text("rotation-v1\n", encoding="utf-8")

            os.environ["IDR_RUN_JOB_WEBHOOK_URL"] = "http://example.test/webhook"
            os.environ["IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE"] = str(token_file)
            os.environ.pop("IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN", None)

            job_manager_module.urllib_request.urlopen = _fake_urlopen
            manager = SQLiteRunJobManager(db_path=str(tmp_path / "run_jobs.sqlite3"))
            manager._post_webhook({"event": "first"})

            token_file.write_text("rotation-v2\n", encoding="utf-8")
            manager._post_webhook({"event": "second"})
    finally:
        os.environ.clear()
        os.environ.update(original_env)
        job_manager_module.urllib_request.urlopen = original_urlopen

    expected = ["Bearer rotation-v1", "Bearer rotation-v2"]
    if captured_headers != expected:
        raise RuntimeError(
            "Webhook token rotation validation failed. "
            f"Expected {expected}, observed {captured_headers}."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate secrets posture and rotation behavior.")
    parser.add_argument(
        "--compose",
        action="append",
        default=[],
        help="Compose file to validate (repeatable).",
    )
    args = parser.parse_args()

    compose_files = [Path(path) for path in args.compose]
    if not compose_files:
        compose_files = [
            Path("docker-compose.yml"),
            Path("docker-compose.prod.yml"),
            Path("docker-compose.enterprise.yml"),
            Path("docker-compose.enterprise.ha.yml"),
        ]

    missing = [path for path in compose_files if not path.exists()]
    if missing:
        for path in missing:
            print(f"Missing compose file: {path}")
        return 1

    errors = _validate_compose_secret_env(compose_files)
    if errors:
        print("Secrets posture validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    _validate_webhook_rotation()
    print("Secrets posture and rotation validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
