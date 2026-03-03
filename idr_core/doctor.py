"""
System diagnostics for install/runtime supportability.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib import request as urllib_request

MIN_PYTHON = (3, 9)
MAX_PYTHON = (3, 12)


@dataclass
class DoctorCheck:
    name: str
    status: str
    severity: str
    detail: str


def _check_python_version() -> DoctorCheck:
    version = sys.version_info
    current = (version.major, version.minor)
    supported = MIN_PYTHON <= current <= MAX_PYTHON
    if supported:
        return DoctorCheck(
            name="python_version",
            status="pass",
            severity="required",
            detail=f"Python {version.major}.{version.minor}.{version.micro} is supported.",
        )
    return DoctorCheck(
        name="python_version",
        status="fail",
        severity="required",
        detail=(
            f"Python {version.major}.{version.minor}.{version.micro} is unsupported. "
            f"Expected {MIN_PYTHON[0]}.{MIN_PYTHON[1]} to {MAX_PYTHON[0]}.{MAX_PYTHON[1]}."
        ),
    )


def _check_module(module: str, severity: str, help_text: str) -> DoctorCheck:
    if importlib.util.find_spec(module):
        return DoctorCheck(
            name=f"python_module:{module}",
            status="pass",
            severity=severity,
            detail=f"Module '{module}' is available.",
        )
    return DoctorCheck(
        name=f"python_module:{module}",
        status="warn" if severity == "optional" else "fail",
        severity=severity,
        detail=help_text,
    )


def _check_command(name: str, command: List[str], severity: str, help_text: str) -> DoctorCheck:
    binary = command[0]
    if shutil.which(binary) is None:
        return DoctorCheck(
            name=name,
            status="warn" if severity == "optional" else "fail",
            severity=severity,
            detail=help_text,
        )
    try:
        proc = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except Exception as exc:
        return DoctorCheck(
            name=name,
            status="warn" if severity == "optional" else "fail",
            severity=severity,
            detail=f"{help_text} (error: {exc})",
        )

    if proc.returncode == 0:
        version_line = (proc.stdout or proc.stderr).strip().splitlines()
        detail = version_line[0] if version_line else "Command available."
        return DoctorCheck(name=name, status="pass", severity=severity, detail=detail)

    return DoctorCheck(
        name=name,
        status="warn" if severity == "optional" else "fail",
        severity=severity,
        detail=f"{help_text} (exit code {proc.returncode})",
    )


def _check_writable_workdir() -> DoctorCheck:
    cwd = Path.cwd()
    probe = cwd / ".idr_doctor_write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return DoctorCheck(
            name="writable_workdir",
            status="pass",
            severity="required",
            detail=f"Current directory is writable: {cwd}",
        )
    except Exception as exc:
        return DoctorCheck(
            name="writable_workdir",
            status="fail",
            severity="required",
            detail=f"Current directory is not writable: {cwd} ({exc})",
        )


def _check_insecure_dev_auth() -> DoctorCheck:
    value = os.getenv("IDR_ALLOW_INSECURE_DEV_AUTH", "").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return DoctorCheck(
            name="insecure_dev_auth",
            status="warn",
            severity="optional",
            detail="IDR_ALLOW_INSECURE_DEV_AUTH is enabled. Disable in production.",
        )
    return DoctorCheck(
        name="insecure_dev_auth",
        status="pass",
        severity="optional",
        detail="IDR_ALLOW_INSECURE_DEV_AUTH is disabled.",
    )


def _check_http_endpoint(
    name: str,
    url: str,
    severity: str,
    help_text: str,
    expected_status: int = 200,
    required_substring: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 5.0,
) -> DoctorCheck:
    if not url:
        return DoctorCheck(
            name=name,
            status="warn" if severity == "optional" else "fail",
            severity=severity,
            detail=help_text,
        )

    req = urllib_request.Request(url, headers=headers or {}, method="GET")
    try:
        with urllib_request.urlopen(req, timeout=timeout_seconds) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        return DoctorCheck(
            name=name,
            status="warn" if severity == "optional" else "fail",
            severity=severity,
            detail=f"{help_text} (error: {exc})",
        )

    if status != expected_status:
        return DoctorCheck(
            name=name,
            status="warn" if severity == "optional" else "fail",
            severity=severity,
            detail=f"Expected HTTP {expected_status} from {url}, got {status}.",
        )

    if required_substring and required_substring not in body:
        return DoctorCheck(
            name=name,
            status="warn" if severity == "optional" else "fail",
            severity=severity,
            detail=f"Endpoint {url} did not include required marker '{required_substring}'.",
        )

    return DoctorCheck(
        name=name,
        status="pass",
        severity=severity,
        detail=f"{url} returned HTTP {status}.",
    )


def collect_doctor_report(
    target: str = "local",
    api_url: str = "",
    metrics_url: str = "",
    whoami_url: str = "",
    token_env: str = "IDR_TOKEN",
    timeout_seconds: float = 5.0,
) -> Dict[str, object]:
    checks: List[DoctorCheck] = [
        _check_python_version(),
        _check_module("duckdb", "optional", "Install with: pip install 'sql-identity-resolution[duckdb]'"),
        _check_module("fastapi", "optional", "Install with: pip install 'sql-identity-resolution[api]'"),
        _check_module("uvicorn", "optional", "Install with: pip install 'sql-identity-resolution[api]'"),
        _check_command(
            "docker",
            ["docker", "--version"],
            "optional",
            "Docker not found. Install Docker Desktop/Engine for container deployment.",
        ),
        _check_command(
            "docker_compose",
            ["docker", "compose", "version"],
            "optional",
            "Docker Compose v2 not found. Install Docker Compose for stack deployment.",
        ),
        _check_writable_workdir(),
        _check_insecure_dev_auth(),
    ]

    if target == "cluster":
        resolved_api_url = api_url or os.getenv("IDR_DOCTOR_API_URL", "")
        resolved_metrics_url = metrics_url or os.getenv("IDR_DOCTOR_METRICS_URL", "")
        resolved_whoami_url = whoami_url or os.getenv("IDR_DOCTOR_WHOAMI_URL", "")

        checks.append(
            _check_command(
                "kubectl",
                ["kubectl", "version", "--client"],
                "required",
                "kubectl not found. Install kubectl to validate cloud/Kubernetes deployments.",
            )
        )

        checks.append(
            _check_http_endpoint(
                "cluster_api_health",
                resolved_api_url,
                "required",
                "Provide --api-url (or IDR_DOCTOR_API_URL) for cluster health validation.",
                expected_status=200,
                timeout_seconds=timeout_seconds,
            )
        )

        checks.append(
            _check_http_endpoint(
                "cluster_metrics",
                resolved_metrics_url,
                "required",
                "Provide --metrics-url (or IDR_DOCTOR_METRICS_URL) for metrics validation.",
                expected_status=200,
                required_substring="idr_http_requests_total",
                timeout_seconds=timeout_seconds,
            )
        )

        token = os.getenv(token_env, "").strip()
        if resolved_whoami_url and token:
            checks.append(
                _check_http_endpoint(
                    "cluster_whoami_auth",
                    resolved_whoami_url,
                    "required",
                    "Authenticated /api/auth/whoami check failed.",
                    expected_status=200,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout_seconds=timeout_seconds,
                )
            )
        elif resolved_whoami_url and not token:
            checks.append(
                DoctorCheck(
                    name="cluster_whoami_auth",
                    status="warn",
                    severity="optional",
                    detail=(
                        f"{resolved_whoami_url} configured but auth token missing. "
                        f"Set env var {token_env} to run authenticated whoami check."
                    ),
                )
            )
        else:
            checks.append(
                DoctorCheck(
                    name="cluster_whoami_auth",
                    status="warn",
                    severity="optional",
                    detail=(
                        "No --whoami-url provided (or IDR_DOCTOR_WHOAMI_URL). "
                        "Skipping authenticated authorization probe."
                    ),
                )
            )

    counts = {
        "pass": sum(1 for c in checks if c.status == "pass"),
        "warn": sum(1 for c in checks if c.status == "warn"),
        "fail": sum(1 for c in checks if c.status == "fail"),
    }
    return {
        "status": "ok" if counts["fail"] == 0 else "fail",
        "target": target,
        "counts": counts,
        "checks": [asdict(c) for c in checks],
    }


def _send_telemetry(url: str, report: Dict[str, object]) -> bool:
    payload = json.dumps(report).encode("utf-8")
    req = urllib_request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=5):
            return True
    except Exception:
        return False


def run_doctor(
    strict: bool = False,
    json_output: bool = False,
    telemetry_url: str = "",
    target: str = "local",
    api_url: str = "",
    metrics_url: str = "",
    whoami_url: str = "",
    token_env: str = "IDR_TOKEN",
    timeout_seconds: float = 5.0,
) -> int:
    report = collect_doctor_report(
        target=target,
        api_url=api_url,
        metrics_url=metrics_url,
        whoami_url=whoami_url,
        token_env=token_env,
        timeout_seconds=timeout_seconds,
    )
    counts = report["counts"]

    if json_output:
        print(json.dumps(report, indent=2))
    else:
        print("IDR Doctor Report")
        print("=================")
        print(f"Target: {report.get('target', 'local')}")
        for check in report["checks"]:
            print(
                f"- [{check['status'].upper():4}] {check['name']}: {check['detail']}"
            )
        print()
        print(
            "Summary: "
            f"{counts['pass']} passed, {counts['warn']} warnings, {counts['fail']} failures"
        )
    if telemetry_url:
        if _send_telemetry(telemetry_url, report):
            print(f"Telemetry submitted to {telemetry_url}")
        else:
            print(f"Telemetry submission failed: {telemetry_url}")

    if counts["fail"] > 0:
        return 1
    if strict and counts["warn"] > 0:
        return 2
    return 0
