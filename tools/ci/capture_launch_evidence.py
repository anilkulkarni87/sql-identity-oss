#!/usr/bin/env python3
"""
Execute clean-host launch validations and collect an evidence pack.

E10-T03 acceptance mapping:
- CI links section in summary
- quickstart timing report artifact
- enterprise stack verification artifact
- MCP contract report artifact
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import socket
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class StepResult:
    name: str
    command: List[str]
    log_path: Path
    status: str
    returncode: int


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def shell_join(cmd: List[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def run_step(
    name: str,
    cmd: List[str],
    log_path: Path,
    cwd: Path,
    env: Optional[Dict[str, str]] = None,
) -> StepResult:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc_env = os.environ.copy()
    if env:
        proc_env.update(env)

    with log_path.open("w", encoding="utf-8") as logf:
        logf.write(f"$ {shell_join(cmd)}\n\n")
        completed = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=proc_env,
            stdout=logf,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )

    status = "pass" if completed.returncode == 0 else "fail"
    return StepResult(
        name=name,
        command=cmd,
        log_path=log_path,
        status=status,
        returncode=completed.returncode,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture clean-host launch evidence pack for E10-T03."
    )
    parser.add_argument(
        "--output-dir",
        default="tmp/launch_evidence",
        help="Base output directory for timestamped evidence packs.",
    )
    parser.add_argument(
        "--compose-file",
        default="docker-compose.enterprise.yml",
        help="Compose file used by enterprise verification.",
    )
    parser.add_argument(
        "--run-ha-check",
        action="store_true",
        help="Also run HA compose verification.",
    )
    parser.add_argument(
        "--skip-enterprise-check",
        action="store_true",
        help="Skip enterprise compose verification.",
    )
    parser.add_argument(
        "--quickstart-max-seconds",
        type=float,
        default=600.0,
    )
    parser.add_argument(
        "--quickstart-rows",
        type=int,
        default=10000,
    )
    parser.add_argument(
        "--skip-venv-setup",
        action="store_true",
        help="Use --python-bin directly and skip venv creation/install.",
    )
    parser.add_argument(
        "--python-bin",
        default="",
        help="Python executable to use when --skip-venv-setup is enabled.",
    )
    parser.add_argument("--ci-test-workflow-url", default="")
    parser.add_argument("--ci-release-workflow-url", default="")
    parser.add_argument("--ci-quickstart-artifact-url", default="")
    parser.add_argument("--ci-mcp-artifact-url", default="")
    parser.add_argument("--ci-enterprise-artifact-url", default="")
    return parser.parse_args()


def git_remote_https(repo_root: Path) -> Optional[str]:
    completed = subprocess.run(
        ["git", "config", "--get", "remote.origin.url"],
        cwd=str(repo_root),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    raw = completed.stdout.strip()
    if not raw:
        return None

    ssh_match = re.match(r"^git@github\.com:(.+?)(?:\.git)?$", raw)
    if ssh_match:
        return f"https://github.com/{ssh_match.group(1)}"

    https_match = re.match(r"^(https://github\.com/.+?)(?:\.git)?$", raw)
    if https_match:
        return https_match.group(1)

    return None


def read_quickstart_report(report_path: Path) -> Optional[dict]:
    if not report_path.exists():
        return None
    try:
        return json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_ci_links(args: argparse.Namespace, repo_url: Optional[str]) -> Dict[str, str]:
    links = {
        "test_workflow": args.ci_test_workflow_url.strip()
        or os.getenv("CI_TEST_WORKFLOW_URL", "").strip(),
        "release_workflow": args.ci_release_workflow_url.strip()
        or os.getenv("CI_RELEASE_WORKFLOW_URL", "").strip(),
        "quickstart_artifact": args.ci_quickstart_artifact_url.strip()
        or os.getenv("CI_QUICKSTART_ARTIFACT_URL", "").strip(),
        "mcp_contract_artifact": args.ci_mcp_artifact_url.strip()
        or os.getenv("CI_MCP_ARTIFACT_URL", "").strip(),
        "enterprise_artifact": args.ci_enterprise_artifact_url.strip()
        or os.getenv("CI_ENTERPRISE_ARTIFACT_URL", "").strip(),
    }

    if repo_url:
        links["test_workflow"] = links["test_workflow"] or f"{repo_url}/actions/workflows/test.yml"
        links["release_workflow"] = (
            links["release_workflow"] or f"{repo_url}/actions/workflows/release.yml"
        )

    return links


def write_summary(
    output_dir: Path,
    steps: List[StepResult],
    quickstart_report: Optional[dict],
    ci_links: Dict[str, str],
    metadata: Dict[str, str],
) -> None:
    status_by_name = {step.name: step.status for step in steps}
    quickstart_elapsed = (
        f"{quickstart_report.get('elapsed_seconds', 0):.2f}s"
        if isinstance(quickstart_report, dict)
        else "n/a"
    )

    summary = output_dir / "launch_evidence_summary.md"
    lines = [
        "# Launch Evidence Pack (E10-T03)",
        "",
        f"- Generated (UTC): `{metadata['generated_at_utc']}`",
        f"- Host: `{metadata['hostname']}`",
        f"- Platform: `{metadata['platform']}`",
        f"- Python: `{metadata['python_version']}`",
        f"- Git SHA: `{metadata['git_sha']}`",
        "",
        "## Validation Status",
        "",
        "| Check | Status | Evidence |",
        "|---|---|---|",
        f"| Quickstart timing gate | `{status_by_name.get('quickstart', 'not_run')}` | `quickstart_ci_report.json` |",
        f"| MCP contract suite | `{status_by_name.get('mcp_contract', 'not_run')}` | `mcp-contract.log`, `mcp-contract-junit.xml` |",
        f"| Enterprise stack verification | `{status_by_name.get('enterprise_stack', 'not_run')}` | `enterprise-stack.log` |",
        f"| Enterprise HA verification | `{status_by_name.get('enterprise_ha_stack', 'not_run')}` | `enterprise-ha-stack.log` |",
        "",
        "## Required E10-T03 Artifacts",
        "",
        f"- Quickstart timing report: `quickstart_ci_report.json` (elapsed `{quickstart_elapsed}`)",
        "- MCP contract report: `mcp-contract.log` and `mcp-contract-junit.xml`",
        "- Enterprise stack check report: `enterprise-stack.log`",
        "- CI links: see section below",
        "",
        "## CI Links",
        "",
        f"- Test workflow: {ci_links['test_workflow'] or 'TODO'}",
        f"- Release workflow: {ci_links['release_workflow'] or 'TODO'}",
        f"- Quickstart artifact run: {ci_links['quickstart_artifact'] or 'TODO'}",
        f"- MCP contract artifact run: {ci_links['mcp_contract_artifact'] or 'TODO'}",
        f"- Enterprise stack artifact run: {ci_links['enterprise_artifact'] or 'TODO'}",
        "",
        "## Step Logs",
        "",
    ]

    for step in steps:
        lines.append(
            f"- `{step.name}`: `{step.status}` (`{step.log_path.relative_to(output_dir)}`)"
        )

    summary.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_index_json(
    output_dir: Path,
    steps: List[StepResult],
    quickstart_report: Optional[dict],
    ci_links: Dict[str, str],
    metadata: Dict[str, str],
) -> None:
    payload = {
        "generated_at_utc": metadata["generated_at_utc"],
        "git_sha": metadata["git_sha"],
        "hostname": metadata["hostname"],
        "platform": metadata["platform"],
        "python_version": metadata["python_version"],
        "checks": {
            step.name: {
                "status": step.status,
                "returncode": step.returncode,
                "log": str(step.log_path.relative_to(output_dir)),
                "command": step.command,
            }
            for step in steps
        },
        "quickstart_report": quickstart_report,
        "ci_links": ci_links,
    }
    (output_dir / "launch_evidence_index.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    generated_at = datetime.now(timezone.utc).isoformat()
    evidence_dir = Path(args.output_dir).resolve() / utc_timestamp()
    evidence_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = evidence_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    print(f"[evidence] output directory: {evidence_dir}")

    python_bin = args.python_bin.strip()
    venv_dir = evidence_dir / ".venv"
    steps: List[StepResult] = []

    if args.skip_venv_setup:
        if not python_bin:
            python_bin = sys.executable
    else:
        steps.append(
            run_step(
                "venv_create",
                [sys.executable, "-m", "venv", str(venv_dir)],
                logs_dir / "venv-create.log",
                ROOT,
            )
        )
        python_bin = str(venv_dir / "bin" / "python")
        if steps[-1].status == "pass":
            steps.append(
                run_step(
                    "venv_install_ci_lock",
                    [python_bin, "-m", "pip", "install", "--upgrade", "pip"],
                    logs_dir / "venv-install-pip.log",
                    ROOT,
                )
            )
        if steps[-1].status == "pass":
            steps.append(
                run_step(
                    "venv_install_deps",
                    [python_bin, "-m", "pip", "install", "-r", "requirements/ci.lock"],
                    logs_dir / "venv-install-deps.log",
                    ROOT,
                )
            )

    prerequisites_ok = all(
        step.status == "pass"
        for step in steps
        if step.name in {"venv_create", "venv_install_ci_lock", "venv_install_deps"}
    )
    if not prerequisites_ok:
        print("[evidence] environment setup failed. See logs and rerun.")

    quickstart_report_path = evidence_dir / "quickstart_ci_report.json"

    if prerequisites_ok:
        quickstart_cmd = [
            python_bin,
            "tools/ci/validate_quickstart_path.py",
            "--max-seconds",
            str(args.quickstart_max_seconds),
            "--rows",
            str(args.quickstart_rows),
            "--output",
            str(evidence_dir / "quickstart_ci.duckdb"),
            "--report",
            str(quickstart_report_path),
        ]
        steps.append(
            run_step(
                "quickstart",
                quickstart_cmd,
                logs_dir / "quickstart.log",
                ROOT,
            )
        )

        mcp_cmd = [
            python_bin,
            "-m",
            "pytest",
            "tests/test_mcp_contract.py",
            "tests/test_mcp_secrets.py",
            "tests/test_mcp_errors.py",
            "-q",
            "--junitxml",
            str(evidence_dir / "mcp-contract-junit.xml"),
        ]
        steps.append(
            run_step(
                "mcp_contract",
                mcp_cmd,
                evidence_dir / "mcp-contract.log",
                ROOT,
            )
        )

        if not args.skip_enterprise_check:
            steps.append(
                run_step(
                    "enterprise_stack",
                    ["bash", "tools/ci/verify_enterprise_stack.sh", args.compose_file],
                    evidence_dir / "enterprise-stack.log",
                    ROOT,
                )
            )

            if args.run_ha_check:
                steps.append(
                    run_step(
                        "enterprise_ha_stack",
                        [
                            "bash",
                            "-lc",
                            (
                                "EXPECT_HEALTHY_SERVICES=api-a,api-b,api,redis,keycloak "
                                "bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.ha.yml"
                            ),
                        ],
                        evidence_dir / "enterprise-ha-stack.log",
                        ROOT,
                    )
                )

    quickstart_report = read_quickstart_report(quickstart_report_path)
    repo_url = git_remote_https(ROOT)

    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )
    git_sha = completed.stdout.strip() if completed.returncode == 0 else "unknown"

    metadata = {
        "generated_at_utc": generated_at,
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "git_sha": git_sha,
    }
    ci_links = build_ci_links(args, repo_url)

    write_summary(
        evidence_dir,
        steps,
        quickstart_report,
        ci_links,
        metadata,
    )
    write_index_json(
        evidence_dir,
        steps,
        quickstart_report,
        ci_links,
        metadata,
    )

    required_checks = {"quickstart", "mcp_contract"}
    if not args.skip_enterprise_check:
        required_checks.add("enterprise_stack")
    if args.run_ha_check:
        required_checks.add("enterprise_ha_stack")

    failed_required = [
        step for step in steps if step.name in required_checks and step.status != "pass"
    ]
    if failed_required:
        print("[evidence] required checks failed:")
        for step in failed_required:
            print(f"  - {step.name}: {step.log_path}")
        print(f"[evidence] summary: {evidence_dir / 'launch_evidence_summary.md'}")
        return 1

    print("[evidence] pack generated successfully.")
    print(f"[evidence] summary: {evidence_dir / 'launch_evidence_summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
