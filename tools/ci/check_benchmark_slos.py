#!/usr/bin/env python3
"""
Enforce benchmark SLO thresholds against benchmark harness artifacts.
"""

from __future__ import annotations

import argparse
import json
import operator
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

OPS = {
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    ">": operator.gt,
}


def _get_path(payload: Dict[str, Any], dotted_path: str) -> Any:
    current: Any = payload
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(f"Path '{dotted_path}' missing at '{part}'")
        current = current[part]
    return current


def _profile_map(artifact: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    profiles = artifact.get("profiles")
    if not isinstance(profiles, list):
        raise ValueError("Artifact missing 'profiles' list")
    mapping: Dict[str, Dict[str, Any]] = {}
    for profile in profiles:
        profile_id = str(profile.get("profile_id", "")).strip()
        if profile_id:
            mapping[profile_id] = profile
    return mapping


def _evaluate_rule(
    rule: Dict[str, Any],
    artifact: Dict[str, Any],
    profiles: Dict[str, Dict[str, Any]],
) -> Tuple[bool, str]:
    rule_id = str(rule.get("id", "unnamed_rule"))
    rule_type = str(rule.get("type", "")).strip()
    path = str(rule.get("path", "")).strip()
    op = str(rule.get("op", "")).strip()
    target = rule.get("value")

    if op not in OPS:
        raise ValueError(f"Rule '{rule_id}' has unsupported operator '{op}'")
    if not path:
        raise ValueError(f"Rule '{rule_id}' missing path")

    if rule_type == "summary_max" or rule_type == "summary_min" or rule_type == "summary_metric":
        summary = artifact.get("summary")
        if not isinstance(summary, dict):
            raise ValueError("Artifact missing 'summary' object")
        actual = _get_path(summary, path)
    elif rule_type == "profile_metric":
        profile_id = str(rule.get("profile_id", "")).strip()
        if not profile_id:
            raise ValueError(f"Rule '{rule_id}' missing profile_id")
        profile = profiles.get(profile_id)
        if profile is None:
            raise KeyError(f"Rule '{rule_id}' references unknown profile '{profile_id}'")
        actual = _get_path(profile, path)
    else:
        raise ValueError(f"Rule '{rule_id}' has unsupported type '{rule_type}'")

    comparison = OPS[op]
    passed = comparison(actual, target)
    message = f"{rule_id}: actual={actual} {op} target={target}"
    return passed, message


def evaluate_slos(artifact: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(thresholds.get("rules"), list):
        raise ValueError("Threshold file must contain 'rules' list")

    profiles = _profile_map(artifact)
    checks: List[Dict[str, Any]] = []
    failed = 0

    for rule in thresholds["rules"]:
        rule_id = str(rule.get("id", "unnamed_rule"))
        try:
            passed, message = _evaluate_rule(rule, artifact=artifact, profiles=profiles)
        except Exception as exc:
            passed = False
            message = f"{rule_id}: evaluation error: {exc}"
        checks.append({"id": rule_id, "passed": passed, "message": message})
        if not passed:
            failed += 1

    return {
        "artifact_version": thresholds.get("artifact_version", "1.0"),
        "total_rules": len(checks),
        "failed_rules": failed,
        "passed_rules": len(checks) - failed,
        "checks": checks,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate benchmark SLO thresholds")
    parser.add_argument(
        "--benchmark-json",
        required=True,
        help="Path to benchmark aggregate artifact JSON",
    )
    parser.add_argument(
        "--thresholds",
        required=True,
        help="Path to benchmark SLO thresholds JSON",
    )
    parser.add_argument(
        "--report",
        default=None,
        help="Optional output path for SLO evaluation report JSON",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact_path = Path(args.benchmark_json)
    thresholds_path = Path(args.thresholds)

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    thresholds = json.loads(thresholds_path.read_text(encoding="utf-8"))

    report = evaluate_slos(artifact=artifact, thresholds=thresholds)

    print(
        f"SLO checks: passed={report['passed_rules']} failed={report['failed_rules']} total={report['total_rules']}"
    )
    for check in report["checks"]:
        marker = "PASS" if check["passed"] else "FAIL"
        print(f"[{marker}] {check['message']}")

    if args.report:
        Path(args.report).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return 1 if report["failed_rules"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
