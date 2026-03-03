#!/usr/bin/env python3
"""
Backup/restore drill harness for enterprise operability validation (E05-T02).

The drill:
1. Snapshot critical state files into a timestamped backup folder.
2. Optionally simulate an incident by corrupting source files.
3. Restore from backup copies.
4. Verify integrity and measure RTO/RPO style signals.
5. Emit JSON evidence artifact and exit non-zero on threshold breach.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class ManagedFile:
    label: str
    path: Path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest(files: List[ManagedFile]) -> Dict[str, Dict[str, object]]:
    payload: Dict[str, Dict[str, object]] = {}
    for entry in files:
        st = entry.path.stat()
        payload[entry.label] = {
            "path": str(entry.path),
            "size_bytes": int(st.st_size),
            "sha256": _sha256(entry.path),
            "mtime_ns": int(st.st_mtime_ns),
        }
    return payload


def _parse_file_arg(raw: str) -> ManagedFile:
    if "=" not in raw:
        raise ValueError(f"Invalid --file value '{raw}'. Expected label=/absolute/or/relative/path")
    label, path_str = raw.split("=", 1)
    label = label.strip()
    path = Path(path_str.strip())
    if not label:
        raise ValueError(f"Invalid --file value '{raw}': missing label")
    if not path_str.strip():
        raise ValueError(f"Invalid --file value '{raw}': missing path")
    return ManagedFile(label=label, path=path)


def _copy_for_backup(
    files: List[ManagedFile], backup_dir: Path
) -> Tuple[Dict[str, Path], Dict[str, float], float]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    copies: Dict[str, Path] = {}
    per_file_seconds: Dict[str, float] = {}

    started = time.perf_counter()
    for entry in files:
        dst = backup_dir / f"{entry.label}.bak"
        step_started = time.perf_counter()
        shutil.copy2(entry.path, dst)
        per_file_seconds[entry.label] = time.perf_counter() - step_started
        copies[entry.label] = dst
    total_seconds = time.perf_counter() - started
    return copies, per_file_seconds, total_seconds


def _simulate_incident(files: List[ManagedFile]) -> Dict[str, str]:
    actions: Dict[str, str] = {}
    for entry in files:
        size = max(1, int(entry.path.stat().st_size))
        incident_bytes = os.urandom(min(size, 4096))
        with entry.path.open("r+b") as handle:
            handle.seek(0)
            handle.write(incident_bytes)
            handle.truncate(max(1, len(incident_bytes)))
        actions[entry.label] = "corrupted"
    return actions


def _restore_from_backup(
    files: List[ManagedFile], copies: Dict[str, Path]
) -> Tuple[Dict[str, float], float]:
    per_file_seconds: Dict[str, float] = {}
    started = time.perf_counter()
    for entry in files:
        source = copies[entry.label]
        entry.path.parent.mkdir(parents=True, exist_ok=True)
        step_started = time.perf_counter()
        shutil.copy2(source, entry.path)
        per_file_seconds[entry.label] = time.perf_counter() - step_started
    total_seconds = time.perf_counter() - started
    return per_file_seconds, total_seconds


def _rpo_bytes(expected: Dict[str, Dict[str, object]], actual: Dict[str, Dict[str, object]]) -> int:
    total = 0
    for label, exp in expected.items():
        act = actual.get(label)
        if not act:
            total += int(exp["size_bytes"])
            continue
        if exp["sha256"] != act["sha256"]:
            total += max(int(exp["size_bytes"]), int(act["size_bytes"]))
    return total


def _verify_same(expected: Dict[str, Dict[str, object]], actual: Dict[str, Dict[str, object]]) -> bool:
    for label, exp in expected.items():
        act = actual.get(label)
        if not act:
            return False
        if exp["sha256"] != act["sha256"] or int(exp["size_bytes"]) != int(act["size_bytes"]):
            return False
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run backup/restore drill with evidence output")
    parser.add_argument(
        "--file",
        action="append",
        dest="files",
        required=True,
        help="Managed file in label=path format. Repeat for multiple files.",
    )
    parser.add_argument(
        "--backup-dir",
        required=True,
        help="Directory where backup artifacts are written.",
    )
    parser.add_argument(
        "--evidence-file",
        default=None,
        help="Optional explicit path for evidence JSON. Defaults to <backup-dir>/drill_report.json.",
    )
    parser.add_argument(
        "--simulate-incident",
        action="store_true",
        help="Corrupt source files between backup and restore to emulate an incident.",
    )
    parser.add_argument(
        "--max-rto-seconds",
        type=float,
        default=300.0,
        help="RTO threshold; drill fails if restore duration exceeds this.",
    )
    parser.add_argument(
        "--max-rpo-bytes",
        type=int,
        default=0,
        help="RPO threshold in bytes; drill fails if estimated data loss exceeds this.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    files = [_parse_file_arg(raw) for raw in (args.files or [])]
    backup_root = Path(args.backup_dir)
    evidence_path = (
        Path(args.evidence_file)
        if args.evidence_file
        else backup_root / "drill_report.json"
    )

    for entry in files:
        if not entry.path.exists():
            print(f"Missing managed file: {entry.label} -> {entry.path}", file=sys.stderr)
            return 2
        if not entry.path.is_file():
            print(f"Managed path is not a file: {entry.label} -> {entry.path}", file=sys.stderr)
            return 2

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = backup_root / f"snapshot_{stamp}"

    started_at = _utc_now_iso()
    pre_manifest = _manifest(files)
    copies, backup_per_file_seconds, backup_duration_seconds = _copy_for_backup(files, backup_dir)

    incident_actions: Dict[str, str] = {}
    if args.simulate_incident:
        incident_actions = _simulate_incident(files)

    restore_per_file_seconds, restore_duration_seconds = _restore_from_backup(files, copies)
    post_manifest = _manifest(files)

    restore_verified = _verify_same(pre_manifest, post_manifest)
    estimated_rpo_bytes = _rpo_bytes(pre_manifest, post_manifest)

    threshold_failures: List[str] = []
    if restore_duration_seconds > float(args.max_rto_seconds):
        threshold_failures.append(
            f"RTO exceeded: restore_duration_seconds={restore_duration_seconds:.6f} > max_rto_seconds={args.max_rto_seconds}"
        )
    if estimated_rpo_bytes > int(args.max_rpo_bytes):
        threshold_failures.append(
            f"RPO exceeded: estimated_rpo_bytes={estimated_rpo_bytes} > max_rpo_bytes={args.max_rpo_bytes}"
        )
    if not restore_verified:
        threshold_failures.append("Integrity verification failed: restored files differ from backup snapshot")

    completed_at = _utc_now_iso()
    status = "passed" if not threshold_failures else "failed"

    report = {
        "artifact_version": "1.0",
        "started_at": started_at,
        "completed_at": completed_at,
        "status": status,
        "managed_files": [f"{entry.label}={entry.path}" for entry in files],
        "backup": {
            "backup_dir": str(backup_dir),
            "duration_seconds": backup_duration_seconds,
            "per_file_seconds": backup_per_file_seconds,
        },
        "incident": {
            "simulated": bool(args.simulate_incident),
            "actions": incident_actions,
        },
        "restore": {
            "duration_seconds": restore_duration_seconds,
            "per_file_seconds": restore_per_file_seconds,
            "verified": restore_verified,
        },
        "objectives": {
            "max_rto_seconds": float(args.max_rto_seconds),
            "max_rpo_bytes": int(args.max_rpo_bytes),
            "observed_rto_seconds": restore_duration_seconds,
            "observed_rpo_bytes": estimated_rpo_bytes,
        },
        "threshold_failures": threshold_failures,
        "manifests": {
            "pre_backup": pre_manifest,
            "post_restore": post_manifest,
        },
    }

    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Backup/restore drill status: {status}")
    print(f"Evidence written to: {evidence_path}")
    if threshold_failures:
        for failure in threshold_failures:
            print(f"- {failure}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
