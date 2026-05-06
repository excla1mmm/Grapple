"""Workflow analyzer: classify uses, permissions, runners.

Covers three classes of risk:
- step-level `uses:` (actions inside `steps:`)
- job-level `uses:` (reusable workflows — entire jobs run under remote control)
- `permissions:` blocks (broad scopes like `id-token: write` or `write-all`)
- `runs-on: self-hosted` — modifier that bumps severity of other findings in the job

Transitive composite analysis is deferred to Phase 3 per CLAUDE.md.
"""

from __future__ import annotations

import logging
import re
from collections import deque
from dataclasses import dataclass
from typing import Any, Literal

from core.classify import classify_uses

try:
    import yaml
except ImportError:
    yaml = None

LOGGER = logging.getLogger(__name__)

Severity = Literal["critical", "high", "medium", "info", "safe"]
FindingKind = Literal["action", "reusable_workflow", "permission", "runner"]

_BUMP_NEXT: dict[Severity, Severity] = {"medium": "high", "high": "critical"}

_SEVERITY_BY_PIN: dict[str, Severity] = {
    "sha": "safe",
    "local": "safe",
    "tag": "medium",
    "branch": "high",
    "docker": "medium",
    "unknown": "medium",
}

# Per CLAUDE.md: which write-permissions are dangerous and why.
_PERMISSION_RISK: dict[str, tuple[Severity, str]] = {
    "id-token": ("critical", "issues OIDC tokens — lets workflows authenticate to AWS/GCP/Azure without a password"),
    "contents": ("high", "allows pushing code to the repository"),
    "packages": ("high", "allows publishing packages"),
    "deployments": ("high", "allows creating and updating deployments"),
    "actions": ("high", "allows triggering and modifying workflows"),
    "checks": ("medium", "allows creating and updating check runs"),
    "issues": ("medium", "allows creating, editing, and closing issues"),
    "pull-requests": ("medium", "allows commenting on and merging pull requests"),
    "statuses": ("medium", "allows creating commit statuses"),
}

_USES_LINE_PATTERN = re.compile(
    r"^\s*-?\s*uses:\s*[\"']?(?P<value>[^\"'#\r\n]+?)[\"']?\s*(?:#.*)?$",
    re.IGNORECASE,
)
_PERMISSIONS_HEADER_PATTERN = re.compile(r"^\s*permissions\s*:", re.IGNORECASE)
_PERMISSION_SCOPE_PATTERN = re.compile(
    r"^\s*(?P<scope>[a-zA-Z0-9_-]+)\s*:\s*(?P<value>[^#\r\n]+)"
)
_SELF_HOSTED_LABEL_PATTERN = re.compile(
    r"(^|[\[\s:,])self-hosted($|[\],\s#])",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Finding:
    kind: FindingKind
    workflow_file: str
    action_name: str
    ref: str
    pin_type: str
    severity: Severity
    message: str
    is_high_risk: bool
    is_pinned: bool
    line_number: int | None = None
    uses_raw: str = ""


class WorkflowLineIndex:
    """Best-effort line lookup for SARIF regions."""

    def __init__(self, workflow_text: str):
        self._uses_lines: dict[str, deque[int]] = {}
        self._permission_headers: deque[int] = deque()
        self._permission_scope_lines: dict[str, deque[int]] = {}
        self._self_hosted_lines: deque[int] = deque()

        for line_number, line in enumerate(workflow_text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            uses_match = _USES_LINE_PATTERN.match(line)
            if uses_match:
                uses_value = uses_match.group("value").strip()
                self._uses_lines.setdefault(uses_value, deque()).append(line_number)

            if _PERMISSIONS_HEADER_PATTERN.match(line):
                self._permission_headers.append(line_number)

            scope_match = _PERMISSION_SCOPE_PATTERN.match(line)
            if (
                scope_match
                and _normalize_yaml_scalar(scope_match.group("value")) == "write"
            ):
                scope = scope_match.group("scope").lower()
                self._permission_scope_lines.setdefault(scope, deque()).append(line_number)

            if (
                _SELF_HOSTED_LABEL_PATTERN.search(stripped)
                and "uses:" not in stripped.lower()
            ):
                self._self_hosted_lines.append(line_number)

    def pop_uses(self, uses_value: str) -> int | None:
        lines = self._uses_lines.get(uses_value)
        if not lines:
            return None
        return lines.popleft()

    def pop_permission_header(self) -> int | None:
        if not self._permission_headers:
            return None
        return self._permission_headers.popleft()

    def pop_permission_scope(self, scope: str) -> int | None:
        lines = self._permission_scope_lines.get(scope)
        if not lines:
            return None
        return lines.popleft()

    def pop_self_hosted(self) -> int | None:
        if not self._self_hosted_lines:
            return None
        return self._self_hosted_lines.popleft()


def _normalize_yaml_scalar(value: object) -> str:
    return str(value).strip().strip("'\"").lower()


@dataclass(frozen=True)
class JobInfo:
    job_id: str
    runs_on: list[str]
    permissions: Any  # dict | str | None
    step_uses: list[str]
    reusable_uses: str | None

    @property
    def is_self_hosted(self) -> bool:
        return any("self-hosted" in r.lower() for r in self.runs_on)


@dataclass(frozen=True)
class WorkflowStructure:
    permissions: Any  # workflow-level
    jobs: list[JobInfo]


def _bump(severity: Severity) -> Severity:
    return _BUMP_NEXT.get(severity, severity)


def parse_workflow(workflow_text: str, workflow_path: str) -> WorkflowStructure:
    """Parse a workflow YAML into a structural view.

    Returns an empty structure on parse failure — the caller can still report
    a YAML-parse warning via separate logging.
    """
    if yaml is None:
        LOGGER.warning("PyYAML not installed; skipping structural analysis of %s", workflow_path)
        return WorkflowStructure(permissions=None, jobs=[])

    try:
        doc = yaml.safe_load(workflow_text)
    except yaml.YAMLError as error:
        LOGGER.warning("YAML parse failed for %s: %s", workflow_path, error)
        return WorkflowStructure(permissions=None, jobs=[])

    if not isinstance(doc, dict):
        return WorkflowStructure(permissions=None, jobs=[])

    jobs: list[JobInfo] = []
    raw_jobs = doc.get("jobs")
    if isinstance(raw_jobs, dict):
        for job_id, job_def in raw_jobs.items():
            if not isinstance(job_def, dict):
                continue
            jobs.append(_parse_job(str(job_id), job_def))

    return WorkflowStructure(permissions=doc.get("permissions"), jobs=jobs)


def _normalize_runs_on(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(x) for x in value]
    if isinstance(value, dict):
        runs_on: list[str] = []
        for key in ("group", "labels"):
            runs_on.extend(_normalize_runs_on(value.get(key)))
        return runs_on
    return []


def _parse_job(job_id: str, job_def: dict) -> JobInfo:
    runs_on_raw = job_def.get("runs-on", "")
    runs_on = _normalize_runs_on(runs_on_raw)

    reusable_uses = None
    job_uses = job_def.get("uses")
    if isinstance(job_uses, str):
        reusable_uses = job_uses.strip()

    step_uses: list[str] = []
    raw_steps = job_def.get("steps")
    if isinstance(raw_steps, list):
        for step in raw_steps:
            if isinstance(step, dict) and isinstance(step.get("uses"), str):
                step_uses.append(step["uses"].strip())

    return JobInfo(
        job_id=job_id,
        runs_on=runs_on,
        permissions=job_def.get("permissions"),
        step_uses=step_uses,
        reusable_uses=reusable_uses,
    )


def _severity_for_action(c: dict[str, object]) -> Severity:
    if c["is_high_risk"] and not c["is_pinned"]:
        return "critical"
    return _SEVERITY_BY_PIN.get(str(c["pin_type"]), "medium")


def _message_for_action(c: dict[str, object]) -> str:
    name, ref = c["action_name"], c["ref"] or "default branch"
    if c["is_high_risk"] and not c["is_pinned"]:
        return f"{name} appears in the incidents database and is not pinned to a SHA."
    if c["pin_type"] == "branch":
        return f"{name}@{ref} is a branch reference — content can change at any time."
    if c["pin_type"] == "tag":
        return f"{name}@{ref} is a tag — tags can be overwritten. Pin to a SHA."
    if c["pin_type"] == "sha":
        return f"{name}@{ref} is pinned to a SHA."
    return f"{name}@{ref} (pin_type={c['pin_type']})"


def _severity_for_reusable(c: dict[str, object]) -> Severity:
    """Reusable workflows are harsher: a tag-pinned reusable = HIGH (vs MEDIUM for step)."""
    if c["is_high_risk"] and not c["is_pinned"]:
        return "critical"
    if c["pin_type"] == "branch":
        return "critical"
    if c["pin_type"] == "tag":
        return "high"
    if c["pin_type"] == "sha":
        return "safe"
    return "medium"


def _message_for_reusable(c: dict[str, object]) -> str:
    name, ref = c["action_name"], c["ref"] or "default branch"
    suffix = " The entire job runs under the workflow's control with all inherited secrets."
    if c["is_high_risk"] and not c["is_pinned"]:
        return f"Reusable workflow {name} is in the incidents database and unpinned.{suffix}"
    if c["pin_type"] == "branch":
        return f"Reusable workflow {name}@{ref} is pinned to a branch.{suffix}"
    if c["pin_type"] == "tag":
        return f"Reusable workflow {name}@{ref} is pinned to a tag — tags can be overwritten. Pin to a SHA."
    if c["pin_type"] == "sha":
        return f"Reusable workflow {name}@{ref} is pinned to a SHA."
    return f"Reusable workflow {name}@{ref}"


def _analyze_permissions(
    perms: Any,
    workflow_path: str,
    job_id: str | None,
    line_index: WorkflowLineIndex,
) -> list[Finding]:
    location = f"job '{job_id}'" if job_id else "workflow level"
    findings: list[Finding] = []

    if perms is None:
        if job_id is None:
            findings.append(Finding(
                kind="permission",
                workflow_file=workflow_path,
                action_name="permissions",
                ref="",
                pin_type="permission",
                severity="info",
                message=(
                    "No top-level `permissions:` block — defaults apply, "
                    "which may grant more than necessary. Set `permissions: read-all` "
                    "or an explicit minimal map."
                ),
                is_high_risk=False,
                is_pinned=False,
            ))
        return findings

    permissions_line = line_index.pop_permission_header()

    if isinstance(perms, str):
        if perms == "write-all":
            findings.append(Finding(
                kind="permission",
                workflow_file=workflow_path,
                action_name="permissions",
                ref="write-all",
                pin_type="permission",
                severity="critical",
                message=(
                    f"`permissions: write-all` at {location} grants every available scope. "
                    "Restrict to only the scopes the workflow actually needs."
                ),
                is_high_risk=False,
                is_pinned=False,
                line_number=permissions_line,
            ))
        # `read-all` and unknown shorthand strings: no finding.
        return findings

    if isinstance(perms, dict):
        for scope, value in perms.items():
            scope_s = str(scope).lower()
            value_s = _normalize_yaml_scalar(value)
            if value_s != "write":
                continue
            severity, why = _PERMISSION_RISK.get(scope_s, ("medium", "grants write access"))
            line_number = line_index.pop_permission_scope(scope_s) or permissions_line
            findings.append(Finding(
                kind="permission",
                workflow_file=workflow_path,
                action_name=f"permissions.{scope_s}",
                ref="write",
                pin_type="permission",
                severity=severity,
                message=f"`{scope_s}: write` at {location} — {why}.",
                is_high_risk=False,
                is_pinned=False,
                line_number=line_number,
            ))
    return findings


def _runner_finding(
    workflow_path: str,
    job: JobInfo,
    line_number: int | None,
) -> Finding:
    return Finding(
        kind="runner",
        workflow_file=workflow_path,
        action_name=f"job '{job.job_id}'",
        ref="self-hosted",
        pin_type="runner",
        severity="info",
        message=(
            f"Job '{job.job_id}' runs on a self-hosted runner. Severity of unpinned "
            "actions in this job is raised one level — self-hosted runners have access "
            "to internal networks, filesystems, and credentials."
        ),
        is_high_risk=False,
        is_pinned=False,
        line_number=line_number,
    )


def analyze_workflow(
    workflow_text: str,
    workflow_path: str,
    high_risk_actions: frozenset[str],
) -> list[Finding]:
    """Return all findings from a single workflow file."""
    structure = parse_workflow(workflow_text, workflow_path)
    line_index = WorkflowLineIndex(workflow_text)
    findings: list[Finding] = list(_analyze_permissions(
        structure.permissions, workflow_path, job_id=None, line_index=line_index
    ))

    for job in structure.jobs:
        findings.extend(_analyze_permissions(
            job.permissions, workflow_path, job_id=job.job_id, line_index=line_index
        ))

        if job.is_self_hosted:
            findings.append(_runner_finding(
                workflow_path,
                job,
                line_number=line_index.pop_self_hosted(),
            ))

        if job.reusable_uses is not None:
            c = classify_uses(job.reusable_uses, high_risk_actions)
            severity = _severity_for_reusable(c)
            if job.is_self_hosted:
                severity = _bump(severity)
            findings.append(Finding(
                kind="reusable_workflow",
                workflow_file=workflow_path,
                action_name=str(c["action_name"]),
                ref=str(c["ref"]),
                pin_type=str(c["pin_type"]),
                severity=severity,
                message=_message_for_reusable(c),
                is_high_risk=bool(c["is_high_risk"]),
                is_pinned=bool(c["is_pinned"]),
                line_number=line_index.pop_uses(job.reusable_uses),
                uses_raw=str(c["uses_raw"]),
            ))

        for uses in job.step_uses:
            c = classify_uses(uses, high_risk_actions)
            severity = _severity_for_action(c)
            if job.is_self_hosted:
                severity = _bump(severity)
            findings.append(Finding(
                kind="action",
                workflow_file=workflow_path,
                action_name=str(c["action_name"]),
                ref=str(c["ref"]),
                pin_type=str(c["pin_type"]),
                severity=severity,
                message=_message_for_action(c),
                is_high_risk=bool(c["is_high_risk"]),
                is_pinned=bool(c["is_pinned"]),
                line_number=line_index.pop_uses(uses),
                uses_raw=str(c["uses_raw"]),
            ))

    return findings
