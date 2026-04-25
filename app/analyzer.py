"""Workflow analyzer: classify uses: and produce risk findings.

Reusable workflows, permissions analysis, self-hosted runner detection,
and transitive composite analysis are not yet implemented (Phase 2/3).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from core.classify import classify_uses, extract_uses_lines

LOGGER = logging.getLogger(__name__)

Severity = Literal["critical", "high", "medium", "info", "safe"]

_SEVERITY_BY_PIN: dict[str, Severity] = {
    "sha": "safe",
    "local": "safe",
    "tag": "medium",
    "branch": "high",
    "docker": "medium",
    "unknown": "medium",
}


@dataclass(frozen=True)
class Finding:
    workflow_file: str
    action_name: str
    ref: str
    pin_type: str
    severity: Severity
    message: str
    is_high_risk: bool
    is_pinned: bool


def _severity_for(classification: dict[str, object]) -> Severity:
    if classification["is_high_risk"] and not classification["is_pinned"]:
        return "critical"
    return _SEVERITY_BY_PIN.get(str(classification["pin_type"]), "medium")


def _message_for(classification: dict[str, object]) -> str:
    name = classification["action_name"]
    ref = classification["ref"] or "default branch"
    if classification["is_high_risk"] and not classification["is_pinned"]:
        return f"{name} appears in the incidents database and is not pinned to a SHA."
    if classification["pin_type"] == "branch":
        return f"{name}@{ref} is a branch reference — content can change at any time."
    if classification["pin_type"] == "tag":
        return f"{name}@{ref} is a tag — tags can be overwritten. Pin to a SHA."
    if classification["pin_type"] == "sha":
        return f"{name}@{ref} is pinned to a SHA."
    return f"{name}@{ref} (pin_type={classification['pin_type']})"


def analyze_workflow(
    workflow_text: str,
    workflow_path: str,
    high_risk_actions: frozenset[str],
) -> list[Finding]:
    """Classify each `uses:` in a workflow file and return findings."""
    findings: list[Finding] = []
    uses_values = extract_uses_lines(workflow_text, Path(workflow_path))

    for uses_value in uses_values:
        c = classify_uses(uses_value, high_risk_actions)
        findings.append(Finding(
            workflow_file=workflow_path,
            action_name=str(c["action_name"]),
            ref=str(c["ref"]),
            pin_type=str(c["pin_type"]),
            severity=_severity_for(c),
            message=_message_for(c),
            is_high_risk=bool(c["is_high_risk"]),
            is_pinned=bool(c["is_pinned"]),
        ))
    return findings
