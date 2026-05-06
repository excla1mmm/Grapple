"""Format Grapple findings into GitHub Check Run output."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from app.analyzer import Finding
from app.fix_plan import SuggestedFix

_SEVERITY_ORDER = ("critical", "high", "medium", "info", "safe")
_SEVERITY_LABEL = {
    "critical": "Critical",
    "high": "High",
    "medium": "Medium",
    "info": "Informational",
    "safe": "Verified",
}
_BLOCKING = {"critical"}
_REMEDIATION = {"critical", "high", "medium"}
_ANNOTATION_LEVEL = {
    "critical": "failure",
    "high": "failure",
    "medium": "warning",
    "info": "notice",
}
_MAX_ANNOTATIONS = 50


def conclusion_for(findings: Iterable[Finding]) -> str:
    """Map findings to a GitHub Check Run conclusion.

    Conservative: only critical findings cause `failure`. High/medium remain
    `neutral` so the App does not block PRs out of the box.
    """
    severities = {f.severity for f in findings}
    if severities & _BLOCKING:
        return "failure"
    if severities & {"high", "medium"}:
        return "neutral"
    return "success"


def title_for(findings: list[Finding]) -> str:
    if not findings:
        return "Grapple Security: no GitHub Actions found"
    counts = Counter(f.severity for f in findings)
    critical = counts.get("critical", 0)
    if critical:
        return f"Grapple Security: {critical} critical {_plural('finding', critical)}"
    issues = counts.get("high", 0) + counts.get("medium", 0)
    if issues:
        return f"Grapple Security: {issues} {_plural('finding', issues)} need review"
    return "Grapple Security: no blocking findings"


def _plural(word: str, count: int) -> str:
    if count == 1:
        return word
    return f"{word}s"


def _location(f: Finding) -> str:
    if f.line_number is None:
        return f"`{f.workflow_file}`"
    return f"`{f.workflow_file}:{f.line_number}`"


def _dependency(f: Finding) -> str:
    if f.kind in ("action", "reusable_workflow"):
        ref = f.ref or "default"
        return f"`{f.action_name}@{ref}`"
    if f.kind == "permission":
        if f.action_name.startswith("permissions."):
            scope = f.action_name.removeprefix("permissions.")
            return f"`permissions.{scope}: {f.ref}`"
        return f"`permissions: {f.ref or 'implicit default'}`"
    return f"`{f.action_name}`"


def _impact(f: Finding) -> str:
    if f.kind == "permission":
        return f.message
    if f.kind == "runner":
        return (
            "The job runs on infrastructure outside GitHub-hosted isolation. "
            "Compromised workflow code may have access to internal networks, "
            "local filesystems, or long-lived credentials."
        )
    if f.kind == "reusable_workflow":
        if f.pin_type == "branch":
            return (
                "A branch-pinned reusable workflow can change without review in this "
                "repository. The entire remote job executes with its configured token "
                "and any inherited secrets."
            )
        if f.pin_type == "tag":
            return (
                "A tag-pinned reusable workflow depends on a mutable Git ref. If the "
                "tag is moved, the remote job can execute different code."
            )
        if f.is_high_risk and not f.is_pinned:
            return (
                "This reusable workflow appears in the incidents database and is not "
                "pinned to an immutable commit."
            )
        return f.message
    if f.is_high_risk and not f.is_pinned:
        return (
            "This action appears in the incidents database and is not SHA-pinned. "
            "A mutable ref can execute attacker-controlled CI code with access to "
            "workflow tokens and available secrets."
        )
    if f.pin_type == "branch":
        return (
            "The action is pinned to a branch. Branch content can change at any time, "
            "including outside this repository's review process."
        )
    if f.pin_type == "tag":
        return (
            "The action is pinned to a tag. Tags are mutable Git refs and can be "
            "moved or overwritten."
        )
    if f.pin_type == "sha":
        return "The action is pinned to an immutable full commit SHA."
    return f.message


def _recommendation(f: Finding) -> str:
    if f.kind in ("action", "reusable_workflow"):
        if f.pin_type == "sha":
            return "No action required."
        return "Pin this dependency to a full 40-character commit SHA."
    if f.kind == "permission":
        if f.ref == "write-all":
            return "Replace `write-all` with the smallest explicit permission map required."
        if f.action_name.startswith("permissions."):
            scope = f.action_name.removeprefix("permissions.")
            return f"Downgrade `{scope}` to `read` or `none` unless write access is required."
        return "Add a top-level `permissions:` block using `read-all` or a minimal explicit map."
    return "Review whether this job must run on a self-hosted runner."


def _risk_context(f: Finding) -> str | None:
    if f.kind != "action":
        return None
    if f.pin_type == "tag" and f.severity == "high":
        return "Severity was raised because this action runs on a self-hosted runner."
    if f.pin_type == "branch" and f.severity == "critical" and not f.is_high_risk:
        return "Severity was raised because this action runs on a self-hosted runner."
    return None


def _render_finding(f: Finding, index: int) -> list[str]:
    finding_id = f"GHA-{index:03d}"
    lines = [
        f"### {finding_id}: {_dependency(f)}",
        "",
        f"- **Location:** {_location(f)}",
        f"- **Type:** `{f.kind}`",
        f"- **Severity:** `{f.severity}`",
    ]
    if f.kind in ("action", "reusable_workflow"):
        lines.append(f"- **Pin type:** `{f.pin_type}`")
    lines.extend([
        f"- **Impact:** {_impact(f)}",
        f"- **Recommendation:** {_recommendation(f)}",
    ])
    context = _risk_context(f)
    if context:
        lines.append(f"- **Risk context:** {context}")
    return lines


def _annotation_title(f: Finding, index: int) -> str:
    return f"GHA-{index:03d}: {_dependency(f).strip('`')}"


def _annotation_message(f: Finding) -> str:
    return f"{_impact(f)} Recommendation: {_recommendation(f)}"


def _summary_counts(findings: list[Finding]) -> str:
    counts = Counter(f.severity for f in findings)
    parts = [
        f"{counts[severity]} {severity}"
        for severity in _SEVERITY_ORDER
        if counts.get(severity, 0)
    ]
    return ", ".join(parts) if parts else "0 findings"


def _ordered_findings(by_severity: dict[str, list[Finding]]) -> list[Finding]:
    ordered: list[Finding] = []
    for severity in _SEVERITY_ORDER:
        ordered.extend(by_severity.get(severity, []))
    return ordered


def _ordered_findings_from_list(findings: list[Finding]) -> list[Finding]:
    by_severity: dict[str, list[Finding]] = {}
    for finding in findings:
        by_severity.setdefault(finding.severity, []).append(finding)
    return _ordered_findings(by_severity)


def annotations_for(findings: list[Finding]) -> list[dict]:
    """Build GitHub Check Run annotations for line-addressable findings."""
    annotations: list[dict] = []
    for index, finding in enumerate(_ordered_findings_from_list(findings), start=1):
        level = _ANNOTATION_LEVEL.get(finding.severity)
        if level is None or finding.line_number is None:
            continue
        annotations.append({
            "path": finding.workflow_file,
            "start_line": finding.line_number,
            "end_line": finding.line_number,
            "annotation_level": level,
            "title": _annotation_title(finding, index),
            "message": _annotation_message(finding),
        })
        if len(annotations) >= _MAX_ANNOTATIONS:
            break
    return annotations


def _remediation_rows(ordered_findings: list[Finding]) -> list[str]:
    rows = [
        "| Finding | Location | Recommendation |",
        "|---|---|---|",
    ]
    for index, finding in enumerate(ordered_findings, start=1):
        if finding.severity not in _REMEDIATION:
            continue
        rows.append(
            f"| GHA-{index:03d} | {_location(finding)} | {_recommendation(finding)} |"
        )
    return rows


def _suggested_fix_rows(suggested_fixes: list[SuggestedFix]) -> list[str]:
    rows = [
        "| Location | Current | Suggested |",
        "|---|---|---|",
    ]
    for fix in suggested_fixes:
        line_suffix = f":{fix.line_number}" if fix.line_number is not None else ""
        rows.append(
            f"| `{fix.workflow_file}{line_suffix}` | `{fix.current}` | `{fix.suggested}` |"
        )
    return rows


def summary_for(
    findings: list[Finding],
    suggested_fixes: list[SuggestedFix] | None = None,
) -> str:
    if not findings:
        return (
            "# Grapple Security Report\n\n"
            "**Conclusion:** Success\n"
            "**Risk level:** None\n"
            "**Scanned workflows:** 0\n"
            "**Findings:** 0\n\n"
            "No `uses:` references were found in `.github/workflows/`."
        )

    by_severity: dict[str, list[Finding]] = {}
    for finding in findings:
        by_severity.setdefault(finding.severity, []).append(finding)
    ordered_findings = _ordered_findings(by_severity)
    conclusion = conclusion_for(findings).title()
    risk_level = next(
        (severity for severity in _SEVERITY_ORDER if by_severity.get(severity)),
        "none",
    ).title()
    workflows = sorted({f.workflow_file for f in findings})
    remediation_findings = [f for f in findings if f.severity in _REMEDIATION]
    suggested_fixes = suggested_fixes or []

    lines: list[str] = [
        "# Grapple Security Report",
        "",
        f"**Conclusion:** {conclusion}",
        f"**Risk level:** {risk_level}",
        f"**Scanned workflows:** {len(workflows)}",
        f"**Findings:** {_summary_counts(findings)}",
        "",
        "Grapple treats only full 40-character commit SHAs as immutable. "
        "Tags, branches, and default-branch references are reported as mutable.",
    ]

    if remediation_findings:
        lines.extend([
            "",
            "## Remediation Summary",
            "",
            *_remediation_rows(ordered_findings),
        ])

    if suggested_fixes:
        lines.extend([
            "",
            "## Suggested Fixes",
            "",
            "These suggestions are read-only. Grapple has not modified the workflow files.",
            "",
            *_suggested_fix_rows(suggested_fixes),
        ])

    finding_index = 1
    for severity in _SEVERITY_ORDER:
        items = [f for f in ordered_findings if f.severity == severity]
        if not items:
            continue
        lines.extend(["", f"## {_SEVERITY_LABEL[severity]} Findings ({len(items)})", ""])
        for finding in items:
            lines.extend(_render_finding(finding, finding_index))
            lines.append("")
            finding_index += 1

    return "\n".join(lines).rstrip()
