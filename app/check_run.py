"""Format Grapple findings into GitHub Check Run output (title, summary, conclusion)."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from app.analyzer import Finding

_SEVERITY_EMOJI = {
    "critical": "🔴",
    "high": "🟠",
    "medium": "🟡",
    "info": "🔵",
    "safe": "✅",
}
_SEVERITY_ORDER = ("critical", "high", "medium", "info", "safe")
_BLOCKING = {"critical"}


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
        return "No GitHub Actions to scan"
    counts = Counter(f.severity for f in findings)
    crit = counts.get("critical", 0)
    if crit:
        return f"{crit} critical issue(s) — action required"
    issues = counts.get("high", 0) + counts.get("medium", 0)
    if issues:
        return f"{issues} issue(s) need attention"
    return "All actions look safe"


def _render_finding(f: Finding) -> str:
    """Render a single finding as a markdown bullet, varying by kind."""
    if f.kind in ("action", "reusable_workflow"):
        ref = f.ref or "default"
        return f"- `{f.action_name}@{ref}` in `{f.workflow_file}` — {f.message}"
    # permission / runner findings: action_name already names the thing,
    # avoid the awkward `permissions.id-token@write` prefix.
    return f"- In `{f.workflow_file}`: {f.message}"


def summary_for(findings: list[Finding]) -> str:
    if not findings:
        return "No `uses:` references found in `.github/workflows/`."

    by_severity: dict[str, list[Finding]] = {}
    for f in findings:
        by_severity.setdefault(f.severity, []).append(f)

    lines: list[str] = []
    for severity in _SEVERITY_ORDER:
        items = by_severity.get(severity, [])
        if not items:
            continue
        emoji = _SEVERITY_EMOJI.get(severity, "")
        lines.append(f"### {emoji} {severity.upper()} ({len(items)})")
        for f in items:
            lines.append(_render_finding(f))
        lines.append("")
    return "\n".join(lines).rstrip()
