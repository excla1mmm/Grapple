"""Convert Grapple findings to SARIF 2.1.0 for GitHub Code Scanning."""

from __future__ import annotations

import gzip
import json
from base64 import b64encode

from app.analyzer import Finding

_TOOL_URI = "https://github.com/excla1mmm/Grapple"

_RULES = [
    {
        "id": "incident-action",
        "name": "KnownIncidentAction",
        "shortDescription": {"text": "Action involved in a known supply chain incident"},
        "helpUri": _TOOL_URI,
        "properties": {"tags": ["supply-chain", "security"]},
    },
    {
        "id": "unpinned-action",
        "name": "UnpinnedAction",
        "shortDescription": {"text": "GitHub Action not pinned to a full commit SHA"},
        "helpUri": _TOOL_URI,
        "properties": {"tags": ["supply-chain", "security"]},
    },
    {
        "id": "reusable-workflow-unpinned",
        "name": "UnpinnedReusableWorkflow",
        "shortDescription": {"text": "Reusable workflow not pinned to a full commit SHA"},
        "helpUri": _TOOL_URI,
        "properties": {"tags": ["supply-chain", "security"]},
    },
    {
        "id": "excess-permissions",
        "name": "ExcessivePermissions",
        "shortDescription": {"text": "Workflow or job grants overly broad permissions"},
        "helpUri": _TOOL_URI,
        "properties": {"tags": ["permissions", "security"]},
    },
    {
        "id": "self-hosted-runner",
        "name": "SelfHostedRunner",
        "shortDescription": {"text": "Job runs on a self-hosted runner"},
        "helpUri": _TOOL_URI,
        "properties": {"tags": ["runner", "security"]},
    },
]

_SEVERITY_TO_LEVEL: dict[str, str] = {
    "critical": "error",
    "high": "error",
    "medium": "warning",
    "info": "note",
}


def _rule_id(f: Finding) -> str:
    if f.kind == "permission":
        return "excess-permissions"
    if f.kind == "runner":
        return "self-hosted-runner"
    if f.kind == "reusable_workflow":
        return "reusable-workflow-unpinned"
    if f.is_high_risk:
        return "incident-action"
    return "unpinned-action"


def findings_to_sarif(findings: list[Finding]) -> dict:
    """Convert findings to a SARIF 2.1.0 document."""
    results = []
    for f in findings:
        level = _SEVERITY_TO_LEVEL.get(f.severity)
        if level is None:
            continue  # skip "safe" findings
        physical_location = {
            "artifactLocation": {
                "uri": f.workflow_file,
                "uriBaseId": "%SRCROOT%",
            },
        }
        if f.line_number is not None:
            physical_location["region"] = {"startLine": f.line_number}
        results.append({
            "ruleId": _rule_id(f),
            "level": level,
            "message": {"text": f.message},
            "locations": [{
                "physicalLocation": physical_location,
            }],
        })

    return {
        "version": "2.1.0",
        "$schema": (
            "https://raw.githubusercontent.com/oasis-tcs/sarif-spec"
            "/master/Schemata/sarif-schema-2.1.0.json"
        ),
        "runs": [{
            "tool": {
                "driver": {
                    "name": "grapple",
                    "informationUri": _TOOL_URI,
                    "rules": _RULES,
                }
            },
            "results": results,
        }],
    }


def compress_sarif(sarif: dict) -> str:
    """Gzip-compress and base64-encode a SARIF document for the GitHub API."""
    compressed = gzip.compress(json.dumps(sarif).encode("utf-8"))
    return b64encode(compressed).decode("ascii")
