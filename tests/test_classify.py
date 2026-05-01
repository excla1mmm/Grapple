import textwrap
from pathlib import Path

import pytest

from core.classify import (
    _parse_incidents_yaml,
    classify_ref,
    classify_uses,
    extract_uses_from_node,
    fallback_extract_uses_lines,
    load_high_risk_actions,
)

HIGH_RISK = frozenset({"tj-actions/changed-files", "aquasecurity/trivy-action"})
SHA = "a" * 40


# --- classify_ref ---

@pytest.mark.parametrize("ref,expected", [
    ("a" * 40, "sha"),
    ("0" * 40, "sha"),
    ("v4", "tag"),
    ("v1.2.3", "tag"),
    ("v1.0-beta", "tag"),
    ("2024.03.15", "tag"),
    ("latest", "tag"),
    ("main", "branch"),
    ("master", "branch"),
    ("develop", "branch"),
    ("feature/my-feature", "branch"),
    ("fix/bug-123", "branch"),
    ("refs/heads/main", "branch"),
    ("refs/tags/v4", "tag"),
    ("some/nested/path", "branch"),
])
def test_classify_ref(ref, expected):
    assert classify_ref(ref) == expected


# --- classify_uses ---

def test_classify_uses_local():
    result = classify_uses("./my-action", HIGH_RISK)
    assert result["pin_type"] == "local"
    assert result["is_pinned"] is True
    assert result["is_high_risk"] is False


def test_classify_uses_docker():
    result = classify_uses("docker://alpine:3.19", HIGH_RISK)
    assert result["pin_type"] == "docker"
    assert result["is_pinned"] is False


def test_classify_uses_sha_pinned():
    result = classify_uses(f"actions/checkout@{SHA}", HIGH_RISK)
    assert result["pin_type"] == "sha"
    assert result["is_pinned"] is True
    assert result["action_name"] == "actions/checkout"
    assert result["ref"] == SHA


def test_classify_uses_tag():
    result = classify_uses("actions/checkout@v4", HIGH_RISK)
    assert result["pin_type"] == "tag"
    assert result["is_pinned"] is False


def test_classify_uses_branch():
    result = classify_uses("actions/checkout@main", HIGH_RISK)
    assert result["pin_type"] == "branch"
    assert result["is_pinned"] is False


def test_classify_uses_no_ref_is_branch():
    result = classify_uses("actions/checkout", HIGH_RISK)
    assert result["pin_type"] == "branch"
    assert result["is_pinned"] is False
    assert result["ref"] == ""


def test_classify_uses_high_risk_unpinned():
    result = classify_uses("tj-actions/changed-files@v35", HIGH_RISK)
    assert result["is_high_risk"] is True
    assert result["is_pinned"] is False


def test_classify_uses_high_risk_pinned():
    result = classify_uses(f"tj-actions/changed-files@{SHA}", HIGH_RISK)
    assert result["is_high_risk"] is True
    assert result["is_pinned"] is True


def test_classify_uses_action_name_strips_subpath():
    result = classify_uses("actions/aws-actions/configure-aws-credentials@v4", HIGH_RISK)
    assert result["action_name"] == "actions/aws-actions"


# --- _parse_incidents_yaml ---

def test_parse_incidents_yaml_valid():
    yaml_text = "- action: tj-actions/changed-files\n- action: aquasecurity/trivy-action\n"
    result = _parse_incidents_yaml(yaml_text)
    assert "tj-actions/changed-files" in result
    assert "aquasecurity/trivy-action" in result


def test_parse_incidents_yaml_invalid_yaml():
    with pytest.raises(RuntimeError, match="Failed to parse"):
        _parse_incidents_yaml(": bad: yaml: ][")


def test_parse_incidents_yaml_not_list():
    with pytest.raises(RuntimeError, match="must be a YAML list"):
        _parse_incidents_yaml("key: value\n")


# --- load_high_risk_actions ---

def test_load_high_risk_actions_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_high_risk_actions(tmp_path / "nonexistent.yml")


def test_load_high_risk_actions_valid(tmp_path):
    incidents = tmp_path / "database.yml"
    incidents.write_text("- action: some/action\n  severity: critical\n")
    result = load_high_risk_actions(incidents)
    assert "some/action" in result


# --- fallback_extract_uses_lines / extract_uses_from_node ---

def test_fallback_extract_uses_lines():
    workflow = textwrap.dedent("""
        steps:
          - uses: actions/checkout@v4
          - uses: actions/setup-node@v3
          - run: echo hello
    """)
    result = fallback_extract_uses_lines(workflow)
    assert "actions/checkout@v4" in result
    assert "actions/setup-node@v3" in result


def test_extract_uses_from_node_nested():
    node = {
        "jobs": {
            "build": {
                "steps": [
                    {"uses": "actions/checkout@v4"},
                    {"run": "echo hello"},
                    {"uses": "actions/setup-node@v3"},
                ]
            }
        }
    }
    result = extract_uses_from_node(node)
    assert "actions/checkout@v4" in result
    assert "actions/setup-node@v3" in result
    assert len(result) == 2
