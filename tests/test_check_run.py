from conftest import make_finding
from app.check_run import annotations_for, conclusion_for, summary_for, title_for
from app.fix_plan import SuggestedFix


# --- conclusion_for ---

def test_conclusion_no_findings():
    assert conclusion_for([]) == "success"


def test_conclusion_safe_only():
    assert conclusion_for([make_finding(severity="safe")]) == "success"


def test_conclusion_info_only():
    assert conclusion_for([make_finding(severity="info")]) == "success"


def test_conclusion_medium():
    assert conclusion_for([make_finding(severity="medium")]) == "neutral"


def test_conclusion_high():
    assert conclusion_for([make_finding(severity="high")]) == "neutral"


def test_conclusion_critical():
    assert conclusion_for([make_finding(severity="critical")]) == "failure"


def test_conclusion_critical_wins_over_safe():
    findings = [make_finding(severity="safe"), make_finding(severity="critical")]
    assert conclusion_for(findings) == "failure"


# --- title_for ---

def test_title_no_findings():
    assert title_for([]) == "Grapple Security: no GitHub Actions found"


def test_title_all_safe():
    assert title_for([make_finding(severity="safe")]) == "Grapple Security: no blocking findings"


def test_title_single_critical():
    assert "1 critical" in title_for([make_finding(severity="critical")])


def test_title_multiple_critical():
    findings = [make_finding(severity="critical")] * 3
    assert "3 critical" in title_for(findings)


def test_title_issues_no_critical():
    findings = [make_finding(severity="high"), make_finding(severity="medium")]
    assert "2 finding" in title_for(findings)


# --- summary_for ---

def test_summary_no_findings():
    text = summary_for([])
    assert "# Grapple Security Report" in text
    assert "**Conclusion:** Success" in text
    assert "`uses:`" in text


def test_summary_groups_by_severity():
    findings = [
        make_finding(severity="critical", action_name="bad/action"),
        make_finding(severity="medium", action_name="ok/action"),
    ]
    text = summary_for(findings)
    assert "## Critical Findings" in text
    assert "## Medium Findings" in text
    assert text.index("## Critical Findings") < text.index("## Medium Findings")


def test_summary_action_finding_format():
    f = make_finding(
        kind="action",
        action_name="actions/checkout",
        ref="v4",
        line_number=7,
    )
    text = summary_for([f])
    assert "### GHA-001: `actions/checkout@v4`" in text
    assert "**Location:** `.github/workflows/ci.yml:7`" in text
    assert "**Pin type:** `tag`" in text
    assert "Pin this dependency to a full 40-character commit SHA" in text


def test_summary_permission_finding_format():
    f = make_finding(
        kind="permission",
        action_name="permissions",
        ref="write-all",
        severity="critical",
        message="write-all grants everything.",
    )
    text = summary_for([f])
    assert "`permissions: write-all`" in text
    assert "**Type:** `permission`" in text
    assert "`permissions@" not in text


def test_summary_safe_findings_included():
    findings = [
        make_finding(severity="critical"),
        make_finding(severity="safe"),
    ]
    text = summary_for(findings)
    assert "## Critical Findings" in text
    assert "## Verified Findings" in text


def test_summary_contains_report_header_and_counts():
    findings = [
        make_finding(severity="critical", workflow_file=".github/workflows/ci.yml"),
        make_finding(severity="medium", workflow_file=".github/workflows/release.yml"),
    ]
    text = summary_for(findings)
    assert "# Grapple Security Report" in text
    assert "**Conclusion:** Failure" in text
    assert "**Risk level:** Critical" in text
    assert "**Scanned workflows:** 2" in text
    assert "**Findings:** 1 critical, 1 medium" in text


def test_summary_contains_remediation_table():
    text = summary_for([make_finding(severity="medium", line_number=12)])
    assert "## Remediation Summary" in text
    assert "| Finding | Location | Recommendation |" in text
    assert "| GHA-001 | `.github/workflows/ci.yml:12` |" in text


def test_summary_contains_suggested_fixes_section():
    fixes = [
        SuggestedFix(
            workflow_file=".github/workflows/ci.yml",
            line_number=7,
            current="actions/checkout@v4",
            suggested=f"actions/checkout@{'1' * 40}",
            resolved_sha="1" * 40,
        )
    ]

    text = summary_for([make_finding(severity="medium")], suggested_fixes=fixes)

    assert "## Suggested Fixes" in text
    assert "read-only" in text
    assert "| `.github/workflows/ci.yml:7` | `actions/checkout@v4` |" in text
    assert f"`actions/checkout@{'1' * 40}`" in text


def test_summary_remediation_ids_match_severity_ordered_sections():
    findings = [
        make_finding(
            severity="medium",
            action_name="actions/checkout",
            ref="v4",
            line_number=7,
        ),
        make_finding(
            severity="critical",
            action_name="tj-actions/changed-files",
            ref="v45",
            line_number=8,
            is_high_risk=True,
        ),
        make_finding(
            severity="high",
            action_name="actions/setup-node",
            ref="main",
            pin_type="branch",
            line_number=9,
        ),
    ]
    text = summary_for(findings)
    assert "| GHA-001 | `.github/workflows/ci.yml:8` |" in text
    assert "### GHA-001: `tj-actions/changed-files@v45`" in text
    assert "| GHA-002 | `.github/workflows/ci.yml:9` |" in text
    assert "### GHA-002: `actions/setup-node@main`" in text
    assert "| GHA-003 | `.github/workflows/ci.yml:7` |" in text
    assert "### GHA-003: `actions/checkout@v4`" in text


def test_summary_runner_finding_includes_runner_recommendation():
    f = make_finding(
        kind="runner",
        action_name="job 'build'",
        ref="self-hosted",
        pin_type="runner",
        severity="info",
    )
    text = summary_for([f])
    assert "Review whether this job must run on a self-hosted runner" in text
    assert "outside GitHub-hosted isolation" in text


def test_summary_self_hosted_bump_context():
    f = make_finding(severity="high", pin_type="tag")
    text = summary_for([f])
    assert "Severity was raised because this action runs on a self-hosted runner" in text


def test_summary_severity_order():
    findings = [
        make_finding(severity="info"),
        make_finding(severity="high"),
        make_finding(severity="critical"),
        make_finding(severity="medium"),
    ]
    text = summary_for(findings)
    positions = {
        s: text.index(f"## {label} Findings")
        for s, label in (
            ("critical", "Critical"),
            ("high", "High"),
            ("medium", "Medium"),
            ("info", "Informational"),
        )
    }
    assert positions["critical"] < positions["high"] < positions["medium"] < positions["info"]


# --- annotations_for ---

def test_annotations_for_line_addressable_findings():
    findings = [
        make_finding(
            severity="medium",
            action_name="actions/checkout",
            ref="v4",
            line_number=7,
        ),
        make_finding(
            severity="critical",
            action_name="tj-actions/changed-files",
            ref="v45",
            line_number=8,
            is_high_risk=True,
        ),
        make_finding(
            severity="high",
            action_name="actions/setup-node",
            ref="main",
            pin_type="branch",
            line_number=9,
        ),
        make_finding(
            kind="runner",
            action_name="job 'build'",
            ref="self-hosted",
            pin_type="runner",
            severity="info",
            line_number=10,
        ),
    ]

    annotations = annotations_for(findings)

    assert [a["title"] for a in annotations] == [
        "GHA-001: tj-actions/changed-files@v45",
        "GHA-002: actions/setup-node@main",
        "GHA-003: actions/checkout@v4",
        "GHA-004: job 'build'",
    ]
    assert [a["annotation_level"] for a in annotations] == [
        "failure",
        "failure",
        "warning",
        "notice",
    ]
    assert annotations[0]["path"] == ".github/workflows/ci.yml"
    assert annotations[0]["start_line"] == 8
    assert annotations[0]["end_line"] == 8
    assert "Recommendation:" in annotations[0]["message"]


def test_annotations_skip_safe_and_file_level_findings():
    findings = [
        make_finding(severity="safe", line_number=4),
        make_finding(severity="critical", line_number=None),
        make_finding(severity="medium", line_number=9),
    ]

    annotations = annotations_for(findings)

    assert len(annotations) == 1
    assert annotations[0]["start_line"] == 9
    assert annotations[0]["annotation_level"] == "warning"


def test_annotations_limit_to_top_50_findings():
    findings = [
        make_finding(severity="medium", line_number=index + 1)
        for index in range(55)
    ]

    annotations = annotations_for(findings)

    assert len(annotations) == 50
    assert annotations[-1]["title"] == "GHA-050: actions/checkout@v4"
    assert annotations[-1]["start_line"] == 50
