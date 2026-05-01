from conftest import make_finding
from app.check_run import conclusion_for, summary_for, title_for


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
    assert title_for([]) == "No GitHub Actions to scan"


def test_title_all_safe():
    assert title_for([make_finding(severity="safe")]) == "All actions look safe"


def test_title_single_critical():
    assert "1 critical" in title_for([make_finding(severity="critical")])


def test_title_multiple_critical():
    findings = [make_finding(severity="critical")] * 3
    assert "3 critical" in title_for(findings)


def test_title_issues_no_critical():
    findings = [make_finding(severity="high"), make_finding(severity="medium")]
    assert "2 issue" in title_for(findings)


# --- summary_for ---

def test_summary_no_findings():
    assert "`uses:`" in summary_for([])


def test_summary_groups_by_severity():
    findings = [
        make_finding(severity="critical", action_name="bad/action"),
        make_finding(severity="medium", action_name="ok/action"),
    ]
    text = summary_for(findings)
    assert "CRITICAL" in text
    assert "MEDIUM" in text
    assert text.index("CRITICAL") < text.index("MEDIUM")


def test_summary_action_finding_format():
    f = make_finding(kind="action", action_name="actions/checkout", ref="v4")
    text = summary_for([f])
    assert "`actions/checkout@v4`" in text


def test_summary_permission_finding_format():
    f = make_finding(
        kind="permission",
        action_name="permissions",
        ref="write-all",
        severity="critical",
        message="write-all grants everything.",
    )
    text = summary_for([f])
    assert "In `" in text
    assert "`permissions@" not in text


def test_summary_safe_findings_included():
    findings = [
        make_finding(severity="critical"),
        make_finding(severity="safe"),
    ]
    text = summary_for(findings)
    assert "CRITICAL" in text
    assert "SAFE" in text


def test_summary_severity_order():
    findings = [
        make_finding(severity="info"),
        make_finding(severity="high"),
        make_finding(severity="critical"),
        make_finding(severity="medium"),
    ]
    text = summary_for(findings)
    positions = {s: text.index(s.upper()) for s in ("critical", "high", "medium", "info")}
    assert positions["critical"] < positions["high"] < positions["medium"] < positions["info"]
