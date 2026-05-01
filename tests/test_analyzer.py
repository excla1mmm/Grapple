import textwrap

from app.analyzer import analyze_workflow, parse_workflow

HIGH_RISK = frozenset({"tj-actions/changed-files"})
SHA = "a" * 40


def wf(content: str) -> str:
    return textwrap.dedent(content).strip()


# --- parse_workflow ---

def test_parse_workflow_basic():
    structure = parse_workflow(wf("""
        jobs:
          build:
            runs-on: ubuntu-latest
            steps:
              - uses: actions/checkout@v4
    """), "ci.yml")
    assert len(structure.jobs) == 1
    job = structure.jobs[0]
    assert job.job_id == "build"
    assert not job.is_self_hosted
    assert "actions/checkout@v4" in job.step_uses
    assert job.reusable_uses is None


def test_parse_workflow_self_hosted():
    structure = parse_workflow(wf("""
        jobs:
          build:
            runs-on: self-hosted
            steps:
              - uses: actions/checkout@v4
    """), "ci.yml")
    assert structure.jobs[0].is_self_hosted


def test_parse_workflow_self_hosted_list():
    structure = parse_workflow(wf("""
        jobs:
          build:
            runs-on: [self-hosted, linux]
            steps: []
    """), "ci.yml")
    assert structure.jobs[0].is_self_hosted


def test_parse_workflow_reusable():
    structure = parse_workflow(wf("""
        jobs:
          deploy:
            uses: org/shared/.github/workflows/deploy.yml@main
    """), "ci.yml")
    assert structure.jobs[0].reusable_uses == "org/shared/.github/workflows/deploy.yml@main"


def test_parse_workflow_top_level_permissions():
    structure = parse_workflow(wf("""
        permissions:
          id-token: write
          contents: read
        jobs:
          build:
            runs-on: ubuntu-latest
            steps: []
    """), "ci.yml")
    assert structure.permissions == {"id-token": "write", "contents": "read"}


def test_parse_workflow_invalid_yaml():
    structure = parse_workflow(": bad yaml ][", "ci.yml")
    assert structure.jobs == []
    assert structure.permissions is None


# --- analyze_workflow: action severity ---

def test_sha_pinned_is_safe():
    text = wf(f"""
        permissions: read-all
        jobs:
          build:
            runs-on: ubuntu-latest
            steps:
              - uses: actions/checkout@{SHA}
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    action_findings = [f for f in findings if f.kind == "action"]
    assert len(action_findings) == 1
    assert action_findings[0].severity == "safe"


def test_tag_is_medium():
    text = wf("""
        permissions: read-all
        jobs:
          build:
            runs-on: ubuntu-latest
            steps:
              - uses: actions/checkout@v4
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    action_findings = [f for f in findings if f.kind == "action"]
    assert action_findings[0].severity == "medium"


def test_branch_is_high():
    text = wf("""
        permissions: read-all
        jobs:
          build:
            runs-on: ubuntu-latest
            steps:
              - uses: actions/checkout@main
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    action_findings = [f for f in findings if f.kind == "action"]
    assert action_findings[0].severity == "high"


def test_high_risk_unpinned_is_critical():
    text = wf("""
        permissions: read-all
        jobs:
          build:
            runs-on: ubuntu-latest
            steps:
              - uses: tj-actions/changed-files@v35
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    action_findings = [f for f in findings if f.kind == "action"]
    assert action_findings[0].severity == "critical"
    assert action_findings[0].is_high_risk is True


# --- analyze_workflow: self-hosted severity bump ---

def test_self_hosted_bumps_tag_to_high():
    text = wf("""
        permissions: read-all
        jobs:
          build:
            runs-on: self-hosted
            steps:
              - uses: actions/checkout@v4
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    action_findings = [f for f in findings if f.kind == "action"]
    assert action_findings[0].severity == "high"


def test_self_hosted_bumps_high_to_critical():
    text = wf("""
        permissions: read-all
        jobs:
          build:
            runs-on: self-hosted
            steps:
              - uses: actions/checkout@main
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    action_findings = [f for f in findings if f.kind == "action"]
    assert action_findings[0].severity == "critical"


def test_self_hosted_runner_finding_present():
    text = wf("""
        permissions: read-all
        jobs:
          build:
            runs-on: self-hosted
            steps: []
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    runner_findings = [f for f in findings if f.kind == "runner"]
    assert len(runner_findings) == 1
    assert runner_findings[0].severity == "info"


# --- analyze_workflow: permissions ---

def test_no_permissions_block_is_info():
    text = wf("""
        jobs:
          build:
            runs-on: ubuntu-latest
            steps: []
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    perm_findings = [f for f in findings if f.kind == "permission"]
    assert any(f.severity == "info" for f in perm_findings)


def test_write_all_is_critical():
    text = wf("""
        permissions: write-all
        jobs:
          build:
            runs-on: ubuntu-latest
            steps: []
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    perm_findings = [f for f in findings if f.kind == "permission"]
    assert any(f.severity == "critical" for f in perm_findings)


def test_id_token_write_is_critical():
    text = wf("""
        permissions:
          id-token: write
        jobs:
          build:
            runs-on: ubuntu-latest
            steps: []
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    perm_findings = [f for f in findings if f.kind == "permission"]
    assert any(
        f.severity == "critical" and "id-token" in f.action_name
        for f in perm_findings
    )


def test_contents_write_is_high():
    text = wf("""
        permissions:
          contents: write
        jobs:
          build:
            runs-on: ubuntu-latest
            steps: []
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    perm_findings = [f for f in findings if f.kind == "permission"]
    assert any(
        f.severity == "high" and "contents" in f.action_name
        for f in perm_findings
    )


def test_read_all_no_permission_finding():
    text = wf("""
        permissions: read-all
        jobs:
          build:
            runs-on: ubuntu-latest
            steps: []
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    perm_findings = [f for f in findings if f.kind == "permission"]
    assert perm_findings == []


# --- analyze_workflow: reusable workflows ---

def test_reusable_branch_is_critical():
    text = wf("""
        permissions: read-all
        jobs:
          deploy:
            uses: org/shared/.github/workflows/deploy.yml@main
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    rw = [f for f in findings if f.kind == "reusable_workflow"]
    assert rw[0].severity == "critical"


def test_reusable_tag_is_high():
    text = wf("""
        permissions: read-all
        jobs:
          deploy:
            uses: org/shared/.github/workflows/deploy.yml@v1
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    rw = [f for f in findings if f.kind == "reusable_workflow"]
    assert rw[0].severity == "high"


def test_reusable_sha_is_safe():
    text = wf(f"""
        permissions: read-all
        jobs:
          deploy:
            uses: org/shared/.github/workflows/deploy.yml@{SHA}
    """)
    findings = analyze_workflow(text, "ci.yml", HIGH_RISK)
    rw = [f for f in findings if f.kind == "reusable_workflow"]
    assert rw[0].severity == "safe"
