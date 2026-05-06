from app.analyzer import Finding
from app.fix_plan import build_suggested_fixes, parse_uses_ref


def finding(**overrides):
    defaults = {
        "kind": "action",
        "workflow_file": ".github/workflows/ci.yml",
        "action_name": "actions/checkout",
        "ref": "v4",
        "pin_type": "tag",
        "severity": "medium",
        "message": "mutable ref",
        "is_high_risk": False,
        "is_pinned": False,
        "line_number": 7,
        "uses_raw": "actions/checkout@v4",
    }
    defaults.update(overrides)
    return Finding(**defaults)


class FakeResolver:
    def __init__(self):
        self.tag_calls = []
        self.branch_calls = []
        self.default_branch_calls = []

    async def get_default_branch(self, owner, repo, token):
        self.default_branch_calls.append((owner, repo, token))
        return "main"

    async def resolve_tag_to_sha(self, owner, repo, tag, token):
        self.tag_calls.append((owner, repo, tag, token))
        return "1" * 40

    async def resolve_branch_to_sha(self, owner, repo, branch, token):
        self.branch_calls.append((owner, repo, branch, token))
        return "2" * 40


def test_parse_uses_ref_preserves_subpath():
    result = parse_uses_ref(finding(
        uses_raw="docker/login-action/subdir@v3",
        action_name="docker/login-action",
        ref="v3",
    ))

    assert result is not None
    assert result.owner == "docker"
    assert result.repo == "login-action"
    assert result.prefix == "docker/login-action/subdir"
    assert result.ref == "v3"


def test_parse_uses_ref_for_reusable_workflow():
    result = parse_uses_ref(finding(
        kind="reusable_workflow",
        uses_raw="org/shared/.github/workflows/deploy.yml@main",
        action_name="org/shared",
        ref="main",
        pin_type="branch",
        severity="critical",
    ))

    assert result is not None
    assert result.owner == "org"
    assert result.repo == "shared"
    assert result.prefix == "org/shared/.github/workflows/deploy.yml"
    assert result.ref == "main"


async def test_build_suggested_fixes_resolves_tag_to_sha():
    resolver = FakeResolver()

    fixes = await build_suggested_fixes(resolver, [finding()], "tok")

    assert resolver.tag_calls == [("actions", "checkout", "v4", "tok")]
    assert fixes[0].current == "actions/checkout@v4"
    assert fixes[0].suggested == f"actions/checkout@{'1' * 40}"
    assert fixes[0].line_number == 7


async def test_build_suggested_fixes_resolves_branch_to_sha():
    resolver = FakeResolver()
    f = finding(
        uses_raw="actions/setup-node@main",
        action_name="actions/setup-node",
        ref="main",
        pin_type="branch",
        severity="high",
        line_number=9,
    )

    fixes = await build_suggested_fixes(resolver, [f], "tok")

    assert resolver.branch_calls == [("actions", "setup-node", "main", "tok")]
    assert fixes[0].suggested == f"actions/setup-node@{'2' * 40}"


async def test_build_suggested_fixes_resolves_default_branch_when_ref_missing():
    resolver = FakeResolver()
    f = finding(
        uses_raw="actions/setup-node",
        action_name="actions/setup-node",
        ref="",
        pin_type="branch",
        severity="high",
    )

    fixes = await build_suggested_fixes(resolver, [f], "tok")

    assert resolver.default_branch_calls == [("actions", "setup-node", "tok")]
    assert resolver.branch_calls == [("actions", "setup-node", "main", "tok")]
    assert fixes[0].suggested == f"actions/setup-node@{'2' * 40}"


async def test_build_suggested_fixes_skips_safe_and_non_action_findings():
    resolver = FakeResolver()
    findings = [
        finding(pin_type="sha", is_pinned=True, ref="a" * 40, uses_raw=f"actions/checkout@{'a' * 40}"),
        finding(kind="permission", action_name="permissions", pin_type="permission"),
    ]

    fixes = await build_suggested_fixes(resolver, findings, "tok")

    assert fixes == []
    assert resolver.tag_calls == []
    assert resolver.branch_calls == []


async def test_build_suggested_fixes_caches_duplicate_refs():
    resolver = FakeResolver()
    findings = [
        finding(line_number=7),
        finding(line_number=8),
    ]

    fixes = await build_suggested_fixes(resolver, findings, "tok")

    assert len(fixes) == 2
    assert resolver.tag_calls == [("actions", "checkout", "v4", "tok")]


async def test_build_suggested_fixes_orders_by_severity():
    resolver = FakeResolver()
    findings = [
        finding(
            severity="medium",
            uses_raw="actions/checkout@v4",
            action_name="actions/checkout",
            ref="v4",
            line_number=7,
        ),
        finding(
            severity="critical",
            uses_raw="tj-actions/changed-files@v45",
            action_name="tj-actions/changed-files",
            ref="v45",
            line_number=8,
            is_high_risk=True,
        ),
        finding(
            severity="high",
            uses_raw="actions/setup-node@main",
            action_name="actions/setup-node",
            ref="main",
            pin_type="branch",
            line_number=9,
        ),
    ]

    fixes = await build_suggested_fixes(resolver, findings, "tok")

    assert [fix.current for fix in fixes] == [
        "tj-actions/changed-files@v45",
        "actions/setup-node@main",
        "actions/checkout@v4",
    ]
