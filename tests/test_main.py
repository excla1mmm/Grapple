from app.main import extract_event_target, process_event


SHA = "a" * 40


def push_payload(**overrides):
    payload = {
        "after": SHA,
        "ref": "refs/heads/main",
        "installation": {"id": 7},
        "repository": {"full_name": "base/repo"},
    }
    payload.update(overrides)
    return payload


def pr_payload(**overrides):
    payload = {
        "action": "opened",
        "number": 123,
        "installation": {"id": 7},
        "repository": {"full_name": "base/repo"},
        "pull_request": {
            "head": {
                "sha": SHA,
                "ref": "contributor-branch",
                "repo": {"full_name": "fork/repo"},
            },
        },
    }
    payload.update(overrides)
    return payload


def test_extract_event_target_push_uses_branch_for_content_ref():
    target = extract_event_target("push", push_payload())

    assert target is not None
    assert target.owner == "base"
    assert target.repo == "repo"
    assert target.content_ref == "main"
    assert target.sarif_ref == "refs/heads/main"
    assert target.head_sha == SHA


def test_extract_event_target_pull_request_uses_head_sha_for_content_ref():
    target = extract_event_target("pull_request", pr_payload())

    assert target is not None
    assert target.owner == "base"
    assert target.repo == "repo"
    assert target.content_ref == SHA
    assert target.sarif_ref == "refs/pull/123/head"
    assert target.head_sha == SHA


async def test_process_event_fetches_fork_pr_workflows_by_head_sha():
    class FakeClient:
        def __init__(self):
            self.workflow_calls = []
            self.check_runs = []
            self.sarif_uploads = []

        async def get_installation_token(self, installation_id):
            assert installation_id == 7
            return "token"

        async def get_workflow_files(self, owner, repo, ref, token):
            self.workflow_calls.append((owner, repo, ref, token))
            return [{
                "path": ".github/workflows/ci.yml",
                "content": """
permissions: read-all
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
""",
            }]

        async def create_check_run(self, **kwargs):
            self.check_runs.append(kwargs)
            return 42

        async def upload_sarif(self, **kwargs):
            self.sarif_uploads.append(kwargs)
            return "sarif-id"

    client = FakeClient()

    await process_event(client, frozenset(), "pull_request", pr_payload())

    assert client.workflow_calls == [("base", "repo", SHA, "token")]
    assert client.check_runs[0]["owner"] == "base"
    assert client.check_runs[0]["repo"] == "repo"
    assert client.check_runs[0]["head_sha"] == SHA
    assert client.sarif_uploads[0]["ref"] == "refs/pull/123/head"
