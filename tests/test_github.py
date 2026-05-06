import hashlib
import hmac

import httpx
import pytest
import respx

from app.github import GitHubAppClient


@pytest.fixture
async def client(rsa_private_key_pem):
    c = GitHubAppClient(
        app_id="12345",
        private_key=rsa_private_key_pem,
        webhook_secret="test-secret",
    )
    yield c
    await c.close()


# --- verify_webhook_signature ---

def test_verify_valid_signature(client):
    payload = b'{"action":"push"}'
    digest = hmac.new(b"test-secret", payload, hashlib.sha256).hexdigest()
    assert client.verify_webhook_signature(payload, f"sha256={digest}") is True


def test_verify_wrong_signature(client):
    assert client.verify_webhook_signature(b"payload", "sha256=deadbeef") is False


def test_verify_none_signature(client):
    assert client.verify_webhook_signature(b"payload", None) is False


def test_verify_empty_signature(client):
    assert client.verify_webhook_signature(b"payload", "") is False


def test_verify_tampered_payload(client):
    payload = b'{"action":"push"}'
    digest = hmac.new(b"test-secret", payload, hashlib.sha256).hexdigest()
    assert client.verify_webhook_signature(b"tampered", f"sha256={digest}") is False


# --- _make_jwt ---

def test_make_jwt_returns_string(client):
    token = client._make_jwt()
    assert isinstance(token, str)
    # JWT has three dot-separated parts
    assert token.count(".") == 2


# --- get_workflow_files ---

@respx.mock
async def test_get_workflow_files_success(client):
    respx.get(
        "https://api.github.com/repos/owner/repo/contents/.github/workflows",
        params={"ref": "main"},
    ).mock(return_value=httpx.Response(200, json=[
        {"type": "file", "name": "ci.yml", "path": ".github/workflows/ci.yml"},
        {"type": "dir",  "name": "templates", "path": ".github/workflows/templates"},
        {"type": "file", "name": "notes.txt", "path": ".github/workflows/notes.txt"},
    ]))
    respx.get(
        "https://api.github.com/repos/owner/repo/contents/.github/workflows/ci.yml",
        params={"ref": "main"},
    ).mock(return_value=httpx.Response(200, text="on: push\njobs: {}"))

    files = await client.get_workflow_files("owner", "repo", "main", "tok")
    assert len(files) == 1
    assert files[0]["name"] == "ci.yml"
    assert files[0]["content"] == "on: push\njobs: {}"


@respx.mock
async def test_get_workflow_files_404_returns_empty(client):
    respx.get(
        "https://api.github.com/repos/owner/repo/contents/.github/workflows",
    ).mock(return_value=httpx.Response(404))

    files = await client.get_workflow_files("owner", "repo", "main", "tok")
    assert files == []


@respx.mock
async def test_get_workflow_files_dict_response_returns_empty(client):
    respx.get(
        "https://api.github.com/repos/owner/repo/contents/.github/workflows",
    ).mock(return_value=httpx.Response(200, json={"type": "file", "name": "ci.yml"}))

    files = await client.get_workflow_files("owner", "repo", "main", "tok")
    assert files == []


# --- create_check_run ---

@respx.mock
async def test_create_check_run_returns_id(client):
    respx.post("https://api.github.com/repos/owner/repo/check-runs").mock(
        return_value=httpx.Response(201, json={"id": 42})
    )
    run_id = await client.create_check_run(
        owner="owner", repo="repo", head_sha="abc123",
        token="tok", title="Grapple results", summary="No issues.",
    )
    assert run_id == 42


async def test_create_check_run_empty_title_raises(client):
    with pytest.raises(ValueError, match="title"):
        await client.create_check_run(
            owner="o", repo="r", head_sha="x", token="t",
            title="", summary="s",
        )


# --- upload_sarif ---

@respx.mock
async def test_upload_sarif_returns_id(client):
    respx.post("https://api.github.com/repos/owner/repo/code-scanning/sarifs").mock(
        return_value=httpx.Response(202, json={"id": "sarif-uuid-123"})
    )
    sarif_id = await client.upload_sarif(
        owner="owner", repo="repo",
        commit_sha="abc123",
        ref="refs/heads/main",
        sarif_gzip_b64="base64encodeddata",
        token="tok",
    )
    assert sarif_id == "sarif-uuid-123"


# --- resolve_branch_to_sha ---

@respx.mock
async def test_resolve_branch_to_sha_returns_commit_sha(client):
    respx.get("https://api.github.com/repos/owner/repo/git/ref/heads/main").mock(
        return_value=httpx.Response(200, json={
            "object": {"type": "commit", "sha": "a" * 40}
        })
    )

    sha = await client.resolve_branch_to_sha("owner", "repo", "main", "tok")

    assert sha == "a" * 40


@respx.mock
async def test_resolve_branch_to_sha_404_returns_none(client):
    respx.get("https://api.github.com/repos/owner/repo/git/ref/heads/missing").mock(
        return_value=httpx.Response(404)
    )

    sha = await client.resolve_branch_to_sha("owner", "repo", "missing", "tok")

    assert sha is None
