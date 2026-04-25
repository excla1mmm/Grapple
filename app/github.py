"""GitHub App client: JWT auth, installation tokens, API calls via httpx."""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass, field

import httpx
import jwt

LOGGER = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
JWT_EXPIRATION_SECONDS = 600  # 10 min (GitHub maximum)
# GitHub recommends dating the JWT 60s in the past to account for clock drift.
JWT_CLOCK_DRIFT_SECONDS = 60


@dataclass
class GitHubAppClient:
    """Async client for GitHub App operations.

    Authentication is two-step:
      1. app_id + private_key -> JWT  (identifies the *App*)
      2. JWT -> installation_token    (scoped to a specific repo/org)

    Usage::

        async with GitHubAppClient(app_id, private_key, webhook_secret) as gh:
            token = await gh.get_installation_token(installation_id)
            files = await gh.get_workflow_files("owner", "repo", "main", token)
    """

    app_id: str
    private_key: str = field(repr=False)
    webhook_secret: str = field(repr=False)
    _client: httpx.AsyncClient = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=GITHUB_API,
            headers={"Accept": "application/vnd.github+json"},
            timeout=30.0,
        )

    async def __aenter__(self) -> GitHubAppClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _make_jwt(self) -> str:
        now = int(time.time())
        payload = {
            "iat": now - JWT_CLOCK_DRIFT_SECONDS,
            "exp": now + JWT_EXPIRATION_SECONDS,
            "iss": self.app_id,
        }
        return jwt.encode(payload, self.private_key, algorithm="RS256")

    async def get_installation_token(self, installation_id: int) -> str:
        """Exchange JWT for a short-lived installation access token."""
        token = self._make_jwt()
        resp = await self._client.post(
            f"/app/installations/{installation_id}/access_tokens",
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        return resp.json()["token"]

    # ------------------------------------------------------------------
    # Reading data
    # ------------------------------------------------------------------

    async def get_workflow_files(
        self,
        owner: str,
        repo: str,
        ref: str,
        token: str,
    ) -> list[dict[str, str]]:
        """Fetch all .yml/.yaml files from .github/workflows/.

        Returns a list of ``{"name": "ci.yml", "content": "...", "path": "..."}``.
        """
        headers = {"Authorization": f"token {token}"}
        resp = await self._client.get(
            f"/repos/{owner}/{repo}/contents/.github/workflows",
            params={"ref": ref},
            headers=headers,
        )
        if resp.status_code == 404:
            return []
        resp.raise_for_status()

        entries = resp.json()
        if isinstance(entries, dict):
            # API returns dict for a single file, list for a directory.
            LOGGER.warning(".github/workflows is not a directory in %s/%s", owner, repo)
            return []

        files: list[dict[str, str]] = []

        for entry in entries:
            if entry.get("type") != "file":
                continue
            name: str = entry.get("name", "")
            if not name.endswith((".yml", ".yaml")):
                continue

            file_resp = await self._client.get(
                f"/repos/{owner}/{repo}/contents/{entry['path']}",
                params={"ref": ref},
                headers={**headers, "Accept": "application/vnd.github.raw+json"},
            )
            if file_resp.status_code != 200:
                LOGGER.warning("Failed to fetch %s/%s:%s — %s", owner, repo, entry["path"], file_resp.status_code)
                continue

            files.append({
                "name": name,
                "path": entry["path"],
                "content": file_resp.text,
            })

        return files

    async def get_action_yml(
        self,
        owner: str,
        repo: str,
        ref: str,
        token: str,
    ) -> str | None:
        """Fetch action.yml (or action.yaml) for transitive composite analysis."""
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.raw+json",
        }
        for filename in ("action.yml", "action.yaml"):
            resp = await self._client.get(
                f"/repos/{owner}/{repo}/contents/{filename}",
                params={"ref": ref},
                headers=headers,
            )
            if resp.status_code == 200:
                return resp.text

        return None

    async def get_default_branch(
        self,
        owner: str,
        repo: str,
        token: str,
    ) -> str:
        """Return the default branch name for a repository."""
        headers = {"Authorization": f"token {token}"}
        resp = await self._client.get(
            f"/repos/{owner}/{repo}",
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()["default_branch"]

    # ------------------------------------------------------------------
    # Writing results
    # ------------------------------------------------------------------

    async def create_check_run(
        self,
        owner: str,
        repo: str,
        head_sha: str,
        token: str,
        *,
        title: str,
        summary: str,
        name: str = "Grapple Security",
        conclusion: str = "neutral",
        annotations: list[dict] | None = None,
    ) -> int:
        """Create a GitHub Check Run with optional annotations.

        ``title`` is required by the GitHub API when ``output`` is present.
        Returns the check_run id.
        """
        if not title:
            raise ValueError("create_check_run: title must be non-empty")
        output: dict = {"title": title, "summary": summary}
        if annotations:
            if len(annotations) > 50:
                LOGGER.warning(
                    "create_check_run: %d annotations submitted, only the first 50 are sent",
                    len(annotations),
                )
            output["annotations"] = annotations[:50]

        body = {
            "name": name,
            "head_sha": head_sha,
            "status": "completed",
            "conclusion": conclusion,
            "output": output,
        }
        headers = {"Authorization": f"token {token}"}
        resp = await self._client.post(
            f"/repos/{owner}/{repo}/check-runs",
            json=body,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()["id"]

    async def upload_sarif(
        self,
        owner: str,
        repo: str,
        commit_sha: str,
        ref: str,
        sarif_gzip_b64: str,
        token: str,
    ) -> str:
        """Upload a SARIF report to GitHub Code Scanning.

        ``sarif_gzip_b64`` must be a base64-encoded gzip of the SARIF JSON.
        Returns the sarif_id for status tracking.
        """
        headers = {"Authorization": f"token {token}"}
        body = {
            "commit_sha": commit_sha,
            "ref": ref,
            "sarif": sarif_gzip_b64,
        }
        resp = await self._client.post(
            f"/repos/{owner}/{repo}/code-scanning/sarifs",
            json=body,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()["id"]

    async def create_pull_request(
        self,
        owner: str,
        repo: str,
        token: str,
        *,
        title: str,
        body: str,
        head: str,
        base: str,
    ) -> int:
        """Create a pull request. Returns the PR number."""
        headers = {"Authorization": f"token {token}"}
        resp = await self._client.post(
            f"/repos/{owner}/{repo}/pulls",
            json={"title": title, "body": body, "head": head, "base": base},
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()["number"]

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    async def resolve_tag_to_sha(
        self,
        owner: str,
        repo: str,
        tag: str,
        token: str,
    ) -> str | None:
        """Resolve a git tag to a full commit SHA.

        Handles both lightweight and annotated tags.
        Returns None if the tag doesn't exist.
        """
        headers = {"Authorization": f"token {token}"}
        resp = await self._client.get(
            f"/repos/{owner}/{repo}/git/ref/tags/{tag}",
            headers=headers,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()

        obj = resp.json()["object"]

        # Annotated tags point to a tag object, not a commit.
        # Dereference one level to get the commit SHA.
        if obj["type"] == "tag":
            resp = await self._client.get(obj["url"], headers=headers)
            resp.raise_for_status()
            obj = resp.json()["object"]

        return obj["sha"]

    def verify_webhook_signature(self, payload: bytes, signature: str | None) -> bool:
        """Verify X-Hub-Signature-256 from a GitHub webhook.

        Returns False if the header is missing — never raises.
        """
        if not signature:
            return False
        expected = hmac.new(
            self.webhook_secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(f"sha256={expected}", signature)
