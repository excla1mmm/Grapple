from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_REPOS_FILE = ROOT_DIR / "data" / "raw" / "repos.jsonl"
DEFAULT_WORKFLOWS_DIR = ROOT_DIR / "data" / "raw" / "workflows"
DEFAULT_ENV_FILE = ROOT_DIR / ".env"

GITHUB_API_BASE = "https://api.github.com"
SEARCH_REPOS_ENDPOINT = f"{GITHUB_API_BASE}/search/repositories"

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class GitHubClient:
    token: str
    user_agent: str = "grapple-fetcher/0.1"
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: int = 2
    request_delay_seconds: float = 0.1

    def request_json(self, url: str) -> Any:
        response = self._request(url)
        return json.loads(response.read().decode("utf-8"))

    def request_text(self, url: str) -> str:
        response = self._request(url)
        return response.read().decode("utf-8", errors="replace")

    def _request(self, url: str):
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "User-Agent": self.user_agent,
            "X-GitHub-Api-Version": "2022-11-28",
        }
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            request = Request(url, headers=headers)

            try:
                response = urlopen(request, timeout=self.timeout_seconds)
                self._sleep_between_requests()
                return response
            except HTTPError as error:
                if error.code == 403:
                    remaining = error.headers.get("X-RateLimit-Remaining")
                    reset_at = error.headers.get("X-RateLimit-Reset")
                    if remaining == "0" and reset_at:
                        self._sleep_until_reset(reset_at)
                        continue

                if error.code >= 500 and attempt < self.max_retries:
                    self._sleep_before_retry(attempt, url, error)
                    last_error = error
                    continue

                raise
            except (URLError, TimeoutError, socket.timeout) as error:
                last_error = error
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt, url, error)
                    continue
                raise

        if last_error is not None:
            raise last_error

        raise RuntimeError(f"Request failed unexpectedly for {url}")

    @staticmethod
    def _sleep_until_reset(reset_at: str) -> None:
        wait_seconds = max(int(reset_at) - int(time.time()) + 1, 1)
        LOGGER.warning("GitHub rate limit hit, sleeping for %s seconds", wait_seconds)
        time.sleep(wait_seconds)

    def _sleep_before_retry(self, attempt: int, url: str, error: Exception) -> None:
        wait_seconds = self.retry_backoff_seconds * attempt
        LOGGER.warning(
            "Request failed for %s (attempt %s/%s): %s. Retrying in %s seconds",
            url,
            attempt,
            self.max_retries,
            error,
            wait_seconds,
        )
        time.sleep(wait_seconds)

    def _sleep_between_requests(self) -> None:
        if self.request_delay_seconds > 0:
            time.sleep(self.request_delay_seconds)


def read_env_file(env_file: Path) -> dict[str, str]:
    values: dict[str, str] = {}

    if not env_file.exists():
        return values

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")

    return values


def resolve_github_token(env_file: Path) -> str:
    env_values = read_env_file(env_file)
    token = os.getenv("GITHUB_TOKEN") or env_values.get("GITHUB_TOKEN", "")

    if not token:
        raise RuntimeError(
            f"GITHUB_TOKEN not found. Set it in the environment or in {env_file}."
        )

    return token


def build_search_url(query: str, per_page: int, page: int) -> str:
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc",
        "per_page": per_page,
        "page": page,
    }
    return f"{SEARCH_REPOS_ENDPOINT}?{urlencode(params)}"


def build_star_range_queries() -> list[str]:
    return [
        "archived:false fork:false stars:>50000",
        "archived:false fork:false stars:10000..50000",
        "archived:false fork:false stars:5000..9999",
        "archived:false fork:false stars:1000..4999",
        "archived:false fork:false stars:300..999",
    ]


def fetch_repositories(client: GitHubClient, limit: int) -> list[dict[str, Any]]:
    repositories: list[dict[str, Any]] = []
    seen_repositories: set[str] = set()
    queries = build_star_range_queries()

    for query in queries:
        page = 1
        while len(repositories) < limit and page <= 10:
            per_page = min(limit - len(repositories), 100)
            if per_page <= 0:
                break

            search_url = build_search_url(query, per_page, page)
            payload = client.request_json(search_url)
            items = payload.get("items", [])

            if not items:
                break

            for item in items:
                full_name = item["full_name"]
                if full_name in seen_repositories:
                    continue

                repositories.append(
                    {
                        "full_name": full_name,
                        "stars": item["stargazers_count"],
                        "language": item.get("language"),
                        "created_at": item.get("created_at"),
                        "pushed_at": item.get("pushed_at"),
                        "size": item.get("size"),
                        "forks": item.get("forks_count"),
                        "default_branch": item.get("default_branch"),
                        "html_url": item.get("html_url"),
                    }
                )
                seen_repositories.add(full_name)

                if len(repositories) >= limit:
                    break

            if len(items) < per_page:
                break

            page += 1

        if len(repositories) >= limit:
            break

    return repositories


def write_repositories(repositories: list[dict[str, Any]], repos_file: Path) -> None:
    repos_file.parent.mkdir(parents=True, exist_ok=True)

    with repos_file.open("w", encoding="utf-8", newline="\n") as handle:
        for repository in repositories:
            handle.write(json.dumps(repository, ensure_ascii=False) + "\n")


def list_workflows(
    client: GitHubClient,
    owner: str,
    repo: str,
) -> list[dict[str, Any]]:
    workflows: list[dict[str, Any]] = []
    page = 1

    while True:
        params = urlencode({"per_page": 100, "page": page})
        url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/actions/workflows?{params}"

        try:
            payload = client.request_json(url)
        except HTTPError as error:
            if error.code == 404:
                LOGGER.info("No workflows found for %s/%s", owner, repo)
                return []
            raise
        except (URLError, TimeoutError, socket.timeout) as error:
            LOGGER.warning(
                "Failed to list workflows for %s/%s: %s",
                owner,
                repo,
                error,
            )
            return workflows

        page_items = payload.get("workflows", [])
        workflows.extend(page_items)

        if len(page_items) < 100:
            break

        page += 1

    return workflows


def is_workflow_file(path: str) -> bool:
    lowered = path.lower()
    return lowered.endswith(".yml") or lowered.endswith(".yaml")


def fetch_workflow_content(
    client: GitHubClient,
    owner: str,
    repo: str,
    workflow_path: str,
    ref: str,
) -> str:
    encoded_path = quote(workflow_path, safe="/")
    params = urlencode({"ref": ref}) if ref else ""
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{encoded_path}"
    if params:
        url = f"{url}?{params}"
    payload = client.request_json(url)

    if payload.get("encoding") == "base64":
        raw_content = payload.get("content", "")
        return base64.b64decode(raw_content).decode("utf-8", errors="replace")

    download_url = payload.get("download_url")
    if download_url:
        return client.request_text(download_url)

    raise ValueError(
        f"GitHub contents response for {owner}/{repo}:{workflow_path} had no decodable content"
    )


def build_workflow_metadata(workflow: dict[str, Any]) -> dict[str, str]:
    updated_at = workflow.get("updated_at", "")
    workflow_id = str(workflow.get("id", ""))
    workflow_state = str(workflow.get("state", ""))
    workflow_name = str(workflow.get("name", ""))

    return {
        "last_commit_date": updated_at,
        "sha": "",
        "workflow_id": workflow_id,
        "workflow_state": workflow_state,
        "workflow_name": workflow_name,
    }


def fetch_workflow_metadata(
    workflow: dict[str, Any],
) -> dict[str, str]:
    return build_workflow_metadata(workflow)


def write_workflow_file(
    workflows_dir: Path,
    owner: str,
    repo: str,
    workflow_path: str,
    content: str,
    metadata: dict[str, str],
) -> None:
    destination = workflows_dir / owner / repo / Path(workflow_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8", newline="\n")

    meta_path = destination.with_suffix(destination.suffix + ".meta.json")
    meta_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def fetch_workflows_for_repository(
    client: GitHubClient,
    repository: dict[str, Any],
    workflows_dir: Path,
) -> int:
    owner, repo = repository["full_name"].split("/", 1)
    workflows = list_workflows(client, owner, repo)
    default_branch = str(repository.get("default_branch", ""))
    workflow_count = 0

    for workflow in workflows:
        workflow_path = workflow.get("path", "")
        workflow_state = str(workflow.get("state", ""))
        if not is_workflow_file(workflow_path):
            continue

        if not workflow_path:
            continue

        if workflow_state and workflow_state.lower() != "active":
            LOGGER.debug(
                "Skipping %s/%s:%s because workflow state is %s",
                owner,
                repo,
                workflow_path,
                workflow_state,
            )
            continue

        try:
            content = fetch_workflow_content(
                client,
                owner,
                repo,
                workflow_path,
                default_branch,
            )
            metadata = fetch_workflow_metadata(workflow)
            write_workflow_file(
                workflows_dir,
                owner,
                repo,
                workflow_path,
                content,
                metadata,
            )
            workflow_count += 1
        except HTTPError as error:
            if error.code == 404:
                LOGGER.info(
                    "Skipping %s/%s:%s because content was not found on default branch",
                    owner,
                    repo,
                    workflow_path,
                )
                continue
            LOGGER.warning(
                "Skipping %s/%s:%s because download failed: %s",
                owner,
                repo,
                workflow_path,
                error,
            )
        except (URLError, TimeoutError, socket.timeout, ValueError) as error:
            LOGGER.warning(
                "Skipping %s/%s:%s because download failed: %s",
                owner,
                repo,
                workflow_path,
                error,
            )

    return workflow_count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch a pilot sample of public GitHub repositories and workflows."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="How many repositories to fetch for the pilot run.",
    )
    parser.add_argument(
        "--repos-file",
        type=Path,
        default=DEFAULT_REPOS_FILE,
        help="Path to repos.jsonl output file.",
    )
    parser.add_argument(
        "--workflows-dir",
        type=Path,
        default=DEFAULT_WORKFLOWS_DIR,
        help="Directory where workflow files will be stored.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="Path to .env file containing GITHUB_TOKEN.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level: DEBUG, INFO, WARNING, ERROR.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s: %(message)s",
    )

    token = resolve_github_token(args.env_file)
    client = GitHubClient(token=token)

    repositories = fetch_repositories(client, args.limit)
    write_repositories(repositories, args.repos_file)
    LOGGER.info("Saved %s repositories to %s", len(repositories), args.repos_file)

    total_workflows = 0
    for index, repository in enumerate(repositories, start=1):
        LOGGER.info(
            "[%s/%s] Fetching workflows for %s",
            index,
            len(repositories),
            repository["full_name"],
        )
        try:
            workflows_found = fetch_workflows_for_repository(
                client,
                repository,
                args.workflows_dir,
            )
        except (HTTPError, URLError, TimeoutError, socket.timeout) as error:
            LOGGER.warning(
                "[%s/%s] Skipping %s because repository fetch failed: %s",
                index,
                len(repositories),
                repository["full_name"],
                error,
            )
            continue
        total_workflows += workflows_found
        LOGGER.info(
            "[%s/%s] Saved %s workflow files for %s",
            index,
            len(repositories),
            workflows_found,
            repository["full_name"],
        )

    LOGGER.info("Finished pilot fetch. Total workflow files saved: %s", total_workflows)


if __name__ == "__main__":
    main()
