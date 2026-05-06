"""FastAPI webhook server for the Grapple GitHub App.

Reads env: GITHUB_APP_ID, GITHUB_PRIVATE_KEY_BASE64, GITHUB_WEBHOOK_SECRET.
Run locally: uvicorn app.main:app --reload --port 8000
"""

from __future__ import annotations

import json
import logging
import os
from base64 import b64decode
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from app.analyzer import Finding, analyze_workflow
from app.check_run import annotations_for, conclusion_for, summary_for, title_for
from app.fix_plan import build_suggested_fixes
from app.github import GitHubAppClient
from app.sarif import compress_sarif, findings_to_sarif
from core.classify import get_high_risk_actions

LOGGER = logging.getLogger(__name__)


def _required_env(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    private_key = b64decode(_required_env("GITHUB_PRIVATE_KEY_BASE64")).decode("utf-8")
    client = GitHubAppClient(
        app_id=_required_env("GITHUB_APP_ID"),
        private_key=private_key,
        webhook_secret=_required_env("GITHUB_WEBHOOK_SECRET"),
    )
    app.state.github = client
    app.state.high_risk_actions = get_high_risk_actions()
    LOGGER.info(
        "Grapple App ready (high_risk_actions=%d)",
        len(app.state.high_risk_actions),
    )
    try:
        yield
    finally:
        await client.close()


app = FastAPI(lifespan=lifespan, title="Grapple Security")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@dataclass(frozen=True)
class EventTarget:
    owner: str
    repo: str
    content_ref: str
    sarif_ref: str
    head_sha: str
    installation_id: int


@app.post("/webhook")
async def webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hub_signature_256: str | None = Header(None),
    x_github_event: str | None = Header(None),
    x_github_delivery: str | None = Header(None),
) -> JSONResponse:
    body = await request.body()
    client: GitHubAppClient = request.app.state.github

    if not client.verify_webhook_signature(body, x_hub_signature_256):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    LOGGER.info(
        "Webhook accepted: event=%s delivery=%s bytes=%d",
        x_github_event,
        x_github_delivery,
        len(body),
    )

    if x_github_event in ("push", "pull_request"):
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Malformed JSON payload")
        background_tasks.add_task(
            process_event,
            client,
            request.app.state.high_risk_actions,
            x_github_event,
            payload,
        )

    return JSONResponse({"accepted": True}, status_code=202)


def extract_event_target(
    event: str, payload: dict
) -> EventTarget | None:
    """Return the repository/commit to scan, or None if not actionable."""
    installation_id = (payload.get("installation") or {}).get("id")
    if not installation_id:
        return None

    full_name = (payload.get("repository") or {}).get("full_name") or ""
    if "/" not in full_name:
        return None
    owner, name = full_name.split("/", 1)

    if event == "push":
        head_sha = payload.get("after") or ""
        ref = payload.get("ref") or ""
        content_ref = (
            ref.removeprefix("refs/heads/") if ref.startswith("refs/heads/") else ref
        )
        sarif_ref = ref
    elif event == "pull_request":
        if payload.get("action") not in ("opened", "synchronize", "reopened"):
            return None
        pr_number = payload.get("number")
        head = (payload.get("pull_request") or {}).get("head") or {}
        head_sha = head.get("sha") or ""
        # For fork PRs, head.ref is a branch in the fork, not in the base repo.
        # The head commit SHA is reachable through the base repository's PR ref
        # and can be used directly by the contents API.
        content_ref = head_sha
        sarif_ref = f"refs/pull/{pr_number}/head" if pr_number else ""
    else:
        return None

    if not (head_sha and content_ref and sarif_ref):
        return None
    return EventTarget(
        owner=owner,
        repo=name,
        content_ref=content_ref,
        sarif_ref=sarif_ref,
        head_sha=head_sha,
        installation_id=installation_id,
    )


async def process_event(
    client: GitHubAppClient,
    high_risk_actions: frozenset[str],
    event: str,
    payload: dict,
) -> None:
    """Fetch workflows for the event, analyze, and post a Check Run.

    Runs as a FastAPI BackgroundTask after the webhook has been acked.
    Errors are logged, never raised — webhook delivery has already succeeded.
    """
    target = extract_event_target(event, payload)
    if target is None:
        LOGGER.info("Skipping %s event — not actionable", event)
        return
    LOGGER.info(
        "Analyzing %s/%s @ %s (event=%s)",
        target.owner,
        target.repo,
        target.head_sha[:8],
        event,
    )

    try:
        token = await client.get_installation_token(target.installation_id)
        files = await client.get_workflow_files(
            target.owner,
            target.repo,
            target.content_ref,
            token,
        )
    except Exception:
        LOGGER.exception("Failed to fetch workflows for %s/%s", target.owner, target.repo)
        return

    findings: list[Finding] = []
    for f in files:
        findings.extend(analyze_workflow(f["content"], f["path"], high_risk_actions))

    suggested_fixes = await build_suggested_fixes(client, findings, token)

    try:
        check_run_id = await client.create_check_run(
            owner=target.owner,
            repo=target.repo,
            head_sha=target.head_sha,
            token=token,
            title=title_for(findings),
            summary=summary_for(findings, suggested_fixes=suggested_fixes),
            conclusion=conclusion_for(findings),
            annotations=annotations_for(findings),
        )
    except Exception:
        LOGGER.exception("Failed to create check run for %s/%s", target.owner, target.repo)
        return

    LOGGER.info(
        "Check run %d created for %s/%s (%d findings, %s)",
        check_run_id,
        target.owner,
        target.repo,
        len(findings),
        conclusion_for(findings),
    )

    actionable = [f for f in findings if f.severity != "safe"]
    if actionable:
        try:
            sarif_id = await client.upload_sarif(
                owner=target.owner,
                repo=target.repo,
                commit_sha=target.head_sha,
                ref=target.sarif_ref,
                sarif_gzip_b64=compress_sarif(findings_to_sarif(findings)),
                token=token,
            )
            LOGGER.info("SARIF uploaded for %s/%s (id=%s)", target.owner, target.repo, sarif_id)
        except Exception:
            LOGGER.warning(
                "SARIF upload failed for %s/%s (non-fatal)",
                target.owner,
                target.repo,
                exc_info=True,
            )
