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
from typing import AsyncIterator

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from app.analyzer import Finding, analyze_workflow
from app.check_run import conclusion_for, summary_for, title_for
from app.github import GitHubAppClient
from app.sarif import compress_sarif, findings_to_sarif
from core.classify import get_high_risk_actions

LOGGER = logging.getLogger(__name__)


def _required_env(name: str) -> str:
    value = os.environ.get(name)
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
) -> tuple[str, str, str, str, int] | None:
    """Return (owner, repo, ref, head_sha, installation_id) or None if not actionable."""
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
        ref_name = ref.removeprefix("refs/heads/") if ref.startswith("refs/heads/") else ref
    elif event == "pull_request":
        if payload.get("action") not in ("opened", "synchronize", "reopened"):
            return None
        head = (payload.get("pull_request") or {}).get("head") or {}
        head_sha = head.get("sha") or ""
        ref_name = head.get("ref") or ""
    else:
        return None

    if not (head_sha and ref_name):
        return None
    return owner, name, ref_name, head_sha, installation_id


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
    owner, repo, ref, head_sha, installation_id = target
    LOGGER.info("Analyzing %s/%s @ %s (event=%s)", owner, repo, head_sha[:8], event)

    try:
        token = await client.get_installation_token(installation_id)
        files = await client.get_workflow_files(owner, repo, ref, token)
    except Exception:
        LOGGER.exception("Failed to fetch workflows for %s/%s", owner, repo)
        return

    findings: list[Finding] = []
    for f in files:
        findings.extend(analyze_workflow(f["content"], f["path"], high_risk_actions))

    try:
        check_run_id = await client.create_check_run(
            owner=owner,
            repo=repo,
            head_sha=head_sha,
            token=token,
            title=title_for(findings),
            summary=summary_for(findings),
            conclusion=conclusion_for(findings),
        )
    except Exception:
        LOGGER.exception("Failed to create check run for %s/%s", owner, repo)
        return

    LOGGER.info(
        "Check run %d created for %s/%s (%d findings, %s)",
        check_run_id,
        owner,
        repo,
        len(findings),
        conclusion_for(findings),
    )

    actionable = [f for f in findings if f.severity != "safe"]
    if actionable:
        try:
            sarif_id = await client.upload_sarif(
                owner=owner,
                repo=repo,
                commit_sha=head_sha,
                ref=f"refs/heads/{ref}",
                sarif_gzip_b64=compress_sarif(findings_to_sarif(findings)),
                token=token,
            )
            LOGGER.info("SARIF uploaded for %s/%s (id=%s)", owner, repo, sarif_id)
        except Exception:
            LOGGER.warning("SARIF upload failed for %s/%s (non-fatal)", owner, repo, exc_info=True)
