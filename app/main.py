"""FastAPI webhook server for the Grapple GitHub App.

Reads env: GITHUB_APP_ID, GITHUB_PRIVATE_KEY_BASE64, GITHUB_WEBHOOK_SECRET.
Run locally: uvicorn app.main:app --reload --port 8000
"""

from __future__ import annotations

import logging
import os
from base64 import b64decode
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from app.github import GitHubAppClient
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

    # TODO(phase-2): dispatch push/pull_request → analyzer → check run / SARIF
    return JSONResponse({"accepted": True}, status_code=202)
