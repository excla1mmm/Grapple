"""Resolve mutable GitHub Actions references into read-only suggested fixes."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from app.analyzer import Finding

LOGGER = logging.getLogger(__name__)

_SEVERITY_RANK = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "info": 3,
    "safe": 4,
}


class RefResolver(Protocol):
    async def get_default_branch(self, owner: str, repo: str, token: str) -> str: ...

    async def resolve_tag_to_sha(
        self,
        owner: str,
        repo: str,
        tag: str,
        token: str,
    ) -> str | None: ...

    async def resolve_branch_to_sha(
        self,
        owner: str,
        repo: str,
        branch: str,
        token: str,
    ) -> str | None: ...


@dataclass(frozen=True)
class SuggestedFix:
    workflow_file: str
    line_number: int | None
    current: str
    suggested: str
    resolved_sha: str


@dataclass(frozen=True)
class UsesRef:
    owner: str
    repo: str
    ref: str
    current: str
    prefix: str


def parse_uses_ref(finding: Finding) -> UsesRef | None:
    if finding.kind not in ("action", "reusable_workflow"):
        return None
    if finding.pin_type not in ("tag", "branch"):
        return None

    current = finding.uses_raw or _fallback_uses(finding)
    if current.startswith(("./", "docker://")):
        return None

    if "@" in current:
        prefix, raw_ref = current.rsplit("@", 1)
    else:
        prefix, raw_ref = current, ""

    parts = prefix.split("/")
    if len(parts) < 2:
        return None

    return UsesRef(
        owner=parts[0],
        repo=parts[1],
        ref=_normalize_ref(raw_ref or finding.ref),
        current=current,
        prefix=prefix,
    )


async def build_suggested_fixes(
    resolver: RefResolver,
    findings: list[Finding],
    token: str,
) -> list[SuggestedFix]:
    fixes: list[SuggestedFix] = []
    cache: dict[tuple[str, str, str, str], str | None] = {}

    for finding in sorted(findings, key=lambda f: _SEVERITY_RANK.get(f.severity, 99)):
        uses_ref = parse_uses_ref(finding)
        if uses_ref is None:
            continue

        try:
            resolved_sha = await _resolve_sha(resolver, finding, uses_ref, token, cache)
        except Exception:
            LOGGER.warning(
                "Failed to resolve suggested fix for %s in %s",
                uses_ref.current,
                finding.workflow_file,
                exc_info=True,
            )
            continue

        if not resolved_sha:
            continue

        fixes.append(SuggestedFix(
            workflow_file=finding.workflow_file,
            line_number=finding.line_number,
            current=uses_ref.current,
            suggested=f"{uses_ref.prefix}@{resolved_sha}",
            resolved_sha=resolved_sha,
        ))

    return fixes


async def _resolve_sha(
    resolver: RefResolver,
    finding: Finding,
    uses_ref: UsesRef,
    token: str,
    cache: dict[tuple[str, str, str, str], str | None],
) -> str | None:
    ref = uses_ref.ref
    if finding.pin_type == "branch" and not ref:
        ref = await resolver.get_default_branch(uses_ref.owner, uses_ref.repo, token)
        ref = _normalize_ref(ref)
    if not ref:
        return None

    cache_key = (uses_ref.owner, uses_ref.repo, finding.pin_type, ref)
    if cache_key in cache:
        return cache[cache_key]

    if finding.pin_type == "tag":
        resolved_sha = await resolver.resolve_tag_to_sha(
            uses_ref.owner,
            uses_ref.repo,
            ref,
            token,
        )
    elif finding.pin_type == "branch":
        resolved_sha = await resolver.resolve_branch_to_sha(
            uses_ref.owner,
            uses_ref.repo,
            ref,
            token,
        )
    else:
        resolved_sha = None

    cache[cache_key] = resolved_sha
    return resolved_sha


def _fallback_uses(finding: Finding) -> str:
    if finding.ref:
        return f"{finding.action_name}@{finding.ref}"
    return finding.action_name


def _normalize_ref(ref: str) -> str:
    if ref.startswith("refs/heads/"):
        return ref.removeprefix("refs/heads/")
    if ref.startswith("refs/tags/"):
        return ref.removeprefix("refs/tags/")
    return ref
