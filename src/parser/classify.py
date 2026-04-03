from __future__ import annotations

import argparse
import csv
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - optional dependency until requirements are set
    yaml = None


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = ROOT_DIR / "data" / "raw" / "workflows"
DEFAULT_OUTPUT_FILE = ROOT_DIR / "data" / "processed" / "actions.csv"

USES_LINE_PATTERN = re.compile(
    r"^\s*-?\s*uses:\s*[\"']?(?P<value>[^\"'#\r\n]+?)[\"']?\s*(?:#.*)?$",
    re.IGNORECASE,
)
SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{40}$")
TAG_PATTERN = re.compile(
    r"^(v?\d+([.-]\d+)*|latest|stable|release[-._]?\d*|v?\d+x)$",
    re.IGNORECASE,
)
BRANCH_HINT_PATTERN = re.compile(
    r"^(main|master|develop|development|dev|trunk|next|feature/.+|feat/.+|fix/.+|hotfix/.+|release/.+)$",
    re.IGNORECASE,
)

HIGH_RISK = {
    "aquasecurity/trivy-action",
    "tj-actions/changed-files",
    "reviewdog/action-misspell",
    "reviewdog/action-actionlint",
    "step-security/harden-runner",
}

LOGGER = logging.getLogger(__name__)


def fallback_extract_uses_lines(workflow_text: str) -> list[str]:
    values: list[str] = []

    for line in workflow_text.splitlines():
        match = USES_LINE_PATTERN.match(line)
        if match:
            values.append(match.group("value").strip())

    return values


def extract_uses_from_node(node: Any) -> list[str]:
    values: list[str] = []

    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str) and key.lower() == "uses" and isinstance(value, str):
                values.append(value.strip())

            values.extend(extract_uses_from_node(value))

    elif isinstance(node, list):
        for item in node:
            values.extend(extract_uses_from_node(item))

    return values


def extract_uses_lines(workflow_text: str, workflow_path: Path) -> list[str]:
    if yaml is None:
        LOGGER.warning(
            "PyYAML is not installed, using text fallback for %s",
            workflow_path,
        )
        return fallback_extract_uses_lines(workflow_text)

    try:
        documents = yaml.safe_load_all(workflow_text)
        values: list[str] = []

        for document in documents:
            values.extend(extract_uses_from_node(document))

        return values
    except yaml.YAMLError as error:
        LOGGER.warning(
            "Failed to parse YAML for %s, using text fallback: %s",
            workflow_path,
            error,
        )
        return fallback_extract_uses_lines(workflow_text)


def normalize_action_name(uses_value: str) -> str:
    if uses_value.startswith("./"):
        return uses_value

    if uses_value.startswith("docker://"):
        return "docker"

    if "@" not in uses_value:
        return uses_value

    action_path, _ = uses_value.rsplit("@", 1)
    parts = action_path.split("/")

    if len(parts) >= 2:
        return "/".join(parts[:2])

    return action_path


def classify_ref(ref: str) -> str:
    normalized_ref = ref.strip()

    if SHA_PATTERN.fullmatch(normalized_ref):
        return "sha"

    if normalized_ref.startswith("refs/heads/"):
        return "branch"

    if normalized_ref.startswith("refs/tags/"):
        return "tag"

    if BRANCH_HINT_PATTERN.fullmatch(normalized_ref):
        return "branch"

    if TAG_PATTERN.fullmatch(normalized_ref):
        return "tag"

    if "/" in normalized_ref:
        return "branch"

    return "unknown"


def classify_uses(uses_value: str) -> dict[str, object]:
    if uses_value.startswith("./"):
        return {
            "uses_raw": uses_value,
            "action_name": uses_value,
            "ref": "",
            "pin_type": "local",
            "is_pinned": False,
            "is_high_risk": False,
        }

    if uses_value.startswith("docker://"):
        return {
            "uses_raw": uses_value,
            "action_name": "docker",
            "ref": uses_value.removeprefix("docker://"),
            "pin_type": "docker",
            "is_pinned": False,
            "is_high_risk": False,
        }

    action_name = normalize_action_name(uses_value)

    if "@" not in uses_value:
        return {
            "uses_raw": uses_value,
            "action_name": action_name,
            "ref": "",
            "pin_type": "unknown",
            "is_pinned": False,
            "is_high_risk": action_name in HIGH_RISK,
        }

    _, ref = uses_value.rsplit("@", 1)
    pin_type = classify_ref(ref)

    return {
        "uses_raw": uses_value,
        "action_name": action_name,
        "ref": ref,
        "pin_type": pin_type,
        "is_pinned": pin_type == "sha",
        "is_high_risk": action_name in HIGH_RISK,
    }


def workflow_files(input_dir: Path) -> list[Path]:
    files = list(input_dir.rglob("*.yml"))
    files.extend(input_dir.rglob("*.yaml"))
    return sorted(files)


def repo_from_path(workflow_path: Path, input_dir: Path) -> tuple[str, str]:
    relative_path = workflow_path.relative_to(input_dir)
    parts = relative_path.parts

    if len(parts) < 3:
        raise ValueError(
            f"Workflow path must look like owner/repo/file.yml, got: {relative_path}"
        )

    repo = f"{parts[0]}/{parts[1]}"
    workflow_file = "/".join(parts[2:])
    return repo, workflow_file


def workflow_last_modified(workflow_path: Path) -> str:
    modified_at = datetime.fromtimestamp(workflow_path.stat().st_mtime, tz=UTC)
    return modified_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def classify_workflows(input_dir: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    for workflow_path in workflow_files(input_dir):
        repo, workflow_file = repo_from_path(workflow_path, input_dir)
        workflow_text = workflow_path.read_text(encoding="utf-8", errors="replace")
        uses_values = extract_uses_lines(workflow_text, workflow_path)
        last_modified = workflow_last_modified(workflow_path)

        for uses_value in uses_values:
            row = classify_uses(uses_value)
            row["repo"] = repo
            row["workflow_file"] = workflow_file
            row["workflow_last_modified"] = last_modified
            rows.append(row)

    return rows


def write_csv(rows: list[dict[str, object]], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "repo",
        "workflow_file",
        "uses_raw",
        "action_name",
        "ref",
        "pin_type",
        "is_pinned",
        "is_high_risk",
        "workflow_last_modified",
    ]

    with output_file.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Classify GitHub Actions uses statements in workflow files."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directory with workflow files in owner/repo/file.yml layout.",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help="CSV file where classification results will be written.",
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

    rows = classify_workflows(args.input_dir)
    write_csv(rows, args.output_file)

    LOGGER.info("Processed %s uses entries", len(rows))
    LOGGER.info("Saved results to: %s", args.output_file)


if __name__ == "__main__":
    main()
