from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_ACTIONS_FILE = ROOT_DIR / "data" / "processed" / "actions.csv"
DEFAULT_REPOS_FILE = ROOT_DIR / "data" / "raw" / "repos.jsonl"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "tables"


def load_actions(actions_file: Path) -> list[dict[str, str]]:
    with actions_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def load_repositories(repos_file: Path) -> list[dict[str, object]]:
    if not repos_file.exists():
        return []

    repositories: list[dict[str, object]] = []
    with repos_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            repositories.append(json.loads(line))

    return repositories


def is_true(value: str) -> bool:
    return value.strip().lower() == "true"


def percentage(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0.00%"
    return f"{(numerator / denominator) * 100:.2f}%"


def summarize(actions: list[dict[str, str]], repositories: list[dict[str, object]]) -> list[dict[str, str]]:
    total_sampled_repos = len(repositories)
    repos_with_workflows = {row["repo"] for row in actions}

    external_actions = [
        row for row in actions if row["pin_type"] not in {"local", "docker"}
    ]
    pinned_external_actions = [
        row for row in external_actions if is_true(row["is_pinned"])
    ]
    unpinned_external_actions = [
        row for row in external_actions if not is_true(row["is_pinned"])
    ]

    repos_with_unpinned = {row["repo"] for row in unpinned_external_actions}
    high_risk_unpinned = [
        row
        for row in unpinned_external_actions
        if is_true(row["is_high_risk"])
    ]

    pin_type_counts = Counter(row["pin_type"] for row in actions)

    summary_rows = [
        {"metric": "sampled_repositories", "value": str(total_sampled_repos)},
        {"metric": "repositories_with_workflows", "value": str(len(repos_with_workflows))},
        {
            "metric": "repositories_with_unpinned_external_actions",
            "value": str(len(repos_with_unpinned)),
        },
        {
            "metric": "repositories_with_unpinned_external_actions_pct",
            "value": percentage(len(repos_with_unpinned), total_sampled_repos or len(repos_with_workflows)),
        },
        {"metric": "total_uses", "value": str(len(actions))},
        {"metric": "external_uses", "value": str(len(external_actions))},
        {"metric": "pinned_external_uses", "value": str(len(pinned_external_actions))},
        {"metric": "unpinned_external_uses", "value": str(len(unpinned_external_actions))},
        {
            "metric": "pinned_external_rate",
            "value": percentage(len(pinned_external_actions), len(external_actions)),
        },
        {"metric": "local_uses", "value": str(pin_type_counts.get("local", 0))},
        {"metric": "docker_uses", "value": str(pin_type_counts.get("docker", 0))},
        {"metric": "sha_uses", "value": str(pin_type_counts.get("sha", 0))},
        {"metric": "tag_uses", "value": str(pin_type_counts.get("tag", 0))},
        {"metric": "branch_uses", "value": str(pin_type_counts.get("branch", 0))},
        {"metric": "unknown_uses", "value": str(pin_type_counts.get("unknown", 0))},
        {"metric": "high_risk_unpinned_uses", "value": str(len(high_risk_unpinned))},
    ]

    return summary_rows


def top_unpinned_actions(
    actions: list[dict[str, str]],
    limit: int,
) -> list[dict[str, str]]:
    counts = Counter(
        row["action_name"]
        for row in actions
        if row["pin_type"] not in {"local", "docker"} and not is_true(row["is_pinned"])
    )

    rows: list[dict[str, str]] = []
    for action_name, count in counts.most_common(limit):
        sample_row = next(row for row in actions if row["action_name"] == action_name)
        rows.append(
            {
                "action_name": action_name,
                "count": str(count),
                "is_high_risk": sample_row["is_high_risk"],
            }
        )

    return rows


def high_risk_unpinned_actions(actions: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = [
        row
        for row in actions
        if row["pin_type"] not in {"local", "docker"}
        and not is_true(row["is_pinned"])
        and is_true(row["is_high_risk"])
    ]

    return sorted(
        rows,
        key=lambda row: (row["action_name"], row["repo"], row["workflow_file"], row["uses_raw"]),
    )


def write_csv(rows: list[dict[str, str]], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        output_file.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with output_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_summary(summary_rows: list[dict[str, str]]) -> None:
    print("Summary")
    for row in summary_rows:
        print(f"- {row['metric']}: {row['value']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute summary statistics from classified GitHub Actions data."
    )
    parser.add_argument(
        "--actions-file",
        type=Path,
        default=DEFAULT_ACTIONS_FILE,
        help="Path to actions.csv produced by src.parser.classify.",
    )
    parser.add_argument(
        "--repos-file",
        type=Path,
        default=DEFAULT_REPOS_FILE,
        help="Path to repos.jsonl produced by src.collector.fetch.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where summary tables will be written.",
    )
    parser.add_argument(
        "--top-limit",
        type=int,
        default=10,
        help="How many top unpinned actions to keep.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    actions = load_actions(args.actions_file)
    repositories = load_repositories(args.repos_file)

    summary_rows = summarize(actions, repositories)
    top_rows = top_unpinned_actions(actions, args.top_limit)
    high_risk_rows = high_risk_unpinned_actions(actions)

    write_csv(summary_rows, args.output_dir / "table1_summary.csv")
    write_csv(top_rows, args.output_dir / "table2_top_unpinned_actions.csv")
    write_csv(high_risk_rows, args.output_dir / "table3_high_risk_unpinned.csv")

    print_summary(summary_rows)
    print(f"Saved summary tables to: {args.output_dir}")


if __name__ == "__main__":
    main()
