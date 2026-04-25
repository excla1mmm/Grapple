# Grapple

Detects unpinned and compromised GitHub Actions in your CI/CD workflows before they become a supply chain attack.

## The problem

Most repositories reference GitHub Actions by tag (`@v4`) or branch (`@main`). Tags can be overwritten; a full SHA pin (`@abc123...`) is the only reference that cannot be tampered with. In March 2025, `tj-actions/changed-files` was compromised this way, leaking secrets from ~23,000 repositories.

## What Grapple does

For every workflow in `.github/workflows/`, Grapple flags:

- `uses:` references that aren't SHA-pinned (tag, branch, or default-branch)
- Actions listed in the [incidents database](https://github.com/excla1mmm/Grapple-db), including tags from before the incident
- Reusable workflows pinned to a branch or tag (`jobs.<id>.uses`) — these run entire jobs under remote control with all inherited secrets
- Overly broad `permissions:` scopes (`id-token: write`, `contents: write`, `permissions: write-all`, missing top-level block)
- Jobs running on `self-hosted` runners — severity of unpinned actions in those jobs is bumped one level

Output is a GitHub Check Run with findings grouped by severity (`critical` / `high` / `medium` / `info` / `safe`). When any critical finding is present, the run conclusion is `failure`.

## Status

| Component | State |
|---|---|
| `core/classify.py` — shared classification engine | Done |
| `research/` — dataset collection + paper tables | Done |
| `app/` — GitHub App webhook server, analyzer, Check Runs | Done |
| Reusable workflows, permissions, self-hosted detection | Done |
| SARIF upload to Code Scanning | Planned |
| Auto-fix PRs (rewrite `@v4` to `@<sha>`) | Planned |
| Transitive composite-action analysis | Planned |
| Trust Score per Action + Web Registry | Planned |

## Repository structure

```text
grapple/
├── core/classify.py     # shared engine: used by app and research
├── app/
│   ├── main.py          # FastAPI webhook + lifespan + dispatch
│   ├── github.py        # GitHub App client (JWT, tokens, API)
│   ├── analyzer.py      # uses, permissions, self-hosted analysis
│   └── check_run.py     # Check Run formatting (severity, summary)
├── research/
│   ├── fetch.py         # downloads workflows for the dataset
│   └── stats.py         # generates tables for the paper
└── incidents/           # git submodule → grapple-db
```

## Setup

```bash
git clone --recurse-submodules git@github.com:excla1mmm/Grapple.git
cd Grapple
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill `.env` with the values you need:

- `GITHUB_TOKEN` — Personal Access Token with `public_repo` scope (for the research pipeline)
- `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY_BASE64`, `GITHUB_WEBHOOK_SECRET` — for the GitHub App

## Running the GitHub App

You'll need a [registered GitHub App](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app) with `Contents: read` and `Checks: write` repository permissions, subscribed to `push` and `pull_request` events.

```bash
# Public webhook tunnel for local dev (recommend smee.io):
smee --url https://smee.io/<your-channel> --target http://localhost:8000/webhook

# In a separate terminal:
set -a; source .env; set +a
uvicorn app.main:app --port 8000 --reload
```

Push to a repository where the App is installed; a Check Run will appear on the commit, grouping findings by severity.

## Running the research pipeline

```bash
python -m research.fetch        # collect workflows
python -m core.classify         # classify uses → research/data/processed/actions.csv
python -m research.stats        # generate paper tables
```

`core.classify` reads incidents from the `incidents/` submodule by default, falling back to:

```text
https://raw.githubusercontent.com/excla1mmm/Grapple-db/main/database.yml
```

Override either source via `--incidents-file PATH` or `--incidents-url URL`.

## Related

- [Grapple-db](https://github.com/excla1mmm/Grapple-db) — community-maintained incidents database
