# Grapple

**Snyk for GitHub Actions.** Grapple detects unpinned and compromised Actions in your CI/CD workflows before they become a supply chain attack.

## The problem

Most repositories reference GitHub Actions by tag (`@v4`) or branch (`@main`). Tags can be overwritten; a full SHA pin (`@abc123...`) is the only reference that cannot be tampered with.

## What Grapple does

- Scans every `uses:` in workflow files and classifies the reference: `sha` / `tag` / `branch` / `local` / `docker`
- Fetches the incidents database from [`Grapple-db`](https://github.com/excla1mmm/Grapple-db) and flags affected actions as high-risk
- Detects unpinned reusable workflows (`jobs.<id>.uses`)
- Analyzes `permissions:` blocks for overly broad scopes
- Escalates severity for jobs running on `self-hosted` runners
- Walks composite action dependencies up to 3 levels deep (transitive analysis)
- Generates a Trust Score (0-100) for each external action

## Planned delivery

| Component | Status |
|---|---|
| `core/classify.py` - shared classification engine | Done |
| `research/` - dataset collection + paper tables | Done |
| GitHub App - Check Runs, auto-fix PRs, SARIF output | Planned |
| Web Registry - public trust cards for every action | Planned |

## Repository structure

```text
grapple/
├── core/classify.py   # shared engine: used by app, web, and research
└── research/
    ├── fetch.py       # downloads workflows for the dataset
    └── stats.py       # generates tables for the paper
```

## Running the research pipeline

```bash
git clone git@github.com:excla1mmm/Grapple.git
cd Grapple
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # add your GITHUB_TOKEN

python -m research.fetch
python -m core.classify
python -m research.stats
```

`core.classify` downloads incidents from:

```text
https://raw.githubusercontent.com/excla1mmm/Grapple-db/main/database.yml
```

You can override the source with:

```bash
python -m core.classify --incidents-url https://raw.githubusercontent.com/excla1mmm/Grapple-db/main/database.yml
```

## Related

- [Grapple-db](https://github.com/excla1mmm/Grapple-db) - community-maintained incidents database
