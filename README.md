# Grapple

**Snyk for GitHub Actions.** Grapple detects unpinned and compromised Actions in your CI/CD workflows before they become a supply chain attack.

## The problem

Most repositories reference GitHub Actions by tag (`@v4`) or branch (`@main`). Tags can be overwritten — in March 2025, `tj-actions/changed-files` was compromised this way, leaking secrets from ~23,000 repositories. A full SHA pin (`@abc123...`) is the only reference that cannot be tampered with.

## What Grapple does

- Scans every `uses:` in your workflow files and classifies the reference: `sha` / `tag` / `branch` / `local` / `docker`
- Flags actions from the [incidents database](https://github.com/excla1mmm/grapple-db) as high-risk
- Detects unpinned reusable workflows (`jobs.<id>.uses`)
- Analyzes `permissions:` blocks for overly broad scopes
- Escalates severity for jobs running on `self-hosted` runners
- Walks composite action dependencies up to 3 levels deep (transitive analysis)
- Generates a Trust Score (0–100) for each external action

## Planned delivery

| Component | Status |
|---|---|
| `core/classify.py` — shared classification engine | Done |
| `research/` — dataset collection + paper tables | Done |
| GitHub App — Check Runs, auto-fix PRs, SARIF output | Planned |
| Web Registry — public trust cards for every action | Planned |

## Repository structure

```
grapple/
├── core/classify.py     # shared engine: used by app/, web/, and research/
├── research/fetch.py    # batch script: downloads workflows for the dataset
├── research/stats.py    # batch script: generates tables for the paper
└── incidents/           # git submodule → grapple-db (incidents database)
```

## Running the research pipeline

```bash
git clone --recurse-submodules git@github.com:excla1mmm/Grapple.git
cd Grapple
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # add your GITHUB_TOKEN

python -m research.fetch        # step 1: collect workflows
python -m core.classify         # step 2: classify uses:
python -m research.stats        # step 3: generate tables
```

## Related

- [grapple-db](https://github.com/excla1mmm/grapple-db) — community-maintained incidents database
