# NHL EDGE — Team Skating Distance · GitHub Pages

Daily team-level skating distance dashboard powered by the **NHL EDGE API**, auto-deployed to **GitHub Pages** via GitHub Actions.

---

## Live site

```
https://<your-github-username>.github.io/<repo-name>/
```

`index.html` auto-redirects to the latest daily report.  
`latest.html` always shows the most recent run.  
`archive/YYYY-MM-DD.html` preserves every past run.

---

## Repository layout

```
.
├── .github/
│   └── workflows/
│       └── nhl-edge-daily.yml     ← GitHub Actions workflow
├── docs/                          ← GitHub Pages source (set in repo Settings)
│   ├── .nojekyll                  ← Tells Pages to skip Jekyll
│   ├── index.html                 ← Auto-generated archive index / redirect
│   ├── latest.html                ← Most recent report (overwritten daily)
│   └── archive/
│       └── YYYY-MM-DD.html        ← Immutable daily snapshots
├── data/
│   └── edge_skating_distance/
│       ├── latest_wide.csv
│       ├── latest_long.csv
│       ├── team_skating_distance_detail_wide_YYYY-MM-DD.csv
│       ├── team_skating_distance_detail_long_YYYY-MM-DD.csv
│       └── raw/
│           └── YYYY-MM-DD/
│               └── *.json         ← Raw API responses per team
├── scripts/
│   └── build_archive_index.py     ← Generates docs/index.html
└── nhl_edge_skating_distance.py   ← Main scraper
```

---

## One-time setup

### 1 — Enable GitHub Pages

1. Go to your repo → **Settings** → **Pages**.
2. Under **Build and deployment**, set **Source** → `GitHub Actions`.
3. Save.

### 2 — Enable workflow write permissions

1. Go to **Settings** → **Actions** → **General**.
2. Under **Workflow permissions**, select **Read and write permissions**.
3. Check **Allow GitHub Actions to create and approve pull requests** (optional but handy).
4. Save.

### 3 — First run

Either push a commit (which won't trigger the scheduled workflow) or manually trigger it:

1. Go to **Actions** → **NHL EDGE — Daily Skating Distance**.
2. Click **Run workflow** → optionally set a date → **Run workflow**.

The workflow will:
- Install dependencies (`requests`, `pandas`)
- Call the NHL EDGE API for all 32 teams
- Write CSVs + raw JSON to `data/`
- Write `docs/latest.html` + `docs/archive/YYYY-MM-DD.html`
- Regenerate `docs/index.html`
- Commit all changes back to the repo
- Deploy `docs/` to GitHub Pages

### 4 — Schedule

The workflow runs at **09:00 UTC** daily (≈ 5 AM ET). You can change the cron expression in `.github/workflows/nhl-edge-daily.yml`:

```yaml
schedule:
  - cron: "0 9 * * *"   # ← change this
```

---

## Manual / local run

```bash
pip install requests pandas
python nhl_edge_skating_distance.py          # uses today's date
python nhl_edge_skating_distance.py --date 2025-11-01   # backfill a specific date
python scripts/build_archive_index.py        # rebuild docs/index.html
```

---

## Data files

| File | Contents |
|------|----------|
| `latest_wide.csv` | One row per team; one column per API leaf value |
| `latest_long.csv` | Long/tidy format — `(date, team, metric_path, value)` |
| `raw/YYYY-MM-DD/<ABBREV>_<ID>.json` | Raw API response per team per day |

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Pages shows a 404 | Wait ~60 s after the first deploy; hard-refresh. Confirm Pages source is set to **GitHub Actions**. |
| Workflow fails on push | Check **Settings → Actions → General → Workflow permissions** = Read & write. |
| API returns 4xx | NHL sometimes refreshes data mid-morning; re-run later or use `--date` to skip a day. |
| Old data cached in browser | Force-refresh (`Ctrl+Shift+R`); Pages CDN caches aggressively. |
