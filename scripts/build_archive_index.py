#!/usr/bin/env python3
"""
build_archive_index.py
Regenerates docs/index.html — a landing page that:
  • Redirects visitors to the latest report
  • Lists all archived daily reports (newest first)

Run automatically by the GitHub Actions workflow after the main scraper.
Can also be run locally: python scripts/build_archive_index.py
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import html as html_mod

ARCHIVE_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.html$")


def list_archive_dates(archive_dir: str) -> list[str]:
    if not os.path.isdir(archive_dir):
        return []
    dates = []
    for fname in os.listdir(archive_dir):
        m = ARCHIVE_DATE_RE.match(fname)
        if m:
            dates.append(m.group(1))
    return sorted(dates, reverse=True)


def make_index(dates: list[str], run_date: str) -> str:
    rows = []
    for d in dates:
        label = "Today" if d == run_date else d
        badge = " <span class='badge'>latest</span>" if d == dates[0] else ""
        rows.append(
            f"<tr>"
            f"<td><a href='archive/{html_mod.escape(d)}.html'>{html_mod.escape(label)}</a>{badge}</td>"
            f"<td class='muted'>{html_mod.escape(d)}</td>"
            f"</tr>"
        )
    rows_html = "\n".join(rows) if rows else "<tr><td colspan='2' class='muted'>No archives yet.</td></tr>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <meta http-equiv="refresh" content="0; url=latest.html"/>
  <title>NHL EDGE — Team Skating Distance</title>
  <style>
    :root {{
      --bg:#0b0f1a; --text:#e7eefc; --muted:#9fb0d0;
      --border:rgba(255,255,255,.08); --accent:#6ea8fe;
      --card:rgba(18,26,42,.75);
    }}
    body {{
      margin:0;
      background:
        radial-gradient(1200px 800px at 20% 0%,rgba(110,168,254,.15),transparent 50%),
        var(--bg);
      color:var(--text);
      font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial;
      display:flex; flex-direction:column; align-items:center;
      padding:40px 20px;
    }}
    h1 {{ font-size:22px; margin:0 0 6px; letter-spacing:.2px; }}
    .sub {{ color:var(--muted); font-size:13px; margin-bottom:24px; }}
    a {{ color:var(--accent); text-decoration:none; }}
    a:hover {{ text-decoration:underline; }}
    .card {{
      border:1px solid var(--border); background:var(--card);
      border-radius:16px; padding:20px 24px;
      box-shadow:0 10px 30px rgba(0,0,0,.28);
      backdrop-filter:blur(10px);
      width:min(480px,100%);
    }}
    .big-link {{
      display:block; text-align:center; font-size:16px; font-weight:700;
      padding:12px; border-radius:10px;
      background:rgba(110,168,254,.15); border:1px solid rgba(110,168,254,.3);
      margin-bottom:20px;
    }}
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ padding:7px 8px; border-bottom:1px solid var(--border); font-size:13px; }}
    th {{ color:var(--muted); text-align:left; }}
    td.muted {{ color:var(--muted); font-size:12px; }}
    .badge {{
      display:inline-block; padding:1px 6px; border-radius:5px;
      font-size:10px; font-weight:700; margin-left:6px;
      background:rgba(51,209,122,.18); color:#33d17a;
      border:1px solid rgba(51,209,122,.3);
    }}
  </style>
</head>
<body>
  <h1>🏒 NHL EDGE — Team Skating Distance</h1>
  <div class="sub">Daily team-level skating distance data from the NHL EDGE API · Updated each morning</div>
  <div class="card">
    <a class="big-link" href="latest.html">→ View Today's Report</a>
    <table>
      <thead><tr><th>Report</th><th>Date</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
  <div class="sub" style="margin-top:16px;font-size:12px;">
    Generated {html_mod.escape(run_date)} &nbsp;·&nbsp;
    Data: <a href="https://api-web.nhle.com">api-web.nhle.com</a>
  </div>
</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docsdir", default="docs")
    ap.add_argument("--date",    default=None)
    args = ap.parse_args()

    run_date    = args.date or dt.date.today().isoformat()
    archive_dir = os.path.join(args.docsdir, "archive")
    dates       = list_archive_dates(archive_dir)

    os.makedirs(args.docsdir, exist_ok=True)
    out = os.path.join(args.docsdir, "index.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(make_index(dates, run_date))

    print(f"Wrote {out}  ({len(dates)} archive entries listed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
