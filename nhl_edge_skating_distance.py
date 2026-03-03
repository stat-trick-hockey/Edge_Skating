#!/usr/bin/env python3
"""
Daily NHL EDGE — Team Skating Distance Detail
(Hard-coded Team IDs) + Tight (Low-Scroll) GitHub Pages HTML

EDGE Endpoint:
  GET https://api-web.nhle.com/v1/edge/team-skating-distance-detail/{team-id}/now

Key fix (v8):
- ✅ Drops the frozen `distancePer60.metric` season field entirely.
  The NHL API does not update this field game-to-game — it is static.
- ✅ Now compares Last-5 games pace vs Previous-5 games pace (G1–5 vs G6–10),
  both computed from `skatingDistanceLast10` distance + TOI.
  - G1–5  = games[0:5]  (most recent)
  - G6–10 = games[5:10] (older)
  - Δ = G1-5 km/60 − G6-10 km/60   (positive = trending up)
- ✅ Movers + main table now show this apples-to-apples pace delta.
- ✅ Labels updated: "G6-10" (older baseline) and "G1-5" (recent).

Still saves:
- raw JSON
- full numeric leaf wide + long CSVs (for schema exploration)
- tight HTML for GitHub Pages

Dependencies:
  pip install requests pandas
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    import pandas as pd
except ImportError:
    print("Missing dependency: pandas. Install with: pip install pandas", file=sys.stderr)
    raise

BASE = "https://api-web.nhle.com"

# ----------------------------
# HARD-CODED NHL TEAM IDS (32)
# ----------------------------
NHL_TEAMS = [
    {"id": 1,  "abbrev": "NJD", "name": "New Jersey Devils"},
    {"id": 2,  "abbrev": "NYI", "name": "New York Islanders"},
    {"id": 3,  "abbrev": "NYR", "name": "New York Rangers"},
    {"id": 4,  "abbrev": "PHI", "name": "Philadelphia Flyers"},
    {"id": 5,  "abbrev": "PIT", "name": "Pittsburgh Penguins"},
    {"id": 6,  "abbrev": "BOS", "name": "Boston Bruins"},
    {"id": 7,  "abbrev": "BUF", "name": "Buffalo Sabres"},
    {"id": 8,  "abbrev": "MTL", "name": "Montreal Canadiens"},
    {"id": 9,  "abbrev": "OTT", "name": "Ottawa Senators"},
    {"id": 10, "abbrev": "TOR", "name": "Toronto Maple Leafs"},
    {"id": 12, "abbrev": "CAR", "name": "Carolina Hurricanes"},
    {"id": 13, "abbrev": "FLA", "name": "Florida Panthers"},
    {"id": 14, "abbrev": "TBL", "name": "Tampa Bay Lightning"},
    {"id": 15, "abbrev": "WSH", "name": "Washington Capitals"},
    {"id": 16, "abbrev": "CHI", "name": "Chicago Blackhawks"},
    {"id": 17, "abbrev": "DET", "name": "Detroit Red Wings"},
    {"id": 18, "abbrev": "NSH", "name": "Nashville Predators"},
    {"id": 19, "abbrev": "STL", "name": "St. Louis Blues"},
    {"id": 20, "abbrev": "CGY", "name": "Calgary Flames"},
    {"id": 21, "abbrev": "COL", "name": "Colorado Avalanche"},
    {"id": 22, "abbrev": "EDM", "name": "Edmonton Oilers"},
    {"id": 23, "abbrev": "VAN", "name": "Vancouver Canucks"},
    {"id": 24, "abbrev": "ANA", "name": "Anaheim Ducks"},
    {"id": 25, "abbrev": "DAL", "name": "Dallas Stars"},
    {"id": 26, "abbrev": "LAK", "name": "Los Angeles Kings"},
    {"id": 28, "abbrev": "SJS", "name": "San Jose Sharks"},
    {"id": 29, "abbrev": "CBJ", "name": "Columbus Blue Jackets"},
    {"id": 30, "abbrev": "MIN", "name": "Minnesota Wild"},
    {"id": 52, "abbrev": "WPG", "name": "Winnipeg Jets"},
    {"id": 54, "abbrev": "VGK", "name": "Vegas Golden Knights"},
    {"id": 55, "abbrev": "SEA", "name": "Seattle Kraken"},
    {"id": 59, "abbrev": "UTA", "name": "Utah Hockey Club"},
]


@dataclass(frozen=True)
class Team:
    team_id: int
    abbrev: str
    name: str


# ---------------------------------------
# Metric specs — Last10 only
# ---------------------------------------
# skatingDistanceLast10[*] per-game fields:
#   toiAll / toiEven / toiPP / toiPK  (seconds)
#   distanceSkatedAll / Even / PP / PK .metric  (km)
#
# We split the array:
#   G1–5  = games[0:5]  (most recent)
#   G6–10 = games[5:10] (older baseline)
#
METRIC_SPECS = [
    {
        "key": "all",
        "label": "All Situations",
        "desc": "All situations skating distance pace (km/60). G1-5 vs G6-10.",
        "last10_dist_field": "distanceSkatedAll.metric",
        "last10_toi_key": "toiAll",
    },
    {
        "key": "es",
        "label": "Even Strength",
        "desc": "Even-strength skating distance pace (km/60). G1-5 vs G6-10.",
        "last10_dist_field": "distanceSkatedEven.metric",
        "last10_toi_key": "toiEven",
    },
    {
        "key": "pp",
        "label": "Power Play",
        "desc": "Power-play skating distance pace (km/60). G1-5 vs G6-10.",
        "last10_dist_field": "distanceSkatedPP.metric",
        "last10_toi_key": "toiPP",
    },
    {
        "key": "pk",
        "label": "Penalty Kill",
        "desc": "Penalty-kill skating distance pace (km/60). G1-5 vs G6-10.",
        "last10_dist_field": "distanceSkatedPK.metric",
        "last10_toi_key": "toiPK",
    },
]


# ----------------------------
# HTTP + IO helpers
# ----------------------------
def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def fetch_json(url: str, timeout: int = 30, retries: int = 3, backoff: float = 1.6) -> Any:
    headers = {
        "User-Agent": "nhl-edge-skating-distance-daily/8.0",
        "Accept": "application/json",
    }
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff ** attempt)
            else:
                raise RuntimeError(f"Failed to fetch {url}: {e}") from e
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


# ----------------------------
# Flatten JSON to numeric leaves (for full schema CSVs)
# ----------------------------
def flatten_numeric_leaves(obj: Any, prefix: str = "") -> Dict[str, float]:
    out: Dict[str, float] = {}

    def rec(x: Any, p: str) -> None:
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            out[p] = float(x)
            return
        if isinstance(x, dict):
            for k, v in x.items():
                newp = f"{p}.{k}" if p else str(k)
                rec(v, newp)
            return
        if isinstance(x, list):
            for i, v in enumerate(x):
                newp = f"{p}[{i}]" if p else f"[{i}]"
                rec(v, newp)
            return

    rec(obj, prefix)
    return out


def to_long_rows(team: Team, flat: Dict[str, float], run_date: str) -> List[Dict[str, Any]]:
    return [
        {
            "date": run_date,
            "teamId": team.team_id,
            "team": team.abbrev,
            "teamName": team.name,
            "metric_path": path,
            "value": val,
        }
        for path, val in flat.items()
    ]


# ----------------------------
# Pace computation from a slice of skatingDistanceLast10
# ----------------------------
def _slice_pace(
    games: list,
    dist_field_path: str,
    toi_key: str,
) -> Tuple[Optional[float], Optional[float], int]:
    """
    Returns (sum_distance_km, sum_toi_seconds, n_games_used) for a slice of games.
    dist_field_path: e.g. 'distanceSkatedAll.metric'
    toi_key: e.g. 'toiAll'
    """
    a, b = dist_field_path.split(".", 1)
    sum_km = 0.0
    sum_toi = 0.0
    n = 0

    for g in games:
        if not isinstance(g, dict):
            continue
        dist_obj = g.get(a)
        v = (dist_obj or {}).get(b) if isinstance(dist_obj, dict) else None
        t = g.get(toi_key)
        if (
            isinstance(v, (int, float)) and not isinstance(v, bool)
            and isinstance(t, (int, float)) and not isinstance(t, bool)
            and float(t) > 0
        ):
            sum_km += float(v)
            sum_toi += float(t)
            n += 1

    if n == 0:
        return None, None, 0
    return sum_km, sum_toi, n


def _km_per60(sum_km: Optional[float], sum_toi_seconds: Optional[float]) -> Optional[float]:
    if sum_km is None or sum_toi_seconds is None or sum_toi_seconds <= 0:
        return None
    return sum_km / (sum_toi_seconds / 3600.0)


@dataclass
class MetricValue:
    # "older" = G6-10 (baseline), "recent" = G1-5
    older_km_per60: Optional[float]    # G6-10 pace
    recent_km_per60: Optional[float]   # G1-5 pace
    full_l10_km: Optional[float]       # total km over all 10 games
    delta_per60: Optional[float]       # recent - older
    pct_per60: Optional[float]         # delta / older * 100
    recent_n: int                      # games used in G1-5
    older_n: int                       # games used in G6-10


def extract_metric(payload: dict, spec: dict) -> MetricValue:
    games = payload.get("skatingDistanceLast10") or []
    if not isinstance(games, list):
        games = []

    dist_field = spec["last10_dist_field"]
    toi_key = spec["last10_toi_key"]

    # Split: index 0 = most recent game
    recent_games = games[:5]
    older_games = games[5:]

    recent_km, recent_toi, recent_n = _slice_pace(recent_games, dist_field, toi_key)
    older_km, older_toi, older_n = _slice_pace(older_games, dist_field, toi_key)
    full_km, _, _ = _slice_pace(games, dist_field, toi_key)

    recent_per60 = _km_per60(recent_km, recent_toi)
    older_per60 = _km_per60(older_km, older_toi)

    delta_per60 = (
        (recent_per60 - older_per60)
        if (recent_per60 is not None and older_per60 is not None)
        else None
    )
    pct_per60 = (
        (delta_per60 / older_per60 * 100.0)
        if (delta_per60 is not None and older_per60 and older_per60 != 0)
        else None
    )

    return MetricValue(
        older_km_per60=older_per60,
        recent_km_per60=recent_per60,
        full_l10_km=full_km,
        delta_per60=delta_per60,
        pct_per60=pct_per60,
        recent_n=recent_n,
        older_n=older_n,
    )


# ----------------------------
# HTML formatting helpers
# ----------------------------
def _escape(s: Any) -> str:
    return html.escape("" if s is None else str(s))


def _fmt_num(x: Any, digits: int = 1) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return _escape(x)


def _fmt_signed(x: Any, digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    try:
        return f"{float(x):+,.{digits}f}"
    except Exception:
        return _escape(x)


def _fmt_pct(x: Any, digits: int = 1) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    try:
        return f"{float(x):+,.{digits}f}%"
    except Exception:
        return _escape(x)


# ----------------------------
# Movers (by Δ% in km/60, recent vs older)
# ----------------------------
def movers_for_metric(
    teams: List[Team],
    metrics_by_team: Dict[str, Dict[str, MetricValue]],
    metric_key: str,
) -> Tuple[List[Tuple], List[Tuple]]:
    rows = []
    for t in teams:
        mv = metrics_by_team.get(t.abbrev, {}).get(metric_key)
        if not mv:
            continue
        if mv.pct_per60 is None or mv.older_km_per60 is None or mv.recent_km_per60 is None:
            continue
        rows.append((t.abbrev, mv.older_km_per60, mv.recent_km_per60, mv.delta_per60 or 0.0, mv.pct_per60))
    up = sorted(rows, key=lambda x: x[4], reverse=True)[:10]
    dn = sorted(rows, key=lambda x: x[4])[:10]
    return up, dn


def movers_table(rows: List[Tuple]) -> str:
    if not rows:
        return "<p class='muted'>Not available today.</p>"
    trs = []
    for team, old, rec, d, p in rows:
        cls_p = "pos" if p > 0 else ("neg" if p < 0 else "")
        cls_d = "pos" if d > 0 else ("neg" if d < 0 else "")
        trs.append(
            "<tr>"
            f"<td class='team'>{_escape(team)}</td>"
            f"<td class='num'>{_fmt_num(old, 2)}</td>"
            f"<td class='num'>{_fmt_num(rec, 2)}</td>"
            f"<td class='num {cls_d}'>{_fmt_signed(d, 2)}</td>"
            f"<td class='num {cls_p}'>{_fmt_pct(p, 1)}</td>"
            "</tr>"
        )
    return (
        "<table class='movers'>"
        "<thead><tr>"
        "<th>Team</th>"
        "<th>G6-10 km/60</th>"
        "<th>G1-5 km/60</th>"
        "<th>Δ km/60</th>"
        "<th>Δ %</th>"
        "</tr></thead>"
        "<tbody>" + "\n".join(trs) + "</tbody></table>"
    )


# ----------------------------
# Tight HTML rendering
# ----------------------------
def make_html_tight(
    run_date: str,
    teams: List[Team],
    metrics_by_team: Dict[str, Dict[str, MetricValue]],
    title: str = "NHL EDGE — Team Skating Distance (Tight View)",
) -> str:
    metric_keys = [m["key"] for m in METRIC_SPECS]
    metric_labels = {m["key"]: m["label"] for m in METRIC_SPECS}
    metric_desc = {m["key"]: m["desc"] for m in METRIC_SPECS}

    all_up, all_dn = movers_for_metric(teams, metrics_by_team, "all")
    pp_up, pp_dn = movers_for_metric(teams, metrics_by_team, "pp")
    pk_up, pk_dn = movers_for_metric(teams, metrics_by_team, "pk")

    def metric_cell(mv: MetricValue) -> str:
        old_p = _fmt_num(mv.older_km_per60, 2)
        rec_p = _fmt_num(mv.recent_km_per60, 2)

        d = mv.delta_per60
        p = mv.pct_per60
        d_cls = "pos" if (d is not None and d > 0) else ("neg" if (d is not None and d < 0) else "")
        p_cls = "pos" if (p is not None and p > 0) else ("neg" if (p is not None and p < 0) else "")

        totals = ""
        if mv.full_l10_km is not None:
            totals = f"<div class='pg muted'>L10 total: {_fmt_num(mv.full_l10_km, 1)} km</div>"

        games_line = f"<div class='pg muted'>G1-5: {mv.recent_n} gms • G6-10: {mv.older_n} gms</div>"

        return (
            "<div class='cellblock'>"
            f"<div class='row1'>"
            f"<span class='k'>G6-10</span> <span class='num'>{old_p}</span>"
            f" <span class='sep'>|</span> "
            f"<span class='k'>G1-5</span> <span class='num'>{rec_p}</span>"
            f"</div>"
            f"<div class='row2'>"
            f"<span class='k'>Δ</span> <span class='num {d_cls}'>{_fmt_signed(d, 2)}</span>"
            f" <span class='sep'>|</span> "
            f"<span class='k'>%</span> <span class='num {p_cls}'>{_fmt_pct(p, 1)}</span>"
            f"</div>"
            f"{totals}"
            f"{games_line}"
            "</div>"
        )

    body_rows: List[str] = []
    for t in teams:
        t_metrics = metrics_by_team.get(t.abbrev, {})
        tds = [
            f"<td class='teamcell'>"
            f"<div class='teamabbr'>{_escape(t.abbrev)}</div>"
            f"<div class='teamname muted'>{_escape(t.name)}</div>"
            f"</td>"
        ]
        for k in metric_keys:
            mv = t_metrics.get(k)
            if mv is None:
                tds.append("<td><div class='cellblock muted'>—</div></td>")
            else:
                tds.append(f"<td>{metric_cell(mv)}</td>")
        body_rows.append("<tr>" + "".join(tds) + "</tr>")

    header = (
        "<tr>"
        "<th>Team</th>"
        + "".join(
            f"<th title='{_escape(metric_desc[k])}'>{_escape(metric_labels[k])}</th>"
            for k in metric_keys
        )
        + "</tr>"
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{_escape(title)} — {run_date}</title>
  <style>
    :root {{
      --bg: #0b0f1a;
      --text: #e7eefc;
      --muted: #9fb0d0;
      --border: rgba(255,255,255,.08);
      --accent: #6ea8fe;
      --pos: #33d17a;
      --neg: #ff6b6b;
      --card: rgba(18,26,42,.75);
    }}
    body {{
      margin: 0;
      background: radial-gradient(1200px 800px at 20% 0%, rgba(110,168,254,.15), transparent 50%),
                  radial-gradient(1200px 800px at 80% 20%, rgba(51,209,122,.10), transparent 55%),
                  var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial;
    }}
    .wrap {{ max-width: 1100px; margin: 0 auto; padding: 18px; }}
    .topbar {{
      display: flex; gap: 10px; align-items: baseline; justify-content: space-between;
      padding: 14px 16px; border: 1px solid var(--border); background: var(--card);
      border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,.35);
      backdrop-filter: blur(10px);
    }}
    h1 {{ font-size: 18px; margin: 0; letter-spacing: .2px; }}
    .sub {{ color: var(--muted); font-size: 13px; }}
    .smalllink a {{ color: var(--accent); text-decoration: none; }}
    .smalllink a:hover {{ text-decoration: underline; }}
    .card {{
      border: 1px solid var(--border);
      background: var(--card);
      border-radius: 16px;
      padding: 14px;
      box-shadow: 0 10px 30px rgba(0,0,0,.28);
      backdrop-filter: blur(10px);
      margin-top: 12px;
    }}
    .grid {{ display: grid; grid-template-columns: 1fr; gap: 12px; margin-top: 12px; }}
    @media (min-width: 980px) {{ .grid.two {{ grid-template-columns: 1fr 1fr; }} }}
    .muted {{ color: var(--muted); font-size: 12px; }}
    table {{ width: 100%; border-collapse: collapse; border-radius: 14px; overflow: hidden; }}
    th, td {{ border-bottom: 1px solid var(--border); padding: 8px 10px; font-size: 12px; vertical-align: top; }}
    th {{
      position: sticky; top: 0;
      background: rgba(18,26,42,.92);
      text-align: left;
      user-select: none;
      white-space: nowrap;
    }}
    tr:hover td {{ background: rgba(110,168,254,.06); }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }}
    .pos {{ color: var(--pos); }}
    .neg {{ color: var(--neg); }}
    .controls {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
    input[type="search"] {{
      width: min(520px, 100%);
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: rgba(0,0,0,.25);
      color: var(--text);
      outline: none;
    }}
    .pill {{
      border: 1px solid var(--border);
      background: rgba(0,0,0,.18);
      padding: 8px 10px;
      border-radius: 999px;
      font-size: 12px;
      color: var(--muted);
    }}
    .teamcell .teamabbr {{ font-weight: 700; letter-spacing: .3px; }}
    .teamcell .teamname {{ margin-top: 2px; }}
    .cellblock {{
      border: 1px solid rgba(255,255,255,.06);
      background: rgba(0,0,0,.12);
      border-radius: 12px;
      padding: 8px;
      min-width: 0;
    }}
    .cellblock .row1, .cellblock .row2 {{
      display: flex;
      gap: 6px;
      align-items: baseline;
      justify-content: space-between;
    }}
    .cellblock .row2 {{ margin-top: 4px; }}
    .cellblock .k {{ color: var(--muted); font-size: 11px; min-width: 30px; }}
    .cellblock .sep {{ color: rgba(255,255,255,.18); }}
    .cellblock .pg {{ margin-top: 6px; font-size: 11px; }}
    table.movers th, table.movers td {{ padding: 7px 8px; }}
    table.movers td.team {{ font-weight: 700; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div>
        <h1>{_escape(title)}</h1>
        <div class="sub">
          Run date: {run_date} &nbsp;•&nbsp;
          Pace in <b>km/60</b> &nbsp;•&nbsp;
          <b>G1-5</b> = last 5 games &nbsp;|&nbsp; <b>G6-10</b> = prior 5 games &nbsp;•&nbsp;
          Δ = G1-5 − G6-10
        </div>
      </div>
      <div class="smalllink sub">
        <a href="archive/{run_date}.html">Open archive</a>
      </div>
    </div>

    <div class="card">
      <div class="sub"><b>Interpretation</b></div>
      <div class="muted" style="margin-top:6px; line-height:1.6;">
        All pace figures are computed from <code>skatingDistanceLast10</code> game-level data:
        <code>SUM(distance) ÷ (SUM(TOI) / 3600)</code>.
        The NHL's pre-computed <code>distancePer60</code> season field is <b>not used</b> — it does not
        update game-to-game. G1-5 is the most recent 5 games; G6-10 is the prior 5. A positive Δ
        means the team is skating more per 60 minutes recently than in the prior window.
      </div>
    </div>

    <!-- ALL movers -->
    <div class="grid two">
      <div class="card">
        <div class="sub" style="margin-bottom:10px;"><b>Top 10 ↑</b> — All Situations (Δ % km/60, G1-5 vs G6-10)</div>
        {movers_table(all_up)}
      </div>
      <div class="card">
        <div class="sub" style="margin-bottom:10px;"><b>Top 10 ↓</b> — All Situations (Δ % km/60, G1-5 vs G6-10)</div>
        {movers_table(all_dn)}
      </div>
    </div>

    <!-- PP/PK movers -->
    <div class="grid two">
      <div class="card">
        <div class="sub" style="margin-bottom:10px;"><b>Top 10 ↑</b> — Power Play (Δ % km/60)</div>
        {movers_table(pp_up)}
      </div>
      <div class="card">
        <div class="sub" style="margin-bottom:10px;"><b>Top 10 ↓</b> — Power Play (Δ % km/60)</div>
        {movers_table(pp_dn)}
      </div>
      <div class="card">
        <div class="sub" style="margin-bottom:10px;"><b>Top 10 ↑</b> — Penalty Kill (Δ % km/60)</div>
        {movers_table(pk_up)}
      </div>
      <div class="card">
        <div class="sub" style="margin-bottom:10px;"><b>Top 10 ↓</b> — Penalty Kill (Δ % km/60)</div>
        {movers_table(pk_dn)}
      </div>
    </div>

    <div class="card">
      <div class="controls">
        <input id="search" type="search" placeholder="Filter teams…" />
        <span class="pill" id="rowcount"></span>
        <span class="pill">Tight view · 1 row per team</span>
      </div>
      <div style="overflow:auto; border-radius: 14px; margin-top: 10px;">
        <table id="main">
          <thead>{header}</thead>
          <tbody>
            {"".join(body_rows)}
          </tbody>
        </table>
      </div>
      <div class="muted" style="margin-top:10px;">
        Full schema saved to CSV. This page is optimized for quick daily scanning.
      </div>
    </div>
  </div>

<script>
(function() {{
  const tbody = document.querySelector("#main tbody");
  const search = document.getElementById("search");
  const rowcount = document.getElementById("rowcount");

  function updateRowCount() {{
    const rows = Array.from(tbody.querySelectorAll("tr"));
    const visible = rows.filter(r => r.style.display !== "none").length;
    rowcount.textContent = `${{visible}} / ${{rows.length}} teams`;
  }}

  search.addEventListener("input", () => {{
    const q = search.value.trim().toLowerCase();
    Array.from(tbody.querySelectorAll("tr")).forEach(r => {{
      r.style.display = (!q || r.textContent.toLowerCase().includes(q)) ? "" : "none";
    }});
    updateRowCount();
  }});

  updateRowCount();
}})();
</script>
</body>
</html>
"""


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="data/edge_skating_distance", help="Data output directory")
    ap.add_argument("--docsdir", default="docs", help="GitHub Pages docs directory")
    ap.add_argument("--sleep", type=float, default=0.12, help="Sleep between team calls (seconds)")
    ap.add_argument("--timeout", type=int, default=30, help="Request timeout (seconds)")
    ap.add_argument("--retries", type=int, default=3, help="Retries per request")
    ap.add_argument("--date", default=None, help="Run date YYYY-MM-DD (default: today local)")
    args = ap.parse_args()

    run_date = args.date or dt.date.today().isoformat()

    outdir = args.outdir
    raw_dir = os.path.join(outdir, "raw", run_date)
    safe_mkdir(outdir)
    safe_mkdir(raw_dir)

    docsdir = args.docsdir
    archive_dir = os.path.join(docsdir, "archive")
    safe_mkdir(docsdir)
    safe_mkdir(archive_dir)

    teams = sorted(
        [Team(team_id=t["id"], abbrev=t["abbrev"], name=t["name"]) for t in NHL_TEAMS],
        key=lambda t: t.abbrev,
    )

    wide_rows_full: List[Dict[str, Any]] = []
    long_rows_full: List[Dict[str, Any]] = []
    metrics_by_team: Dict[str, Dict[str, MetricValue]] = {}

    for team in teams:
        url = f"{BASE}/v1/edge/team-skating-distance-detail/{team.team_id}/now"
        payload = fetch_json(url, timeout=args.timeout, retries=args.retries)

        write_json(os.path.join(raw_dir, f"{team.abbrev}_{team.team_id}.json"), payload)

        flat = flatten_numeric_leaves(payload)

        wide_row: Dict[str, Any] = {
            "date": run_date,
            "teamId": team.team_id,
            "team": team.abbrev,
            "teamName": team.name,
        }
        wide_row.update(flat)
        wide_rows_full.append(wide_row)
        long_rows_full.extend(to_long_rows(team, flat, run_date))

        metrics_by_team[team.abbrev] = {}
        for spec in METRIC_SPECS:
            metrics_by_team[team.abbrev][spec["key"]] = extract_metric(payload, spec)

        time.sleep(max(0.0, args.sleep))

    # ---- Full CSVs ----
    df_wide = pd.DataFrame(wide_rows_full)
    df_long = pd.DataFrame(long_rows_full)

    id_cols = ["date", "teamId", "team", "teamName"]
    other_cols = [c for c in df_wide.columns if c not in id_cols]
    df_wide = df_wide[id_cols + sorted(other_cols)]

    wide_csv = os.path.join(outdir, f"team_skating_distance_detail_wide_{run_date}.csv")
    long_csv = os.path.join(outdir, f"team_skating_distance_detail_long_{run_date}.csv")
    df_wide.to_csv(wide_csv, index=False)
    df_long.to_csv(long_csv, index=False)
    df_wide.to_csv(os.path.join(outdir, "latest_wide.csv"), index=False)
    df_long.to_csv(os.path.join(outdir, "latest_long.csv"), index=False)

    # ---- Tight HTML ----
    html_doc = make_html_tight(run_date, teams, metrics_by_team)
    latest_html = os.path.join(docsdir, "latest.html")
    archive_html = os.path.join(archive_dir, f"{run_date}.html")

    with open(latest_html, "w", encoding="utf-8") as f:
        f.write(html_doc)
    with open(archive_html, "w", encoding="utf-8") as f:
        f.write(html_doc)

    print("Wrote:")
    print(f"  {wide_csv}")
    print(f"  {long_csv}")
    print(f"  {latest_html}")
    print(f"  {archive_html}")
    print(f"  Raw JSON → {raw_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
