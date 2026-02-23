#!/usr/bin/env python3
"""
Daily NHL EDGE — Team Skating Distance Detail
(Hard-coded Team IDs) + Tight (Low-Scroll) GitHub Pages HTML

EDGE Endpoint:
  GET https://api-web.nhle.com/v1/edge/team-skating-distance-detail/{team-id}/now

Key fixes:
- ✅ Correct "season" extraction even when season fields aren't labeled with "season"
  (though this endpoint is now handled explicitly).
- ✅ Fixes the “these look off” issue by comparing apples-to-apples:
    - Season pace:   skatingDistanceDetails[].distancePer60.metric  (km/60)
    - Last10 pace:   computed from skatingDistanceLast10[*] using distance + TOI (km/60)
- ✅ Movers + main tight table now show km/60 deltas, not season-total vs last10-total.

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
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
    {"id": 1, "abbrev": "NJD", "name": "New Jersey Devils"},
    {"id": 2, "abbrev": "NYI", "name": "New York Islanders"},
    {"id": 3, "abbrev": "NYR", "name": "New York Rangers"},
    {"id": 4, "abbrev": "PHI", "name": "Philadelphia Flyers"},
    {"id": 5, "abbrev": "PIT", "name": "Pittsburgh Penguins"},
    {"id": 6, "abbrev": "BOS", "name": "Boston Bruins"},
    {"id": 7, "abbrev": "BUF", "name": "Buffalo Sabres"},
    {"id": 8, "abbrev": "MTL", "name": "Montreal Canadiens"},
    {"id": 9, "abbrev": "OTT", "name": "Ottawa Senators"},
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
# Tight metrics (endpoint-aware)
# ---------------------------------------
# Season:
#   skatingDistanceDetails[*] where strengthCode in {"all","es","pp","pk"} AND positionCode=="all"
#   - distanceTotal.metric   (KM)
#   - distancePer60.metric   (KM/60)
#
# Last10:
#   skatingDistanceLast10[*] per-game:
#   - toiAll/toiEven/toiPP/toiPK (seconds)
#   - distanceSkatedAll/Even/PP/PK.metric (KM)
#   We compute:
#     last10_total_km = SUM(distanceSkatedX.metric)
#     last10_km_per60 = last10_total_km / (SUM(toiX)/3600)
#
METRIC_SPECS = [
    {
        "key": "all",
        "label": "All Situations",
        "desc": "All situations skating distance pace (km/60) and totals (km).",
        "season_strength": "all",
        "last10_dist_field": "distanceSkatedAll.metric",
        "last10_toi_key": "toiAll",
    },
    {
        "key": "es",
        "label": "Even Strength",
        "desc": "Even-strength skating distance pace (km/60) and totals (km).",
        "season_strength": "es",
        "last10_dist_field": "distanceSkatedEven.metric",
        "last10_toi_key": "toiEven",
    },
    {
        "key": "pp",
        "label": "Power Play",
        "desc": "Power-play skating distance pace (km/60) and totals (km).",
        "season_strength": "pp",
        "last10_dist_field": "distanceSkatedPP.metric",
        "last10_toi_key": "toiPP",
    },
    {
        "key": "pk",
        "label": "Penalty Kill",
        "desc": "Penalty-kill skating distance pace (km/60) and totals (km).",
        "season_strength": "pk",
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
        "User-Agent": "nhl-edge-skating-distance-daily/7.0",
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
                time.sleep(backoff**attempt)
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
# Endpoint-aware extraction
# ----------------------------
def _get_details_row(payload: dict, strength_code: str) -> Optional[dict]:
    rows = payload.get("skatingDistanceDetails") or []
    for r in rows:
        if not isinstance(r, dict):
            continue
        if r.get("strengthCode") == strength_code and r.get("positionCode") == "all":
            return r
    return None


def _get_season_total_km(payload: dict, strength_code: str) -> Optional[float]:
    r = _get_details_row(payload, strength_code)
    if not r:
        return None
    v = (r.get("distanceTotal") or {}).get("metric")
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _get_season_km_per60(payload: dict, strength_code: str) -> Optional[float]:
    r = _get_details_row(payload, strength_code)
    if not r:
        return None
    v = (r.get("distancePer60") or {}).get("metric")
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _sum_last10_distance_and_toi(payload: dict, dist_field_path: str, toi_key: str) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    """
    Returns: (sum_distance_km, sum_toi_seconds, n_games_used)
    dist_field_path: 'distanceSkatedAll.metric' etc.
    toi_key: 'toiAll' etc.
    """
    games = payload.get("skatingDistanceLast10") or []
    if not isinstance(games, list) or not games:
        return None, None, None

    a, b = dist_field_path.split(".", 1)
    sum_km = 0.0
    sum_toi = 0.0
    found_dist = 0
    found_toi = 0

    for g in games:
        if not isinstance(g, dict):
            continue

        # distance
        dist_obj = g.get(a)
        if isinstance(dist_obj, dict):
            v = dist_obj.get(b)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                sum_km += float(v)
                found_dist += 1

        # toi
        toi = g.get(toi_key)
        if isinstance(toi, (int, float)) and not isinstance(toi, bool):
            sum_toi += float(toi)
            found_toi += 1

    if found_dist == 0:
        return None, None, None
    if found_toi == 0 or sum_toi <= 0:
        return sum_km, None, found_dist

    return sum_km, sum_toi, found_dist


def _km_per60(sum_km: Optional[float], sum_toi_seconds: Optional[float]) -> Optional[float]:
    if sum_km is None or sum_toi_seconds is None or sum_toi_seconds <= 0:
        return None
    hours = sum_toi_seconds / 3600.0
    return sum_km / hours


@dataclass
class MetricValue:
    season_total_km: Optional[float]
    last10_total_km: Optional[float]
    season_km_per60: Optional[float]
    last10_km_per60: Optional[float]
    delta_per60: Optional[float]
    pct_per60: Optional[float]
    last10_games_used: Optional[int]
    season_path: str
    last10_path: str


def extract_metric(payload: dict, spec: dict) -> MetricValue:
    sc = spec["season_strength"]

    season_total_km = _get_season_total_km(payload, sc)
    season_km_per60 = _get_season_km_per60(payload, sc)

    last10_total_km, last10_toi, n_games = _sum_last10_distance_and_toi(
        payload, spec["last10_dist_field"], spec["last10_toi_key"]
    )
    last10_km_per60 = _km_per60(last10_total_km, last10_toi)

    delta_per60 = (
        (last10_km_per60 - season_km_per60)
        if (last10_km_per60 is not None and season_km_per60 is not None)
        else None
    )
    pct_per60 = (delta_per60 / season_km_per60 * 100.0) if (delta_per60 is not None and season_km_per60) else None

    return MetricValue(
        season_total_km=season_total_km,
        last10_total_km=last10_total_km,
        season_km_per60=season_km_per60,
        last10_km_per60=last10_km_per60,
        delta_per60=delta_per60,
        pct_per60=pct_per60,
        last10_games_used=n_games,
        season_path=f"skatingDistanceDetails[strengthCode={sc}, positionCode=all].distanceTotal.metric + distancePer60.metric",
        last10_path=f"SUM(skatingDistanceLast10[*].{spec['last10_dist_field']}) + SUM({spec['last10_toi_key']})",
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


def _fmt_signed(x: Any, digits: int = 1) -> str:
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
# Movers (by Δ% in km/60)
# ----------------------------
def movers_for_metric(
    teams: List[Team],
    metrics_by_team: Dict[str, Dict[str, MetricValue]],
    metric_key: str,
) -> Tuple[List[Tuple[str, float, float, float, float]], List[Tuple[str, float, float, float, float]]]:
    rows: List[Tuple[str, float, float, float, float]] = []
    for t in teams:
        mv = metrics_by_team[t.abbrev].get(metric_key)
        if not mv:
            continue
        if mv.pct_per60 is None or mv.season_km_per60 is None or mv.last10_km_per60 is None:
            continue
        rows.append((t.abbrev, mv.season_km_per60, mv.last10_km_per60, mv.delta_per60 or 0.0, mv.pct_per60))
    up = sorted(rows, key=lambda x: x[4], reverse=True)[:10]
    dn = sorted(rows, key=lambda x: x[4])[:10]
    return up, dn


def movers_table(rows: List[Tuple[str, float, float, float, float]]) -> str:
    if not rows:
        return "<p class='muted'>Not available today.</p>"
    trs = []
    for team, s, l, d, p in rows:
        cls_p = "pos" if p > 0 else ("neg" if p < 0 else "")
        cls_d = "pos" if d > 0 else ("neg" if d < 0 else "")
        trs.append(
            "<tr>"
            f"<td class='team'>{_escape(team)}</td>"
            f"<td class='num'>{_fmt_num(s, 2)}</td>"
            f"<td class='num'>{_fmt_num(l, 2)}</td>"
            f"<td class='num {cls_d}'>{_fmt_signed(d, 2)}</td>"
            f"<td class='num {cls_p}'>{_fmt_pct(p, 1)}</td>"
            "</tr>"
        )
    return (
        "<table class='movers'>"
        "<thead><tr><th>Team</th><th>Season km/60</th><th>Last10 km/60</th><th>Δ km/60</th><th>Δ %</th></tr></thead>"
        "<tbody>" + "\n".join(trs) + "</tbody></table>"
    )


# ----------------------------
# Tight HTML rendering (main table uses km/60 with optional totals line)
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

    # Movers use km/60 deltas
    all_up, all_dn = movers_for_metric(teams, metrics_by_team, "all")
    pp_up, pp_dn = movers_for_metric(teams, metrics_by_team, "pp")
    pk_up, pk_dn = movers_for_metric(teams, metrics_by_team, "pk")

    def metric_cell(mv: MetricValue) -> str:
        # Row 1: S km/60 | L10 km/60
        s_p = _fmt_num(mv.season_km_per60, 2)
        l_p = _fmt_num(mv.last10_km_per60, 2)

        # Row 2: Δ km/60 | Δ %
        d = mv.delta_per60
        p = mv.pct_per60
        d_cls = "pos" if (d is not None and d > 0) else ("neg" if (d is not None and d < 0) else "")
        p_cls = "pos" if (p is not None and p > 0) else ("neg" if (p is not None and p < 0) else "")

        # Row 3 (muted): totals for context (season total km and last10 total km)
        totals = ""
        if mv.season_total_km is not None or mv.last10_total_km is not None:
            totals = (
                f"<div class='pg muted'>totals km: S {_fmt_num(mv.season_total_km, 0)} • "
                f"L10 {_fmt_num(mv.last10_total_km, 1)}</div>"
            )

        # Row 4 (muted): games used in last10 array (sometimes <10)
        games_line = ""
        if mv.last10_games_used is not None:
            games_line = f"<div class='pg muted'>L10 games used: {mv.last10_games_used}</div>"

        return (
            "<div class='cellblock'>"
            f"<div class='row1'><span class='k'>S</span> <span class='num'>{s_p}</span>"
            f" <span class='sep'>|</span> <span class='k'>L10</span> <span class='num'>{l_p}</span></div>"
            f"<div class='row2'><span class='k'>Δ</span> <span class='num {d_cls}'>{_fmt_signed(d, 2)}</span>"
            f" <span class='sep'>|</span> <span class='k'>%</span> <span class='num {p_cls}'>{_fmt_pct(p, 1)}</span></div>"
            f"{totals}"
            f"{games_line}"
            "</div>"
        )

    # Main table rows
    body_rows: List[str] = []
    for t in teams:
        t_metrics = metrics_by_team.get(t.abbrev, {})
        tds = [
            f"<td class='teamcell'><div class='teamabbr'>{_escape(t.abbrev)}</div>"
            f"<div class='teamname muted'>{_escape(t.name)}</div></td>"
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
        + "".join(f"<th title='{_escape(metric_desc[k])}'>{_escape(metric_labels[k])}</th>" for k in metric_keys)
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
      padding: 14px 14px;
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

    .controls {{
      display:flex; gap:10px; align-items:center; flex-wrap:wrap;
    }}
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

    /* Tight cell blocks */
    .teamcell .teamabbr {{ font-weight: 700; letter-spacing: .3px; }}
    .teamcell .teamname {{ margin-top: 2px; }}
    .cellblock {{
      border: 1px solid rgba(255,255,255,.06);
      background: rgba(0,0,0,.12);
      border-radius: 12px;
      padding: 8px 8px;
      min-width: 0;
    }}
    .cellblock .row1, .cellblock .row2 {{
      display: flex;
      gap: 6px;
      align-items: baseline;
      justify-content: space-between;
    }}
    .cellblock .row2 {{ margin-top: 4px; }}
    .cellblock .k {{ color: var(--muted); font-size: 11px; min-width: 22px; }}
    .cellblock .sep {{ color: rgba(255,255,255,.18); }}
    .cellblock .pg {{ margin-top: 6px; font-size: 11px; }}

    /* Movers */
    table.movers th, table.movers td {{ padding: 7px 8px; }}
    table.movers td.team {{ font-weight: 700; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div>
        <h1>{_escape(title)}</h1>
        <div class="sub">Run date: {run_date} • Main tables use <b>km/60</b> pace • Δ is <b>Last10 − Season</b></div>
      </div>
      <div class="smalllink sub">
        <a href="archive/{run_date}.html">Open archive</a>
      </div>
    </div>

    <div class="card">
      <div class="sub"><b>Interpretation</b></div>
      <div class="muted" style="margin-top:6px; line-height:1.4;">
        Season pace comes directly from EDGE (<code>distancePer60.metric</code>). Last10 pace is computed from the last 10 games:
        <code>SUM(distance)</code> ÷ <code>(SUM(TOI)/3600)</code>. Totals are shown in the cell footer for context.
      </div>
    </div>

    <!-- ALL movers -->
    <div class="grid two">
      <div class="card">
        <div class="sub" style="margin-bottom:10px;"><b>Top 10 ↑</b> — All Situations (Δ % km/60)</div>
        {movers_table(all_up)}
      </div>
      <div class="card">
        <div class="sub" style="margin-bottom:10px;"><b>Top 10 ↓</b> — All Situations (Δ % km/60)</div>
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
        <span class="pill">Tight view: 1 row per team</span>
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
        Full schema is saved to CSV; this page is optimized for quick daily scanning.
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
    const rows = Array.from(tbody.querySelectorAll("tr"));
    rows.forEach(r => {{
      if (!q) {{ r.style.display = ""; return; }}
      r.style.display = r.textContent.toLowerCase().includes(q) ? "" : "none";
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

    teams = [Team(team_id=t["id"], abbrev=t["abbrev"], name=t["name"]) for t in NHL_TEAMS]
    teams = sorted(teams, key=lambda t: t.abbrev)

    # Full schema outputs
    wide_rows_full: List[Dict[str, Any]] = []
    long_rows_full: List[Dict[str, Any]] = []

    # Tight metrics store: metrics_by_team[TEAM_ABBREV][metric_key] -> MetricValue
    metrics_by_team: Dict[str, Dict[str, MetricValue]] = {}

    for team in teams:
        url = f"{BASE}/v1/edge/team-skating-distance-detail/{team.team_id}/now"
        payload = fetch_json(url, timeout=args.timeout, retries=args.retries)

        # raw json
        write_json(os.path.join(raw_dir, f"{team.abbrev}_{team.team_id}.json"), payload)

        # full schema flatten
        flat = flatten_numeric_leaves(payload)

        # Full wide row (everything numeric)
        wide_row: Dict[str, Any] = {
            "date": run_date,
            "teamId": team.team_id,
            "team": team.abbrev,
            "teamName": team.name,
        }
        wide_row.update(flat)
        wide_rows_full.append(wide_row)

        # Full long rows (everything numeric)
        long_rows_full.extend(to_long_rows(team, flat, run_date))

        # Tight metrics
        metrics_by_team[team.abbrev] = {}
        for spec in METRIC_SPECS:
            metrics_by_team[team.abbrev][spec["key"]] = extract_metric(payload, spec)

        time.sleep(max(0.0, args.sleep))

    # ---- Full CSVs (schema complete) ----
    df_wide_full = pd.DataFrame(wide_rows_full)
    df_long_full = pd.DataFrame(long_rows_full)

    id_cols = ["date", "teamId", "team", "teamName"]
    other_cols = [c for c in df_wide_full.columns if c not in id_cols]
    df_wide_full = df_wide_full[id_cols + sorted(other_cols)]

    wide_csv = os.path.join(outdir, f"team_skating_distance_detail_wide_{run_date}.csv")
    long_csv = os.path.join(outdir, f"team_skating_distance_detail_long_{run_date}.csv")
    df_wide_full.to_csv(wide_csv, index=False)
    df_long_full.to_csv(long_csv, index=False)

    df_wide_full.to_csv(os.path.join(outdir, "latest_wide.csv"), index=False)
    df_long_full.to_csv(os.path.join(outdir, "latest_long.csv"), index=False)

    # ---- Tight HTML ----
    html_doc = make_html_tight(run_date, teams, metrics_by_team)
    latest_html_path = os.path.join(docsdir, "latest.html")
    archive_html_path = os.path.join(archive_dir, f"{run_date}.html")

    with open(latest_html_path, "w", encoding="utf-8") as f:
        f.write(html_doc)
    with open(archive_html_path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    print("Wrote:")
    print(f"- {wide_csv} (full schema)")
    print(f"- {long_csv} (full schema)")
    print(f"- {latest_html_path} (tight view)")
    print(f"- {archive_html_path} (tight view)")
    print(f"Raw JSON in: {raw_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
