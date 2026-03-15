#!/usr/bin/env python3
"""
Daily NHL EDGE — Team Skating Distance Detail
(Hard-coded Team IDs) + Tight (Low-Scroll) GitHub Pages HTML

EDGE Endpoint:
  GET https://api-web.nhle.com/v1/edge/team-skating-distance-detail/{team-id}/now

v8.1:
- Drops frozen distancePer60.metric — not updated game-to-game by NHL.
- Compares G1-5 (last 5 games) vs G6-10 (prior 5), both computed from
  skatingDistanceLast10: SUM(distance) / (SUM(TOI)/3600).
- Adds two small inline bars per metric cell: G6-10 (grey) vs G1-5 (green/red/blue).
  Bars scale relative to each other; longer value = 100% width.
- Movers tables also use bars.

Dependencies: pip install requests pandas
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


METRIC_SPECS = [
    {"key": "all", "label": "All Situations", "desc": "All situations km/60. G1-5 vs G6-10.",
     "last10_dist_field": "distanceSkatedAll.metric",  "last10_toi_key": "toiAll"},
    {"key": "es",  "label": "Even Strength",  "desc": "Even-strength km/60. G1-5 vs G6-10.",
     "last10_dist_field": "distanceSkatedEven.metric", "last10_toi_key": "toiEven"},
    {"key": "pp",  "label": "Power Play",     "desc": "Power-play km/60. G1-5 vs G6-10.",
     "last10_dist_field": "distanceSkatedPP.metric",   "last10_toi_key": "toiPP"},
    {"key": "pk",  "label": "Penalty Kill",   "desc": "Penalty-kill km/60. G1-5 vs G6-10.",
     "last10_dist_field": "distanceSkatedPK.metric",   "last10_toi_key": "toiPK"},
]


# ── Yesterday data lookup ────────────────────────────────────────────────────

# Maps metric key → (dist field suffix, toi field suffix) in the wide CSV columns
_METRIC_CSV_FIELDS = {
    "all": ("distanceSkatedAll.metric", "toiAll"),
    "pp":  ("distanceSkatedPP.metric",  "toiPP"),
    "pk":  ("distanceSkatedPK.metric",  "toiPK"),
}

def _csv_pace(df, dist_suffix, toi_suffix, g_start, g_end):
    """Compute km/60 pace from wide-CSV columns for games [g_start, g_end)."""
    dist_cols = [c for c in df.columns
                 if c.startswith("skatingDistanceLast10[") and dist_suffix in c
                 and any(f"[{i}]" in c for i in range(g_start, g_end))]
    toi_cols  = [c for c in df.columns
                 if c.startswith("skatingDistanceLast10[") and c.endswith(f"].{toi_suffix}")
                 and any(f"[{i}]" in c for i in range(g_start, g_end))]
    if not dist_cols or not toi_cols:
        return pd.Series([None] * len(df))
    d = df[dist_cols].sum(axis=1)
    t = df[toi_cols].sum(axis=1)
    return (d / (t / 3600.0)).where(t > 0)

def load_yesterday_data(outdir: str, run_date: str) -> Dict[str, Any]:
    """
    Load yesterday's wide CSV and return:
      {
        "pace_ranks":  {abbrev: rank},           # overall All Situations G1-5 pace rank
        "tile_deltas": {metric_key: {abbrev: delta_per60}}  # delta km/60 per metric
      }
    Returns empty dicts if yesterday's file doesn't exist.
    """
    today = dt.date.fromisoformat(run_date)
    yesterday = (today - dt.timedelta(days=1)).isoformat()
    csv_path = os.path.join(outdir, f"team_skating_distance_detail_wide_{yesterday}.csv")
    if not os.path.exists(csv_path):
        return {"pace_ranks": {}, "tile_deltas": {}}
    try:
        df = pd.read_csv(csv_path)
        result: Dict[str, Any] = {"pace_ranks": {}, "tile_deltas": {}}

        # Overall pace rank from All Situations G1-5
        all_pace = _csv_pace(df, "distanceSkatedAll.metric", "toiAll", 0, 5)
        df["_all_pace"] = all_pace
        ranked = df.dropna(subset=["_all_pace"]).sort_values("_all_pace", ascending=False).reset_index(drop=True)
        result["pace_ranks"] = {row["team"]: int(idx + 1) for idx, row in ranked.iterrows()}

        # Per-metric delta km/60 (G1-5 pace minus G6-10 pace) for tile position ranking
        for mk, (dist_suf, toi_suf) in _METRIC_CSV_FIELDS.items():
            g15  = _csv_pace(df, dist_suf, toi_suf, 0, 5)
            g610 = _csv_pace(df, dist_suf, toi_suf, 5, 10)
            delta = g15 - g610
            result["tile_deltas"][mk] = dict(
                zip(df["team"], delta.where(g15.notna() & g610.notna()))
            )

        return result
    except Exception as e:
        print(f"Warning: could not load yesterday data from {csv_path}: {e}", file=sys.stderr)
        return {"pace_ranks": {}, "tile_deltas": {}}


# ── HTTP + IO ────────────────────────────────────────────────────────────────

def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def fetch_json(url: str, timeout: int = 30, retries: int = 3, backoff: float = 1.6) -> Any:
    headers = {"User-Agent": "nhl-edge-skating-distance-daily/8.1", "Accept": "application/json"}
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

def flatten_numeric_leaves(obj: Any, prefix: str = "") -> Dict[str, float]:
    out: Dict[str, float] = {}
    def rec(x: Any, p: str) -> None:
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            out[p] = float(x); return
        if isinstance(x, dict):
            for k, v in x.items(): rec(v, f"{p}.{k}" if p else str(k))
        elif isinstance(x, list):
            for i, v in enumerate(x): rec(v, f"{p}[{i}]" if p else f"[{i}]")
    rec(obj, prefix)
    return out

def to_long_rows(team: Team, flat: Dict[str, float], run_date: str) -> List[Dict[str, Any]]:
    return [{"date": run_date, "teamId": team.team_id, "team": team.abbrev,
             "teamName": team.name, "metric_path": path, "value": val}
            for path, val in flat.items()]


# ── Pace computation ─────────────────────────────────────────────────────────

def _slice_pace(games: list, dist_field_path: str, toi_key: str
                ) -> Tuple[Optional[float], Optional[float], int]:
    a, b = dist_field_path.split(".", 1)
    sum_km = sum_toi = 0.0
    n = 0
    for g in games:
        if not isinstance(g, dict): continue
        dist_obj = g.get(a)
        v = (dist_obj or {}).get(b) if isinstance(dist_obj, dict) else None
        t = g.get(toi_key)
        if (isinstance(v, (int, float)) and not isinstance(v, bool)
                and isinstance(t, (int, float)) and not isinstance(t, bool) and float(t) > 0):
            sum_km += float(v); sum_toi += float(t); n += 1
    return (sum_km, sum_toi, n) if n else (None, None, 0)

def _km_per60(sum_km: Optional[float], sum_toi_s: Optional[float]) -> Optional[float]:
    if sum_km is None or sum_toi_s is None or sum_toi_s <= 0: return None
    return sum_km / (sum_toi_s / 3600.0)


@dataclass
class MetricValue:
    older_km_per60:  Optional[float]   # G6-10
    recent_km_per60: Optional[float]   # G1-5
    full_l10_km:     Optional[float]
    delta_per60:     Optional[float]
    pct_per60:       Optional[float]
    recent_n: int
    older_n:  int

def extract_metric(payload: dict, spec: dict) -> MetricValue:
    games = payload.get("skatingDistanceLast10") or []
    if not isinstance(games, list): games = []
    df, tk = spec["last10_dist_field"], spec["last10_toi_key"]
    rkm, rtoi, rn = _slice_pace(games[:5], df, tk)
    okm, otoi, on = _slice_pace(games[5:], df, tk)
    fkm, _,    _  = _slice_pace(games,     df, tk)
    rp = _km_per60(rkm, rtoi)
    op = _km_per60(okm, otoi)
    delta = (rp - op) if (rp is not None and op is not None) else None
    pct   = (delta / op * 100.0) if (delta is not None and op) else None
    return MetricValue(older_km_per60=op, recent_km_per60=rp, full_l10_km=fkm,
                       delta_per60=delta, pct_per60=pct, recent_n=rn, older_n=on)


# ── HTML helpers ─────────────────────────────────────────────────────────────

def _escape(s: Any) -> str:
    return html.escape("" if s is None else str(s))

def _fmt_num(x: Any, d: int = 1) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)): return "—"
    try: return f"{float(x):,.{d}f}"
    except: return _escape(x)

def _fmt_signed(x: Any, d: int = 2) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)): return "—"
    try: return f"{float(x):+,.{d}f}"
    except: return _escape(x)

def _fmt_pct(x: Any, d: int = 1) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)): return "—"
    try: return f"{float(x):+,.{d}f}%"
    except: return _escape(x)

def _arrow(d: Optional[float]) -> str:
    """Return a green ▲ or red ▼ arrow span based on sign of delta, or empty string."""
    if d is None: return ""
    if d > 0: return "<span class='pos'>&#9650;</span> "
    if d < 0: return "<span class='neg'>&#9660;</span> "
    return ""

def _mini_bars(older: Optional[float], recent: Optional[float]) -> str:
    """
    Two proportional bars scaled to show divergence, not absolute values.
    The bar range spans midpoint ± (max_delta * padding), so small differences
    become visually obvious. Both bars always start from the left edge;
    the one that deviates more from midpoint fills more of the track.
    """
    if older is None or recent is None:
        return ""
    mid   = (older + recent) / 2.0
    delta = abs(recent - older)

    # Minimum visible range: at least 2% of midpoint so flat values
    # still render as two distinct (near-equal) bars rather than nothing.
    min_range = mid * 0.02
    half_range = max(delta * 1.5, min_range)

    lo = mid - half_range
    hi = mid + half_range

    def pct(v: float) -> float:
        return round(max(0.0, min(100.0, (v - lo) / (hi - lo) * 100)), 1)

    op = pct(older)
    rp = pct(recent)

    rc = "var(--pos)" if recent > older * 1.001 else ("var(--neg)" if recent < older * 0.999 else "var(--accent)")

    return (
        "<div class='minibars'>"
        "<div class='bar-row'><span class='bar-label'>G6-10</span>"
        "<div class='bar-track'><div class='bar-fill bar-older' "
        f"style='width:{op}%'></div></div>"
        f"<span class='bar-val muted'>{_fmt_num(older, 2)}</span></div>"
        "<div class='bar-row'><span class='bar-label'>G1-5</span>"
        "<div class='bar-track'><div class='bar-fill' "
        f"style='width:{rp}%;background:{rc}'></div></div>"
        f"<span class='bar-val' style='color:{rc}'>{_fmt_num(recent, 2)}</span></div>"
        "</div>"
    )


# ── Movers ───────────────────────────────────────────────────────────────────

def movers_for_metric(teams, metrics_by_team, metric_key):
    rows = []
    for t in teams:
        mv = metrics_by_team.get(t.abbrev, {}).get(metric_key)
        if not mv or mv.pct_per60 is None or mv.older_km_per60 is None or mv.recent_km_per60 is None:
            continue
        rows.append((t.abbrev, mv.older_km_per60, mv.recent_km_per60, mv.delta_per60 or 0.0, mv.pct_per60))
    return sorted(rows, key=lambda x: x[4], reverse=True)[:10], sorted(rows, key=lambda x: x[4])[:10]

def movers_table(rows, rank_deltas: Dict[str, int] = None) -> str:
    if not rows: return "<p class='muted'>Not available today.</p>"
    if rank_deltas is None:
        rank_deltas = {}
    trs = []
    for team, old, rec, d, p in rows:
        dc = "pos" if d > 0 else ("neg" if d < 0 else "")
        pc = "pos" if p > 0 else ("neg" if p < 0 else "")
        arrow = _arrow(d)
        rd = rank_deltas.get(team)
        if rd is not None and rd != 0:
            badge_cls = "rank-up" if rd > 0 else "rank-dn"
            badge_arrow = "▲" if rd > 0 else "▼"
            rank_badge = f"<span class='{badge_cls}'>{badge_arrow}{abs(rd)}</span>"
        else:
            rank_badge = ""
        trs.append(
            f"<tr><td class='team-cell'>{_escape(team)}{rank_badge}</td>"
            f"<td class='bars-cell'>{_mini_bars(old, rec)}</td>"
            f"<td class='num {dc}'>{arrow}{_fmt_signed(d)}</td>"
            f"<td class='num {pc}'>{_fmt_pct(p)}</td></tr>"
        )
    return (
        "<table class='movers'><thead><tr>"
        "<th>Team</th><th>G6-10 vs G1-5 (km/60)</th><th>Δ km/60</th><th>Δ %</th>"
        "</tr></thead><tbody>" + "\n".join(trs) + "</tbody></table>"
    )


# ── HTML page ────────────────────────────────────────────────────────────────

def make_html_tight(run_date, teams, metrics_by_team,
                    rank_deltas: Dict[str, int] = None,
                    tile_rank_deltas: Dict[str, Dict[str, int]] = None,
                    title="NHL EDGE — Team Skating Distance (Tight View)") -> str:
    if rank_deltas is None:
        rank_deltas = {}
    if tile_rank_deltas is None:
        tile_rank_deltas = {}
    keys   = [m["key"]   for m in METRIC_SPECS]
    labels = {m["key"]: m["label"] for m in METRIC_SPECS}
    descs  = {m["key"]: m["desc"]  for m in METRIC_SPECS}

    all_up, all_dn = movers_for_metric(teams, metrics_by_team, "all")
    pp_up,  pp_dn  = movers_for_metric(teams, metrics_by_team, "pp")
    pk_up,  pk_dn  = movers_for_metric(teams, metrics_by_team, "pk")

    def cell(mv: MetricValue) -> str:
        d, p = mv.delta_per60, mv.pct_per60
        dc = "pos" if (d is not None and d > 0) else ("neg" if (d is not None and d < 0) else "")
        pc = "pos" if (p is not None and p > 0) else ("neg" if (p is not None and p < 0) else "")
        arrow = _arrow(d)
        return (
            "<div class='cellblock'>"
            f"{_mini_bars(mv.older_km_per60, mv.recent_km_per60)}"
            f"<div class='delta-row'><span class='k'>Δ</span> "
            f"<span class='num {dc}'>{arrow}{_fmt_signed(d)}</span>"
            f"<span class='sep'>&nbsp;|&nbsp;</span>"
            f"<span class='k'>%</span> <span class='num {pc}'>{_fmt_pct(p)}</span></div>"
            f"<div class='pg muted'>G1-5: {mv.recent_n} gms &nbsp;·&nbsp; G6-10: {mv.older_n} gms</div>"
            "</div>"
        )

    body_rows = []
    for t in teams:
        tm = metrics_by_team.get(t.abbrev, {})
        delta = rank_deltas.get(t.abbrev)
        if delta is not None and delta != 0:
            badge_cls = "rank-up" if delta > 0 else "rank-dn"
            badge_arrow = "▲" if delta > 0 else "▼"
            badge = f"<span class='{badge_cls}'>{badge_arrow}{abs(delta)}</span>"
        else:
            badge = ""
        tds = [f"<td class='teamcell'><div class='teamabbr'>{_escape(t.abbrev)}{badge}</div>"
               f"<div class='teamname muted'>{_escape(t.name)}</div></td>"]
        for k in keys:
            mv = tm.get(k)
            tds.append(f"<td>{cell(mv)}</td>" if mv else "<td><div class='cellblock muted'>—</div></td>")
        body_rows.append("<tr>" + "".join(tds) + "</tr>")

    header = ("<tr><th>Team</th>"
              + "".join(f"<th title='{_escape(descs[k])}'>{_escape(labels[k])}</th>" for k in keys)
              + "</tr>")

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{_escape(title)} — {run_date}</title>
  <style>
    :root {{
      --bg:#0b0f1a; --text:#e7eefc; --muted:#9fb0d0;
      --border:rgba(255,255,255,.08); --accent:#6ea8fe;
      --pos:#33d17a; --neg:#ff6b6b; --card:rgba(18,26,42,.75);
      --bar-track:rgba(255,255,255,.07); --bar-older:rgba(159,176,208,.40);
    }}
    body {{
      margin:0;
      background:
        radial-gradient(1200px 800px at 20% 0%,  rgba(110,168,254,.15),transparent 50%),
        radial-gradient(1200px 800px at 80% 20%, rgba(51,209,122,.10), transparent 55%),
        var(--bg);
      color:var(--text);
      font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial;
    }}
    .wrap {{ max-width:1100px; margin:0 auto; padding:18px; }}
    .topbar {{
      display:flex; gap:10px; align-items:baseline; justify-content:space-between;
      padding:14px 16px; border:1px solid var(--border); background:var(--card);
      border-radius:16px; box-shadow:0 10px 30px rgba(0,0,0,.35); backdrop-filter:blur(10px);
    }}
    h1 {{ font-size:18px; margin:0; letter-spacing:.2px; }}
    .sub {{ color:var(--muted); font-size:13px; }}
    .smalllink a {{ color:var(--accent); text-decoration:none; }}
    .smalllink a:hover {{ text-decoration:underline; }}
    .card {{
      border:1px solid var(--border); background:var(--card); border-radius:16px; padding:14px;
      box-shadow:0 10px 30px rgba(0,0,0,.28); backdrop-filter:blur(10px); margin-top:12px;
    }}
    .grid {{ display:grid; grid-template-columns:1fr; gap:12px; margin-top:12px; }}
    @media(min-width:980px) {{ .grid.two {{ grid-template-columns:1fr 1fr; }} }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .pos {{ color:var(--pos); }} .neg {{ color:var(--neg); }}
    table {{ width:100%; border-collapse:collapse; border-radius:14px; overflow:hidden; }}
    th,td {{ border-bottom:1px solid var(--border); padding:8px 10px; font-size:12px; vertical-align:top; }}
    th {{ position:sticky; top:0; background:rgba(18,26,42,.92); text-align:left; user-select:none; white-space:nowrap; }}
    tr:hover td {{ background:rgba(110,168,254,.06); }}
    td.num {{ text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }}
    .controls {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
    input[type="search"] {{
      width:min(520px,100%); padding:10px 12px; border-radius:12px;
      border:1px solid var(--border); background:rgba(0,0,0,.25); color:var(--text); outline:none;
    }}
    .pill {{ border:1px solid var(--border); background:rgba(0,0,0,.18); padding:8px 10px; border-radius:999px; font-size:12px; color:var(--muted); }}
    /* team cell */
    .teamcell .teamabbr {{ font-weight:700; letter-spacing:.3px; }}
    .teamcell .teamname {{ margin-top:2px; }}
    /* metric cell */
    .cellblock {{ border:1px solid rgba(255,255,255,.06); background:rgba(0,0,0,.12); border-radius:12px; padding:8px 10px; min-width:150px; }}
    /* mini bars */
    .minibars {{ display:flex; flex-direction:column; gap:5px; margin-bottom:8px; }}
    .bar-row   {{ display:flex; align-items:center; gap:6px; }}
    .bar-label {{ font-size:10px; color:var(--muted); min-width:30px; flex-shrink:0; letter-spacing:.2px; }}
    .bar-track {{ flex:1; height:6px; background:var(--bar-track); border-radius:99px; overflow:hidden; }}
    .bar-fill  {{ height:100%; border-radius:99px; transition:width .25s ease; }}
    .bar-older {{ background:var(--bar-older); }}
    .bar-val   {{ font-size:11px; font-variant-numeric:tabular-nums; min-width:34px; text-align:right; flex-shrink:0; }}
    /* delta row */
    .delta-row {{ display:flex; align-items:baseline; gap:4px; font-size:11px; margin-top:1px; }}
    .delta-row .k   {{ color:var(--muted); min-width:10px; }}
    .delta-row .sep {{ color:rgba(255,255,255,.2); }}
    .cellblock .pg  {{ margin-top:5px; }}
    /* rank delta badges */
    .rank-up {{ font-size:10px; font-weight:700; color:var(--pos); margin-left:5px; letter-spacing:0; }}
    .rank-dn {{ font-size:10px; font-weight:700; color:var(--neg); margin-left:5px; letter-spacing:0; }}
    /* movers */
    table.movers th, table.movers td {{ padding:7px 8px; }}
    table.movers td.team-cell {{ font-weight:700; min-width:36px; }}
    table.movers td.bars-cell {{ min-width:160px; }}
    table.movers .minibars    {{ margin-bottom:0; }}
  </style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div>
      <h1>{_escape(title)}</h1>
      <div class="sub">Run date: {run_date} &nbsp;•&nbsp; Pace in <b>km/60</b> &nbsp;•&nbsp; <b>G1-5</b> = last 5 games &nbsp;|&nbsp; <b>G6-10</b> = prior 5 &nbsp;•&nbsp; Δ = G1-5 − G6-10</div>
    </div>
    <div class="smalllink sub"><a href="archive/{run_date}.html">Archive</a></div>
  </div>

  <div class="card">
    <div class="sub"><b>Interpretation</b></div>
    <div class="muted" style="margin-top:6px;line-height:1.6;">
      All pace computed from <code>skatingDistanceLast10</code>: <code>SUM(distance) ÷ (SUM(TOI)/3600)</code>.
      NHL's pre-computed <code>distancePer60</code> is <b>not used</b> — it does not update game-to-game.
      Bars: G6-10 grey baseline vs G1-5 coloured recent. Green = up, red = down, blue = flat.
    </div>
  </div>

  <div class="grid two">
    <div class="card"><div class="sub" style="margin-bottom:10px;"><b>Top 10 ↑</b> — All Situations</div>{movers_table(all_up, tile_rank_deltas.get("all", {}))}</div>
    <div class="card"><div class="sub" style="margin-bottom:10px;"><b>Top 10 ↓</b> — All Situations</div>{movers_table(all_dn, tile_rank_deltas.get("all", {}))}</div>
  </div>
  <div class="grid two">
    <div class="card"><div class="sub" style="margin-bottom:10px;"><b>Top 10 ↑</b> — Power Play</div>{movers_table(pp_up, tile_rank_deltas.get("pp", {}))}</div>
    <div class="card"><div class="sub" style="margin-bottom:10px;"><b>Top 10 ↓</b> — Power Play</div>{movers_table(pp_dn, tile_rank_deltas.get("pp", {}))}</div>
    <div class="card"><div class="sub" style="margin-bottom:10px;"><b>Top 10 ↑</b> — Penalty Kill</div>{movers_table(pk_up, tile_rank_deltas.get("pk", {}))}</div>
    <div class="card"><div class="sub" style="margin-bottom:10px;"><b>Top 10 ↓</b> — Penalty Kill</div>{movers_table(pk_dn, tile_rank_deltas.get("pk", {}))}</div>
  </div>

  <div class="card">
    <div class="controls">
      <input id="search" type="search" placeholder="Filter teams…"/>
      <span class="pill" id="rowcount"></span>
      <span class="pill">Tight view · 1 row per team</span>
    </div>
    <div style="overflow:auto;border-radius:14px;margin-top:10px;">
      <table id="main">
        <thead>{header}</thead>
        <tbody>{"".join(body_rows)}</tbody>
      </table>
    </div>
    <div class="muted" style="margin-top:10px;">Full schema saved to CSV.</div>
  </div>
</div>
<script>
(function() {{
  const tbody=document.querySelector("#main tbody"),search=document.getElementById("search"),rowcount=document.getElementById("rowcount");
  function upd() {{ const r=Array.from(tbody.querySelectorAll("tr")); rowcount.textContent=r.filter(x=>x.style.display!=="none").length+" / "+r.length+" teams"; }}
  search.addEventListener("input",()=>{{ const q=search.value.trim().toLowerCase(); Array.from(tbody.querySelectorAll("tr")).forEach(r=>{{ r.style.display=(!q||r.textContent.toLowerCase().includes(q))?"":"none"; }}); upd(); }});
  upd();
}})();
</script>
</body>
</html>
"""


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir",  default="data/edge_skating_distance")
    ap.add_argument("--docsdir", default="docs")
    ap.add_argument("--sleep",   type=float, default=0.12)
    ap.add_argument("--timeout", type=int,   default=30)
    ap.add_argument("--retries", type=int,   default=3)
    ap.add_argument("--date",    default=None)
    args = ap.parse_args()

    run_date = args.date or dt.date.today().isoformat()
    raw_dir  = os.path.join(args.outdir, "raw", run_date)
    arch_dir = os.path.join(args.docsdir, "archive")
    for d in [args.outdir, raw_dir, args.docsdir, arch_dir]:
        safe_mkdir(d)

    teams = sorted([Team(t["id"], t["abbrev"], t["name"]) for t in NHL_TEAMS], key=lambda t: t.abbrev)

    # Load yesterday's data for rank comparisons
    yesterday_data = load_yesterday_data(args.outdir, run_date)
    yesterday_pace_ranks = yesterday_data["pace_ranks"]
    yesterday_tile_deltas = yesterday_data["tile_deltas"]  # {metric_key: {abbrev: delta}}

    wide_rows: List[Dict[str, Any]] = []
    long_rows: List[Dict[str, Any]] = []
    metrics_by_team: Dict[str, Dict[str, MetricValue]] = {}

    for team in teams:
        url     = f"{BASE}/v1/edge/team-skating-distance-detail/{team.team_id}/now"
        payload = fetch_json(url, timeout=args.timeout, retries=args.retries)
        write_json(os.path.join(raw_dir, f"{team.abbrev}_{team.team_id}.json"), payload)

        flat     = flatten_numeric_leaves(payload)
        wide_row = {"date": run_date, "teamId": team.team_id, "team": team.abbrev, "teamName": team.name}
        wide_row.update(flat)
        wide_rows.append(wide_row)
        long_rows.extend(to_long_rows(team, flat, run_date))
        metrics_by_team[team.abbrev] = {s["key"]: extract_metric(payload, s) for s in METRIC_SPECS}
        time.sleep(max(0.0, args.sleep))

    df_w = pd.DataFrame(wide_rows)
    df_l = pd.DataFrame(long_rows)
    id_c = ["date", "teamId", "team", "teamName"]
    df_w = df_w[id_c + sorted(c for c in df_w.columns if c not in id_c)]

    w_csv = os.path.join(args.outdir, f"team_skating_distance_detail_wide_{run_date}.csv")
    l_csv = os.path.join(args.outdir, f"team_skating_distance_detail_long_{run_date}.csv")
    df_w.to_csv(w_csv, index=False);  df_l.to_csv(l_csv, index=False)
    df_w.to_csv(os.path.join(args.outdir, "latest_wide.csv"), index=False)
    df_l.to_csv(os.path.join(args.outdir, "latest_long.csv"), index=False)

    # Compute today's All Situations pace rank (rank 1 = highest pace) for main table badge
    today_pace_sorted = sorted(
        [(t.abbrev, metrics_by_team[t.abbrev]["all"].recent_km_per60)
         for t in teams
         if metrics_by_team.get(t.abbrev, {}).get("all") and
            metrics_by_team[t.abbrev]["all"].recent_km_per60 is not None],
        key=lambda x: x[1], reverse=True
    )
    today_pace_ranks = {abbr: idx + 1 for idx, (abbr, _) in enumerate(today_pace_sorted)}

    # Main table badge: overall pace rank delta (positive = moved up)
    rank_deltas: Dict[str, int] = {}
    if yesterday_pace_ranks:
        for abbr, today_rank in today_pace_ranks.items():
            if abbr in yesterday_pace_ranks:
                rank_deltas[abbr] = yesterday_pace_ranks[abbr] - today_rank

    # Tile-specific rank deltas: position in each movers tile vs yesterday
    # For each metric, rank all teams by delta_per60 (up tile: descending, dn tile: ascending)
    # Compare today's top-10 position to yesterday's position in the same ranking.
    tile_rank_deltas: Dict[str, Dict[str, int]] = {}
    for mk in ("all", "pp", "pk"):
        yest_mk_deltas = yesterday_tile_deltas.get(mk, {})
        if not yest_mk_deltas:
            tile_rank_deltas[mk] = {}
            continue
        # Yesterday's full ranking by delta (descending for ↑ tile)
        yest_sorted_up = sorted(
            [(a, v) for a, v in yest_mk_deltas.items() if v is not None and not (isinstance(v, float) and __import__("math").isnan(v))],
            key=lambda x: x[1], reverse=True
        )
        yest_ranks_up = {abbr: idx + 1 for idx, (abbr, _) in enumerate(yest_sorted_up)}
        # Today's ranking for this metric
        today_mk_deltas = {
            t.abbrev: (metrics_by_team[t.abbrev][mk].delta_per60 or 0.0)
            for t in teams
            if metrics_by_team.get(t.abbrev, {}).get(mk) and
               metrics_by_team[t.abbrev][mk].delta_per60 is not None
        }
        today_sorted_up = sorted(today_mk_deltas.items(), key=lambda x: x[1], reverse=True)
        today_ranks_up = {abbr: idx + 1 for idx, (abbr, _) in enumerate(today_sorted_up)}
        # Delta: positive = moved up in this tile's ranking
        mk_tile_deltas = {}
        for abbr, today_rank in today_ranks_up.items():
            if abbr in yest_ranks_up:
                mk_tile_deltas[abbr] = yest_ranks_up[abbr] - today_rank
        tile_rank_deltas[mk] = mk_tile_deltas

    html_doc = make_html_tight(run_date, teams, metrics_by_team,
                               rank_deltas=rank_deltas,
                               tile_rank_deltas=tile_rank_deltas)
    lat  = os.path.join(args.docsdir, "latest.html")
    arch = os.path.join(arch_dir, f"{run_date}.html")
    for p in [lat, arch]:
        with open(p, "w", encoding="utf-8") as f: f.write(html_doc)

    print(f"Wrote: {w_csv}\n       {l_csv}\n       {lat}\n       {arch}\n       Raw → {raw_dir}/")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
