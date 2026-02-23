#!/usr/bin/env python3
"""
edge_instagram_card.py
──────────────────────────────────────────────────────────────────
Generates a single 1080x1080 Instagram-ready PNG from the NHL EDGE
team skating distance API — same data source as the daily HTML report.

Aesthetic: broadcast sports editorial (dark charcoal, ice-blue +
electric-yellow accents, hard geometry, condensed type).

What's on the card:
  • Header — "EDGE SKATING" brand + date
  • Top 5 teams by season km/60  (all-situations)
  • Bottom 5 teams (least skating)
  • Top 5 risers  (Last10 − Season Δ%)
  • Top 5 fallers
  • Mini league-range bar for each team entry

Output:
  docs/instagram/YYYY-MM-DD_edge_skating.png
  docs/instagram/latest_edge_skating.png

Usage:
    python edge_instagram_card.py
    python edge_instagram_card.py --date 2025-11-01
    python edge_instagram_card.py --outdir docs/instagram

Deps: pip install pillow requests pandas
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    raise SystemExit("Missing Pillow — run: pip install pillow")

# ─────────────────────────────────────────────────────────────────
# DESIGN TOKENS
# ─────────────────────────────────────────────────────────────────
W = H = 1080
PAD   = 52
TZ    = ZoneInfo("America/Toronto")
API   = "https://api-web.nhle.com"

C_BG      = (8,  11,  20)
C_BG2     = (13,  18,  32)
C_BG3     = (20,  26,  44)
C_ICE     = (0,  200, 255)
C_YELLOW  = (255, 210,   0)
C_WHITE   = (242, 246, 255)
C_MUTED   = (110, 132, 168)
C_DIM     = (45,   56,  80)
C_GREEN   = (0,  215, 110)
C_RED     = (255,  70,  70)
C_ORANGE  = (255, 145,  30)

# ─────────────────────────────────────────────────────────────────
# FONTS
# ─────────────────────────────────────────────────────────────────
_BOLD  = "/usr/share/fonts/truetype/google-fonts/Poppins-Bold.ttf"
_REG   = "/usr/share/fonts/truetype/google-fonts/Poppins-Regular.ttf"
_MED   = "/usr/share/fonts/truetype/google-fonts/Poppins-Medium.ttf"
_COND  = "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed-Bold.ttf"
_COND2 = "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf"

def _fp(p: str, fb: str = "") -> str:
    return p if os.path.exists(p) else fb

def F(size: int, style: str = "bold") -> ImageFont.FreeTypeFont:
    m = {
        "bold": _fp(_BOLD,  _fp(_COND,  "")),
        "reg":  _fp(_REG,   _fp(_COND2, "")),
        "med":  _fp(_MED,   _fp(_COND2, "")),
        "cond": _fp(_COND,  _fp(_BOLD,  "")),
    }
    p = m.get(style, "")
    return ImageFont.truetype(p, size) if p else ImageFont.load_default()

# ─────────────────────────────────────────────────────────────────
# NHL TEAMS — same list as the main script
# ─────────────────────────────────────────────────────────────────
NHL_TEAMS = [
    (1,"NJD"),(2,"NYI"),(3,"NYR"),(4,"PHI"),(5,"PIT"),
    (6,"BOS"),(7,"BUF"),(8,"MTL"),(9,"OTT"),(10,"TOR"),
    (12,"CAR"),(13,"FLA"),(14,"TBL"),(15,"WSH"),(16,"CHI"),
    (17,"DET"),(18,"NSH"),(19,"STL"),(20,"CGY"),(21,"COL"),
    (22,"EDM"),(23,"VAN"),(24,"ANA"),(25,"DAL"),(26,"LAK"),
    (28,"SJS"),(29,"CBJ"),(30,"MIN"),(52,"WPG"),(54,"VGK"),
    (55,"SEA"),(59,"UTA"),
]

# ─────────────────────────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────────────────────────
_S = requests.Session()
_S.headers["User-Agent"] = "nhl-edge-ig-card/1.0"

def _get(url: str) -> Dict:
    r = _S.get(url, timeout=25)
    r.raise_for_status()
    return r.json()

def _try(url: str) -> Optional[Dict]:
    try: return _get(url)
    except Exception as e:
        print(f"  [warn] {url}: {e}")
        return None

def _details_row(payload: dict, sc: str = "all") -> Optional[dict]:
    for r in (payload.get("skatingDistanceDetails") or []):
        if isinstance(r, dict) and r.get("strengthCode") == sc \
                and r.get("positionCode") == "all":
            return r
    return None

def _fv(obj: Any, *keys: str) -> Optional[float]:
    cur = obj
    for k in keys:
        if not isinstance(cur, dict): return None
        cur = cur.get(k)
    try: return float(cur) if cur is not None else None
    except Exception: return None

def _last10_km60(payload: dict) -> Optional[float]:
    """Compute last-10 km/60 for all-situations."""
    games = payload.get("skatingDistanceLast10") or []
    if not games: return None
    dist = toi = 0.0; nd = nt = 0
    for g in games:
        if not isinstance(g, dict): continue
        v = _fv(g, "distanceSkatedAll", "metric")
        if v is not None: dist += v; nd += 1
        t = g.get("toiAll")
        if isinstance(t, (int, float)): toi += float(t); nt += 1
    if nd == 0 or toi == 0: return None
    return dist / (toi / 3600.0)

def fetch_all_teams(sleep: float = 0.12) -> List[Dict]:
    """Fetch EDGE skating distance for all teams. Returns list of dicts."""
    rows = []
    for tid, abbrev in NHL_TEAMS:
        url = f"{API}/v1/edge/team-skating-distance-detail/{tid}/now"
        payload = _try(url)
        if not payload:
            rows.append({"abbrev": abbrev, "s_p60": None, "l10_p60": None,
                         "delta": None, "pct": None, "s_total": None})
            time.sleep(sleep); continue

        det = _details_row(payload, "all")
        s_p60   = _fv(det, "distancePer60", "metric")
        s_total = _fv(det, "distanceTotal", "metric")
        l10_p60 = _last10_km60(payload)
        delta   = (l10_p60 - s_p60) if (l10_p60 and s_p60) else None
        pct     = (delta / s_p60 * 100) if (delta and s_p60) else None

        rows.append({"abbrev": abbrev, "s_p60": s_p60, "l10_p60": l10_p60,
                     "delta": delta, "pct": pct, "s_total": s_total})
        time.sleep(sleep)
    return rows

# ─────────────────────────────────────────────────────────────────
# CANVAS
# ─────────────────────────────────────────────────────────────────

def make_canvas() -> Tuple[Image.Image, ImageDraw.ImageDraw]:
    img  = Image.new("RGB", (W, H), C_BG)
    draw = ImageDraw.Draw(img, "RGBA")

    for y in range(H):
        t = y / H
        r = int(C_BG[0] + (C_BG2[0]-C_BG[0])*t)
        g = int(C_BG[1] + (C_BG2[1]-C_BG[1])*t)
        b = int(C_BG[2] + (C_BG2[2]-C_BG[2])*t)
        draw.line([(0,y),(W,y)], fill=(r,g,b))

    # Grid
    for x in range(0, W, 54):
        draw.line([(x,0),(x,H)], fill=(255,255,255,4))
    for y in range(0, H, 54):
        draw.line([(0,y),(W,y)], fill=(255,255,255,3))

    # Decorative concentric circles (top-right)
    for i in range(5):
        r = 300 - i*44
        a = 9 - i*1
        cx, cy = W-60, 100
        draw.ellipse([cx-r,cy-r,cx+r,cy+r], outline=(0,200,255,a), width=2)

    # Corner ticks
    col = (*C_ICE, 65); m=20; s=24; t=3
    for pts in [
        [(m,m+s),(m,m),(m+s,m)],
        [(W-m-s,m),(W-m,m),(W-m,m+s)],
        [(m,H-m-s),(m,H-m),(m+s,H-m)],
        [(W-m-s,H-m),(W-m,H-m),(W-m,H-m-s)],
    ]:
        for i in range(len(pts)-1):
            draw.line([pts[i],pts[i+1]], fill=col, width=t)

    return img, draw

# ─────────────────────────────────────────────────────────────────
# DRAW PRIMITIVES
# ─────────────────────────────────────────────────────────────────

def tw(draw: ImageDraw.ImageDraw, text: str, fnt) -> int:
    bb = draw.textbbox((0,0), text, font=fnt)
    return bb[2]-bb[0]

def ice_text(draw, x, y, text, fnt):
    bb = draw.textbbox((x,y), text, font=fnt)
    total = max(bb[2]-bb[0], 1); cx = x
    for ch in text:
        cb = draw.textbbox((cx,y), ch, font=fnt); cw = cb[2]-cb[0]
        t  = max(0., min(1., (cx-x)/total))
        col = tuple(int(C_WHITE[i]+(C_ICE[i]-C_WHITE[i])*t) for i in range(3))
        draw.text((cx,y), ch, font=fnt, fill=col); cx += cw

def yellow_text(draw, x, y, text, fnt):
    bb = draw.textbbox((x,y), text, font=fnt)
    total = max(bb[2]-bb[0], 1); cx = x
    for ch in text:
        cb = draw.textbbox((cx,y), ch, font=fnt); cw = cb[2]-cb[0]
        t   = max(0., min(1., (cx-x)/total))
        col = tuple(int(C_YELLOW[i]+(C_WHITE[i]-C_YELLOW[i])*t) for i in range(3))
        draw.text((cx,y), ch, font=fnt, fill=col); cx += cw

def chip(draw, x, y, text, fnt, bg=(255,255,255,18), fg=C_WHITE,
         border=None, px=10, py=4) -> int:
    bb = draw.textbbox((0,0), text, font=fnt)
    pw = bb[2]-bb[0]+px*2; ph = bb[3]-bb[1]+py*2
    draw.rectangle([x,y,x+pw,y+ph], fill=bg)
    if border: draw.rectangle([x,y,x+pw,y+ph], outline=border, width=1)
    draw.text((x+px, y+py), text, font=fnt, fill=fg)
    return x+pw

def slash_div(draw, y, x0=PAD, x1=W-PAD, color=C_DIM, w=1):
    draw.line([(x0+14,y),(x1-14,y)], fill=color, width=w)
    draw.line([(x0,y+7),(x0+12,y-7)], fill=color, width=w)
    draw.line([(x1-12,y+7),(x1,y-7)], fill=color, width=w)

def section_hdr(draw, y, label, sub="") -> int:
    draw.rectangle([PAD,y,PAD+4,y+32], fill=C_YELLOW)
    f = F(18,"bold")
    draw.text((PAD+14, y+5), label.upper(), font=f, fill=C_WHITE)
    if sub:
        fs = F(12,"reg")
        draw.text((PAD+14+tw(draw,label.upper(),f)+12, y+10),
                  sub, font=fs, fill=C_MUTED)
    return y+42

def draw_header(draw, date_str: str) -> int:
    draw.rectangle([0,0,W,90], fill=(*C_BG,255))
    draw.rectangle([0,86,W,90], fill=C_ICE)

    f_brand = F(36,"bold")
    draw.text((PAD,18), "EDGE", font=f_brand, fill=C_YELLOW)
    ew = tw(draw,"EDGE",f_brand)
    draw.text((PAD+ew+10,18), "SKATING", font=f_brand, fill=C_WHITE)
    bw = ew+10+tw(draw,"SKATING",f_brand)
    draw.line([(PAD+bw+20,22),(PAD+bw+20,66)], fill=(*C_DIM,180), width=1)

    f_date = F(16,"bold"); f_tag = F(12,"reg")
    dw = tw(draw,date_str,f_date)
    draw.text((W-PAD-dw,20), date_str, font=f_date, fill=C_YELLOW)
    tag = "NHL EDGE API · km/60"
    draw.text((W-PAD-tw(draw,tag,f_tag),48), tag, font=f_tag, fill=C_MUTED)
    return 106

def _footer(draw):
    fy = H-PAD+12
    draw.text((PAD,fy), "Source: api-web.nhle.com / edge/team-skating-distance-detail",
              font=F(11,"reg"), fill=(*C_MUTED,130))
    chip(draw, W-PAD-126, fy-4, "@nhl_edge", F(12,"bold"),
         bg=(*C_ICE,20), fg=C_ICE, border=(*C_ICE,48))

# ─────────────────────────────────────────────────────────────────
# MINI INLINE BAR  (shows team vs league range)
# ─────────────────────────────────────────────────────────────────

def mini_bar(draw, x: int, y: int,
             val: Optional[float], lo: float, hi: float, avg: float,
             bar_w: int = 90, bar_h: int = 6,
             above_col=C_ICE, below_col=C_MUTED) -> None:
    if val is None or hi == lo: return
    span = hi - lo
    pv = max(0., min(1., (val-lo)/span))
    pa = max(0., min(1., (avg-lo)/span))
    col = above_col if val >= avg else below_col

    # Track
    draw.rectangle([x, y+1, x+bar_w, y+bar_h-1],
                   fill=(*C_DIM, 80))
    # Fill
    fw = max(2, int(pv*bar_w))
    draw.rectangle([x, y+1, x+fw, y+bar_h-1], fill=(*col,180))
    # Avg tick
    ax = x + int(pa*bar_w)
    draw.line([(ax,y),(ax,y+bar_h)], fill=(255,255,255,100), width=1)

# ─────────────────────────────────────────────────────────────────
# TEAM ROW
# ─────────────────────────────────────────────────────────────────

def team_row(draw, y: int, rank: int,
             abbrev: str, val: Optional[float],
             sub_val: Optional[float],   # l10 or pct
             sub_label: str,
             lo: float, hi: float, avg: float,
             val_col=C_WHITE,
             row_h: int = 52) -> int:

    # Row bg
    draw.rectangle([PAD, y, W-PAD, y+row_h-4], fill=(*C_BG3, 100))
    # Left rank bar — gold #1, silver #2, else dim
    bar_c = C_YELLOW if rank==1 else (C_MUTED if rank==2 else C_DIM)
    draw.rectangle([PAD, y, PAD+3, y+row_h-4], fill=bar_c)

    # Rank
    f_rk = F(13,"bold")
    draw.text((PAD+10, y+10), f"#{rank}", font=f_rk, fill=C_DIM)

    # Team abbrev
    f_team = F(22,"bold")
    draw.text((PAD+44, y+10), abbrev, font=f_team, fill=C_WHITE)

    # Mini bar
    bar_x = PAD+130
    mini_bar(draw, bar_x, y+18,
             val, lo, hi, avg, bar_w=160, bar_h=8,
             above_col=C_ICE if val_col==C_WHITE else val_col,
             below_col=C_MUTED)

    # Sub-label
    f_sub = F(11,"reg")
    draw.text((bar_x, y+30), f"lg avg {avg:.1f} km/60",
              font=f_sub, fill=C_DIM)

    # Main value — right side
    f_val = F(24,"bold")
    val_s = f"{val:.1f}" if val is not None else "-"
    vw    = tw(draw, val_s, f_val)
    if val_col == C_YELLOW:
        yellow_text(draw, W-PAD-vw-80, y+8, val_s, f_val)
    elif val_col == C_GREEN:
        draw.text((W-PAD-vw-80, y+8), val_s, font=f_val, fill=C_GREEN)
    elif val_col == C_RED:
        draw.text((W-PAD-vw-80, y+8), val_s, font=f_val, fill=C_RED)
    else:
        ice_text(draw, W-PAD-vw-80, y+8, val_s, f_val)

    # Sub value chip (l10 or Δ%)
    if sub_val is not None:
        s_str = f"{sub_label} {sub_val:+.1f}%" if "%" in sub_label else f"{sub_label} {sub_val:.1f}"
        s_col = C_GREEN if sub_val > 0 else (C_RED if sub_val < 0 else C_MUTED)
        s_bg  = (*s_col, 28)
        s_bd  = (*s_col, 55)
        chip(draw, W-PAD-78, y+10, s_str, F(11,"bold"),
             bg=s_bg, fg=s_col, border=s_bd, px=7, py=4)

    return y + row_h

# ─────────────────────────────────────────────────────────────────
# MOVER ROW  (for risers/fallers — shows Δ% prominently)
# ─────────────────────────────────────────────────────────────────

def mover_row(draw, y: int, rank: int,
              abbrev: str,
              s_p60: Optional[float],
              l10_p60: Optional[float],
              pct: Optional[float],
              row_h: int = 48) -> int:

    draw.rectangle([PAD, y, W-PAD, y+row_h-4], fill=(*C_BG3, 90))

    is_up  = (pct or 0) > 0
    accent = C_GREEN if is_up else C_RED
    draw.rectangle([PAD, y, PAD+3, y+row_h-4], fill=accent)

    f_rk   = F(12,"bold"); f_team = F(20,"bold")
    f_val  = F(13,"reg");  f_pct  = F(22,"bold")

    draw.text((PAD+10, y+10), f"#{rank}", font=f_rk, fill=C_DIM)
    draw.text((PAD+44, y+10), abbrev,     font=f_team, fill=C_WHITE)

    # Season km/60  →  L10 km/60
    if s_p60 is not None and l10_p60 is not None:
        detail = f"{s_p60:.1f}  →  {l10_p60:.1f} km/60"
        draw.text((PAD+130, y+12), detail, font=f_val, fill=C_MUTED)

    # Δ% large right
    pct_s = f"{pct:+.1f}%" if pct is not None else "—"
    pw = tw(draw, pct_s, f_pct)
    draw.text((W-PAD-pw, y+8), pct_s, font=f_pct, fill=accent)

    return y + row_h

# ─────────────────────────────────────────────────────────────────
# BUILD CARD
# ─────────────────────────────────────────────────────────────────

def build_card(rows: List[Dict], today: dt.date) -> Image.Image:
    img, draw = make_canvas()
    date_str = today.strftime("%b %-d · %Y")
    y = draw_header(draw, date_str)
    y += 6

    # Filter rows with valid data
    valid = [r for r in rows if r["s_p60"] is not None]
    if not valid:
        draw.text((PAD+20, y+20), "No EDGE data available.",
                  font=F(18,"reg"), fill=C_MUTED)
        _footer(draw)
        return img

    all_s   = [r["s_p60"]   for r in valid]
    lo, hi  = min(all_s), max(all_s)
    avg     = sum(all_s) / len(all_s)

    ranked  = sorted(valid, key=lambda r: -(r["s_p60"] or 0))
    # Assign correct 1-based league ranks to every team
    for rank_i, r in enumerate(ranked, 1):
        r["_rank"] = rank_i

    top5    = ranked[:5]
    # Bottom 5: worst skaters first (#32 → #28), so sort ascending
    bot5    = sorted(ranked[-5:], key=lambda r: (r["s_p60"] or 0))

    movers  = [r for r in valid if r["pct"] is not None]
    risers  = sorted(movers, key=lambda r: -(r["pct"] or 0))[:5]
    fallers = sorted(movers, key=lambda r: (r["pct"] or 0))[:5]

    # ── TOP 5 ─────────────────────────────────────────────────────
    y = section_hdr(draw, y, "Top 5 — Season km/60",
                    "· most distance per 60 min")
    for r in top5:
        y = team_row(draw, y, r["_rank"],
                     r["abbrev"], r["s_p60"],
                     r["l10_p60"], "L10",
                     lo, hi, avg,
                     val_col=C_YELLOW if r["_rank"]==1 else C_WHITE)
        y += 3

    y += 8
    slash_div(draw, y, color=(*C_ICE, 35)); y += 16

    # ── BOTTOM 5 ──────────────────────────────────────────────────
    y = section_hdr(draw, y, "Bottom 5 — Season km/60",
                    "· least distance per 60 min")
    for i, r in enumerate(bot5, 1):
        y = team_row(draw, y, r["_rank"],
                     r["abbrev"], r["s_p60"],
                     r["l10_p60"], "L10",
                     lo, hi, avg,
                     val_col=C_RED)
        y += 3

    y += 8
    slash_div(draw, y, color=(*C_YELLOW, 30)); y += 16

    # ── RISERS & FALLERS side-by-side ─────────────────────────────
    # Two columns
    col_w = (W - PAD*2 - 14) // 2
    lx = PAD; rx = PAD + col_w + 14

    # Section titles
    f_sec = F(15,"bold"); f_sub_s = F(11,"reg")

    # Left — risers
    draw.rectangle([lx, y, lx+4, y+30], fill=C_GREEN)
    draw.text((lx+12, y+4), "RISING", font=f_sec, fill=C_WHITE)
    draw.text((lx+12+tw(draw,"RISING",f_sec)+8, y+9),
              "Δ% L10 vs Season", font=f_sub_s, fill=C_MUTED)

    # Right — fallers
    draw.rectangle([rx, y, rx+4, y+30], fill=C_RED)
    draw.text((rx+12, y+4), "FALLING", font=f_sec, fill=C_WHITE)
    draw.text((rx+12+tw(draw,"FALLING",f_sec)+8, y+9),
              "Δ% L10 vs Season", font=f_sub_s, fill=C_MUTED)

    y += 38

    f_rk   = F(12,"bold"); f_t = F(18,"bold")
    f_v    = F(12,"reg");  f_p = F(19,"bold")

    for i in range(5):
        ry = y + i * 46

        # Riser row
        if i < len(risers):
            r   = risers[i]
            acc = C_GREEN
            draw.rectangle([lx, ry, lx+col_w, ry+42], fill=(*C_BG3,80))
            draw.rectangle([lx, ry, lx+3, ry+42], fill=acc)
            draw.text((lx+10, ry+6), f"#{i+1}", font=f_rk, fill=C_DIM)
            draw.text((lx+38, ry+6), r["abbrev"], font=f_t, fill=C_WHITE)
            if r["s_p60"] and r["l10_p60"]:
                det = f"{r['s_p60']:.1f} > {r['l10_p60']:.1f}"
                draw.text((lx+38+tw(draw,r["abbrev"],f_t)+8, ry+12),
                          det, font=f_v, fill=C_MUTED)
            ps = f"{r['pct']:+.1f}%" if r["pct"] is not None else "-"
            pw2 = tw(draw, ps, f_p)
            draw.text((lx+col_w-pw2-6, ry+6), ps, font=f_p, fill=acc)

        # Faller row
        if i < len(fallers):
            r   = fallers[i]
            acc = C_RED
            draw.rectangle([rx, ry, rx+col_w, ry+42], fill=(*C_BG3,80))
            draw.rectangle([rx, ry, rx+3, ry+42], fill=acc)
            draw.text((rx+10, ry+6), f"#{i+1}", font=f_rk, fill=C_DIM)
            draw.text((rx+38, ry+6), r["abbrev"], font=f_t, fill=C_WHITE)
            if r["s_p60"] and r["l10_p60"]:
                det = f"{r['s_p60']:.1f} > {r['l10_p60']:.1f}"
                draw.text((rx+38+tw(draw,r["abbrev"],f_t)+8, ry+12),
                          det, font=f_v, fill=C_MUTED)
            ps = f"{r['pct']:+.1f}%" if r["pct"] is not None else "—"
            pw2 = tw(draw, ps, f_p)
            draw.text((rx+col_w-pw2-6, ry+6), ps, font=f_p, fill=acc)

    _footer(draw)
    return img

# ─────────────────────────────────────────────────────────────────
# MOCK DATA (for --mock flag / CI testing without network)
# ─────────────────────────────────────────────────────────────────

def mock_rows() -> List[Dict]:
    import random
    random.seed(42)
    data = [
        ("BOS",120.4,122.1),("CAR",119.8,121.0),("EDM",118.5,116.2),
        ("COL",117.9,119.5),("FLA",117.1,115.8),("TOR",116.4,118.9),
        ("VGK",115.8,114.2),("DAL",115.2,117.6),("NYR",114.8,113.5),
        ("WSH",114.3,116.0),("WPG",113.9,112.4),("MIN",113.5,114.8),
        ("TBL",113.1,111.9),("NSH",112.6,110.3),("STL",112.2,113.8),
        ("PIT",111.8,112.5),("CHI",111.4,109.7),("DET",110.9,112.1),
        ("BUF",110.5,108.8),("PHI",110.1,111.4),("MTL",109.7,107.2),
        ("CGY",109.3,110.6),("VAN",108.9,107.5),("OTT",108.5,109.8),
        ("NJD",108.1,106.4),("LAK",107.7,108.9),("ANA",107.3,105.8),
        ("SEA",106.9,107.2),("CBJ",106.5,104.1),("SJS",106.1,107.3),
        ("UTA",105.7,103.2),("NYI",105.3,106.8),
    ]
    rows = []
    for abbrev, s, l in data:
        delta = l - s
        pct   = delta/s*100 if s else None
        rows.append({"abbrev":abbrev,"s_p60":s,"l10_p60":l,
                     "delta":delta,"pct":pct,"s_total":s*60})
    return rows

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date",   default=None,
                    help="YYYY-MM-DD (default: today ET)")
    ap.add_argument("--outdir", default="docs/instagram",
                    help="Output directory")
    ap.add_argument("--mock",   action="store_true",
                    help="Use mock data (no network required)")
    ap.add_argument("--sleep",  type=float, default=0.12,
                    help="Delay between API calls (s)")
    args = ap.parse_args()

    now   = dt.datetime.now(TZ)
    today = dt.date.fromisoformat(args.date) if args.date else now.date()
    out   = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    if args.mock:
        print("[EDGE IG] Using mock data")
        rows = mock_rows()
    else:
        print(f"[EDGE IG] Fetching all 32 teams for {today}...")
        rows = fetch_all_teams(sleep=args.sleep)

    print("[EDGE IG] Building card...")
    img = build_card(rows, today)

    p1 = out / f"{today.isoformat()}_edge_skating.png"
    p2 = out / "latest_edge_skating.png"
    img.save(p1, "PNG", optimize=True)
    img.save(p2, "PNG", optimize=True)
    print(f"  [OK] {p1}")
    print(f"  [OK] {p2}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
