"""
modules/revenue_engineering.py
================================
Module 1: Revenue Engineering — The Financial Terminal
Disesuaikan untuk Lead Gen Property (bukan e-commerce).

Sections:
1. North Star Ribbon  — KPI utama per portfolio
2. Campaign Terminal  — Funnel breakdown (TOF vs BOF)
3. Context Graph      — Daily spend + CTR trend
4. AI Insight         — Per section
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

from utils.data_loader import DataLoader
from components.inline_insight import render_inline_insight

import re as _re

def _html(s: str):
    """Minify dan render HTML — hapus newline supaya Streamlit tidak salah parse."""
    s = _re.sub(r'>\s+<', '><', s)
    s = _re.sub(r'\s+', ' ', s).strip()
    st.markdown(s, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════

DARK_BG        = "#0E1117"
CARD_BG        = "#1B1F2B"
CARD_BORDER    = "#2D3348"
NEON_BLUE      = "#00D4FF"
NEON_GREEN     = "#00FF88"
NEON_PURPLE    = "#A855F7"
NEON_ORANGE    = "#FF6B35"
NEON_RED       = "#FF3B5C"
NEON_YELLOW    = "#FFD700"
TEXT_PRIMARY   = "#FFFFFF"
TEXT_SECONDARY = "#8892A0"

FUNNEL_COLORS = {
    'TOF'          : '#00D4FF',
    'MOF'          : '#A855F7',
    'BOF'          : '#FF6B35',
    'RET'          : '#00FF88',
    'UNCATEGORIZED': '#FF3B5C',
}

FUNNEL_LABELS = {
    'TOF'          : 'Top of Funnel',
    'MOF'          : 'Middle of Funnel',
    'BOF'          : 'Bottom of Funnel',
    'RET'          : 'Retention',
    'UNCATEGORIZED': 'Uncategorized',
}

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════

def fmt_idr(amount: float) -> str:
    """Format angka ke IDR."""
    return f"Rp {amount:,.0f}"

def fmt_num(n: float) -> str:
    return f"{int(n):,}"

def get_funnel_stage(campaign_name: str) -> str:
    name = str(campaign_name).lower()
    if 'awareness' in name:                       return 'TOF'
    if 'leads' in name or 'lead' in name:         return 'BOF'
    if 'retarget' in name or '|ret|' in name:     return 'RET'
    if 'mof' in name or '|mof|' in name:          return 'MOF'
    return 'UNCATEGORIZED'

def prepare_ads_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean & enrich ads DataFrame."""
    if df.empty:
        return df
    df = df.copy()

    # ── Standardize column names (handle variations) ──────────────
    col_map = {}
    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower == 'campaign name'  : col_map[col] = 'campaign_name'
        if col_lower == 'ad set name'    : col_map[col] = 'adset_name'
        if col_lower == 'amount spent'   : col_map[col] = 'spend'
        if col_lower == 'link clicks'    : col_map[col] = 'clicks'
    if col_map:
        df = df.rename(columns=col_map)

    # ── Ensure required columns exist ─────────────────────────────
    required_defaults = {
        'campaign_name': 'Unknown Campaign',
        'portfolio'    : 'Unknown',
        'impressions'  : 0,
        'clicks'       : 0,
        'spend'        : 0.0,
        'cpm'          : 0.0,
        'cpc'          : 0.0,
        'ctr'          : 0.0,
    }
    for col, default in required_defaults.items():
        if col not in df.columns:
            df[col] = default

    # ── Parse date ────────────────────────────────────────────────
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # ── Funnel stage dari campaign name ───────────────────────────
    df['funnel_stage'] = df['campaign_name'].astype(str).apply(get_funnel_stage)

    # ── Derived metrics (recalculate untuk akurasi) ───────────────
    df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0)
    df['clicks']      = pd.to_numeric(df['clicks'],      errors='coerce').fillna(0)
    df['spend']       = pd.to_numeric(df['spend'],       errors='coerce').fillna(0)

    df['ctr_calc'] = df['clicks'] / df['impressions'].clip(lower=1) * 100
    df['cpm_calc'] = df['spend']  / df['impressions'].clip(lower=1) * 1000
    df['cpc_calc'] = df['spend']  / df['clicks'].clip(lower=1)

    return df

def _html(html_string: str):
    """Render HTML - strip newlines supaya Streamlit tidak salah parse."""
    import re
    # Hapus newline dan spasi berlebih antar tag
    clean = re.sub(r'>\s+<', '><', html_string)
    clean = re.sub(r'\s+', ' ', clean).strip()
    st.markdown(clean, unsafe_allow_html=True)

def divider():
    _html('<div style="height:1px;background:linear-gradient(to right,transparent,#2D3348,transparent);margin:28px 0;"></div>')

def section_header(title: str):
    st.markdown(
        f'<div style="font-size:11px;font-weight:600;color:#8892A0;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:16px;">{title}</div>',
        unsafe_allow_html=True
    )

# ══════════════════════════════════════════════════════════════════
# SECTION 1: NORTH STAR RIBBON
# ══════════════════════════════════════════════════════════════════

def render_north_star(df: pd.DataFrame, portfolio: str):
    """
    North Star Ribbon — KPI utama.
    Metrics: Total Spend, Impressions, Clicks, CTR, CPM, CPC, Top Campaign
    """
    section_header("North Star Ribbon — Key Performance Indicators")

    if df.empty:
        st.info("Tidak ada data tersedia.")
        return

    # Aggregate
    total_spend       = df['spend'].sum()
    total_impressions = df['impressions'].sum()
    total_clicks      = df['clicks'].sum()
    avg_ctr           = total_clicks / max(total_impressions, 1) * 100
    avg_cpm           = total_spend  / max(total_impressions, 1) * 1000
    avg_cpc           = total_spend  / max(total_clicks, 1)

    # Top campaign by CTR
    camp_sum = df.groupby('campaign_name').agg(
        spend=('spend','sum'),
        impressions=('impressions','sum'),
        clicks=('clicks','sum'),
    ).reset_index()
    camp_sum['ctr'] = camp_sum['clicks'] / camp_sum['impressions'].clip(lower=1) * 100
    top_camp        = camp_sum.loc[camp_sum['ctr'].idxmax(), 'campaign_name'] \
                      if not camp_sum.empty else 'N/A'
    top_camp_ctr    = camp_sum['ctr'].max() if not camp_sum.empty else 0

    # ── Render 6 metric cards ────────────────────────────────────
    cols = st.columns(6)
    metrics = [
        ("Total Spend",   fmt_idr(total_spend),           "Ad budget used",       NEON_BLUE),
        ("Impressions",   fmt_num(total_impressions),      "Total ad views",       NEON_PURPLE),
        ("Clicks",        fmt_num(total_clicks),           "Total link clicks",    NEON_GREEN),
        ("Avg CTR",       f"{avg_ctr:.2f}%",              "Click-through rate",   NEON_YELLOW
         if avg_ctr >= 1.5 else NEON_RED),
        ("Avg CPM",       fmt_idr(avg_cpm),               "Cost per 1K views",    TEXT_SECONDARY),
        ("Avg CPC",       fmt_idr(avg_cpc),               "Cost per click",       TEXT_SECONDARY),
    ]

    for i, (label, value, note, color) in enumerate(metrics):
        with cols[i]:
            _html(
                f'<div style="background:{CARD_BG};border:1px solid {CARD_BORDER};border-top:3px solid {color};border-radius:8px;padding:14px 16px;">'
                f'<div style="font-size:10px;color:{TEXT_SECONDARY};text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">{label}</div>'
                f'<div style="font-size:20px;font-weight:700;color:{color};line-height:1.2;">{value}</div>'
                f'<div style="font-size:10px;color:#3A4055;margin-top:4px;">{note}</div>'
                f'</div>'
            )

    # ── Top Campaign Banner ───────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    stage     = get_funnel_stage(top_camp)
    stg_color = FUNNEL_COLORS.get(stage, NEON_BLUE)

    _html(
        f'<div style="background:rgba(0,212,255,0.05);border:1px solid rgba(0,212,255,0.2);border-left:4px solid {stg_color};border-radius:8px;padding:12px 20px;display:flex;align-items:center;justify-content:space-between;">'
        f'<div>'
        f'<div style="font-size:10px;color:{TEXT_SECONDARY};text-transform:uppercase;letter-spacing:1px;">Top Performing Campaign</div>'
        f'<div style="font-size:15px;font-weight:600;color:{TEXT_PRIMARY};margin-top:4px;">{top_camp}</div>'
        f'</div>'
        f'<div style="text-align:right;">'
        f'<div style="font-size:24px;font-weight:700;color:{stg_color};">{top_camp_ctr:.2f}%</div>'
        f'<div style="font-size:10px;color:{TEXT_SECONDARY};">CTR</div>'
        f'</div>'
        f'</div>'
    )


# ══════════════════════════════════════════════════════════════════
# SECTION 2: CAMPAIGN TERMINAL (Funnel Breakdown)
# ══════════════════════════════════════════════════════════════════

def render_campaign_terminal(df: pd.DataFrame):
    """Campaign Terminal — Funnel breakdown per campaign."""
    section_header("Campaign Terminal — Funnel Breakdown")

    if df.empty:
        st.info("Tidak ada data tersedia.")
        return

    # ── Aggregate per campaign ────────────────────────────────────
    camp = df.groupby(['funnel_stage','campaign_name','portfolio']).agg(
        spend=('spend','sum'), impressions=('impressions','sum'), clicks=('clicks','sum'),
    ).reset_index()
    camp['ctr'] = camp['clicks'] / camp['impressions'].clip(lower=1) * 100
    camp['cpm'] = camp['spend']  / camp['impressions'].clip(lower=1) * 1000
    camp['cpc'] = camp['spend']  / camp['clicks'].clip(lower=1)

    stage_order = ['UNCATEGORIZED','TOF','MOF','BOF','RET']
    stage_icons = {'UNCATEGORIZED':'&#128308;','TOF':'&#128309;','MOF':'&#128995;','BOF':'&#128992;','RET':'&#128994;'}

    TH = 'padding:10px 12px;text-align:left;font-size:10px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.8px;white-space:nowrap;'

    stage_totals = df.groupby('funnel_stage').agg(
        spend=('spend','sum'), impressions=('impressions','sum'), clicks=('clicks','sum'),
    ).reset_index()
    stage_totals['ctr'] = stage_totals['clicks'] / stage_totals['impressions'].clip(lower=1) * 100
    stage_totals['cpm'] = stage_totals['spend']  / stage_totals['impressions'].clip(lower=1) * 1000
    stage_totals['cpc'] = stage_totals['spend']  / stage_totals['clicks'].clip(lower=1)

    # Build all rows as a single string — no multiline f-strings
    rows = ''
    for stage in stage_order:
        stage_camps = camp[camp['funnel_stage'] == stage]
        if stage_camps.empty:
            continue
        color = FUNNEL_COLORS.get(stage, '#888')
        icon  = stage_icons.get(stage, '&#9898;')
        label = FUNNEL_LABELS.get(stage, stage)
        r     = int(color[1:3],16)
        g     = int(color[3:5],16)
        b     = int(color[5:7],16)

        # Stage header row
        st_row = stage_totals[stage_totals['funnel_stage'] == stage]
        if not st_row.empty:
            s = st_row.iloc[0]
            rows += (
                f'<tr style="background:rgba({r},{g},{b},0.08);border-bottom:1px solid {color}33;">'
                f'<td style="padding:10px 12px;font-size:14px;">{icon}</td>'
                f'<td colspan="2" style="padding:10px 12px;font-size:12px;font-weight:700;color:{color};text-transform:uppercase;letter-spacing:1px;">{label}</td>'
                f'<td style="padding:10px 12px;text-align:right;font-size:13px;font-weight:700;color:{color};">{fmt_idr(s["spend"])}</td>'
                f'<td style="padding:10px 12px;text-align:right;font-size:13px;font-weight:700;color:{color};">{fmt_num(s["impressions"])}</td>'
                f'<td style="padding:10px 12px;text-align:right;font-size:13px;font-weight:700;color:{color};">{fmt_idr(s["cpm"])}</td>'
                f'<td style="padding:10px 12px;text-align:right;font-size:13px;font-weight:700;color:{color};">{fmt_num(s["clicks"])}</td>'
                f'<td style="padding:10px 12px;text-align:right;font-size:13px;font-weight:700;color:{color};">{s["ctr"]:.2f}%</td>'
                f'<td style="padding:10px 12px;text-align:right;font-size:13px;font-weight:700;color:{color};">{fmt_idr(s["cpc"])}</td>'
                f'</tr>'
            )

        # Campaign rows
        for _, row in stage_camps.iterrows():
            cc = NEON_GREEN if row['ctr'] >= 2.0 else (NEON_YELLOW if row['ctr'] >= 1.0 else NEON_RED)

            # Status badge
            camp_status = row.get('status', 'UNKNOWN') if 'status' in row.index else 'UNKNOWN'
            if camp_status == 'ACTIVE':
                status_color = '#00FF88'
                status_bg    = 'rgba(0,255,136,0.1)'
                status_dot   = '&#9679;'
            elif camp_status == 'PAUSED':
                status_color = '#FFD700'
                status_bg    = 'rgba(255,215,0,0.1)'
                status_dot   = '&#9646;'
            elif camp_status in ('ARCHIVED', 'DELETED'):
                status_color = '#FF3B5C'
                status_bg    = 'rgba(255,59,92,0.1)'
                status_dot   = '&#9632;'
            else:
                status_color = '#8892A0'
                status_bg    = 'rgba(136,146,160,0.1)'
                status_dot   = '&#9711;'

            status_badge = (
                f'<span style="display:inline-block;padding:2px 7px;border-radius:20px;'
                f'font-size:10px;font-weight:700;color:{status_color};background:{status_bg};">'
                f'{status_dot} {camp_status}</span>'
            )

            rows += (
                f'<tr style="border-bottom:1px solid #1a2035;">'
                f'<td style="padding:8px 12px;"></td>'
                f'<td style="padding:8px 12px;font-size:13px;color:#f1f5f9;font-weight:500;">'
                f'{row["campaign_name"]}'
                f'<td style="padding:8px 12px;font-size:11px;color:#6b7280;">{row["portfolio"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;font-size:13px;color:#f1f5f9;">{fmt_idr(row["spend"])}</td>'
                f'<td style="padding:8px 12px;text-align:right;font-size:13px;color:#f1f5f9;">{fmt_num(row["impressions"])}</td>'
                f'<td style="padding:8px 12px;text-align:right;font-size:13px;color:#8892A0;">{fmt_idr(row["cpm"])}</td>'
                f'<td style="padding:8px 12px;text-align:right;font-size:13px;color:#f1f5f9;">{fmt_num(row["clicks"])}</td>'
                f'<td style="padding:8px 12px;text-align:right;"><span style="display:inline-block;padding:2px 8px;border-radius:20px;font-size:12px;font-weight:700;color:{cc};background:rgba(255,255,255,0.05);">{row["ctr"]:.2f}%</span></td>'
                f'<td style="padding:8px 12px;text-align:right;font-size:13px;color:#8892A0;">{fmt_idr(row["cpc"])}</td>'
                f'</tr>'
            )

    # Grand total
    gs = df["spend"].sum(); gi = df["impressions"].sum(); gc = df["clicks"].sum()
    gctr = gc/max(gi,1)*100; gcpm = gs/max(gi,1)*1000; gcpc = gs/max(gc,1)
    rows += (
        f'<tr style="background:#111827;border-top:2px solid #2D3348;">'
        f'<td style="padding:12px;"></td>'
        f'<td colspan="2" style="padding:12px;font-size:12px;font-weight:700;color:{TEXT_SECONDARY};text-transform:uppercase;letter-spacing:1px;">Grand Total</td>'
        f'<td style="padding:12px;text-align:right;font-size:14px;font-weight:700;color:{TEXT_PRIMARY};">{fmt_idr(gs)}</td>'
        f'<td style="padding:12px;text-align:right;font-size:14px;font-weight:700;color:{TEXT_PRIMARY};">{fmt_num(gi)}</td>'
        f'<td style="padding:12px;text-align:right;font-size:14px;font-weight:700;color:{TEXT_PRIMARY};">{fmt_idr(gcpm)}</td>'
        f'<td style="padding:12px;text-align:right;font-size:14px;font-weight:700;color:{TEXT_PRIMARY};">{fmt_num(gc)}</td>'
        f'<td style="padding:12px;text-align:right;font-size:14px;font-weight:700;color:{TEXT_PRIMARY};">{gctr:.2f}%</td>'
        f'<td style="padding:12px;text-align:right;font-size:14px;font-weight:700;color:{TEXT_PRIMARY};">{fmt_idr(gcpc)}</td>'
        f'</tr>'
    )

    header = (
        f'<tr style="background:#111827;border-bottom:2px solid #1f2937;">'
        f'<th style="{TH}width:20px;"></th>'
        f'<th style="{TH}min-width:220px;">Campaign</th>'
        f'<th style="{TH}">Portfolio</th>'
        f'<th style="{TH}text-align:right;">Spend</th>'
        f'<th style="{TH}text-align:right;">Impressions</th>'
        f'<th style="{TH}text-align:right;">CPM</th>'
        f'<th style="{TH}text-align:right;">Clicks</th>'
        f'<th style="{TH}text-align:right;">CTR</th>'
        f'<th style="{TH}text-align:right;">CPC</th>'
        f'</tr>'
    )

    # Strip semua newline dari rows sebelum render
    import re
    rows_clean  = re.sub(r'\s+', ' ', rows).strip()
    header_clean = re.sub(r'\s+', ' ', header).strip()

    st.markdown(
        f'<div style="border-radius:10px;border:1px solid #1f2937;background:#0d1117;overflow:hidden;">'
        f'<div style="overflow-x:auto;">'
        f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
        f'<thead>{header_clean}</thead>'
        f'<tbody>{rows_clean}</tbody>'
        f'</table></div></div>',
        unsafe_allow_html=True
    )


# ══════════════════════════════════════════════════════════════════
# SECTION 3: CONTEXT GRAPH
# ══════════════════════════════════════════════════════════════════

def render_context_graph(df: pd.DataFrame):
    """
    Context Graph — Daily Spend (bar) + CTR (line) dual-axis chart.
    Per campaign dengan toggle.
    """
    section_header("Context Graph — Daily Spend & CTR Trend")

    if df.empty:
        st.info("Tidak ada data tersedia.")
        return

    # ── Controls ──────────────────────────────────────────────────
    col_c1, col_c2 = st.columns([2, 2])
    with col_c1:
        view_by = st.selectbox(
            "View by",
            ["All Campaigns", "By Funnel Stage", "By Campaign"],
            key="rev_context_view"
        )
    with col_c2:
        metric_line = st.selectbox(
            "Line metric",
            ["CTR (%)", "CPC (Rp)", "CPM (Rp)"],
            key="rev_context_metric"
        )

    # ── Aggregate ─────────────────────────────────────────────────
    if view_by == "All Campaigns":
        daily = df.groupby('date').agg(
            spend=('spend','sum'),
            impressions=('impressions','sum'),
            clicks=('clicks','sum'),
        ).reset_index()
        daily['ctr'] = daily['clicks'] / daily['impressions'].clip(lower=1) * 100
        daily['cpc'] = daily['spend']  / daily['clicks'].clip(lower=1)
        daily['cpm'] = daily['spend']  / daily['impressions'].clip(lower=1) * 1000
        groups = [('All Campaigns', daily, NEON_BLUE)]

    elif view_by == "By Funnel Stage":
        groups = []
        for stage in ['TOF','MOF','BOF','RET']:
            sub = df[df['funnel_stage'] == stage]
            if sub.empty: continue
            daily = sub.groupby('date').agg(
                spend=('spend','sum'),
                impressions=('impressions','sum'),
                clicks=('clicks','sum'),
            ).reset_index()
            daily['ctr'] = daily['clicks'] / daily['impressions'].clip(lower=1) * 100
            daily['cpc'] = daily['spend']  / daily['clicks'].clip(lower=1)
            daily['cpm'] = daily['spend']  / daily['impressions'].clip(lower=1) * 1000
            groups.append((FUNNEL_LABELS[stage], daily, FUNNEL_COLORS[stage]))

    else:  # By Campaign
        groups = []
        colors_list = [NEON_BLUE, NEON_PURPLE, NEON_ORANGE, NEON_GREEN, NEON_YELLOW]
        for i, camp in enumerate(df['campaign_name'].unique()):
            sub = df[df['campaign_name'] == camp]
            daily = sub.groupby('date').agg(
                spend=('spend','sum'),
                impressions=('impressions','sum'),
                clicks=('clicks','sum'),
            ).reset_index()
            daily['ctr'] = daily['clicks'] / daily['impressions'].clip(lower=1) * 100
            daily['cpc'] = daily['spend']  / daily['clicks'].clip(lower=1)
            daily['cpm'] = daily['spend']  / daily['impressions'].clip(lower=1) * 1000
            groups.append((camp, daily, colors_list[i % len(colors_list)]))

    # ── Build chart ───────────────────────────────────────────────
    fig = go.Figure()

    line_col_map = {
        "CTR (%)": ('ctr', '%', 1),
        "CPC (Rp)": ('cpc', 'Rp', 0),
        "CPM (Rp)": ('cpm', 'Rp', 0),
    }
    line_col, line_suffix, line_dec = line_col_map[metric_line]

    for label, daily, color in groups:
        if daily.empty: continue

        # Bar: spend
        fig.add_trace(go.Bar(
            name        = f"{label} — Spend",
            x           = daily['date'],
            y           = daily['spend'],
            marker_color= color,
            opacity     = 0.7,
            yaxis       = 'y',
            hovertemplate = (
                f"<b>{label}</b><br>"
                "Date: %{x}<br>"
                "Spend: Rp %{y:,.0f}<extra></extra>"
            ),
        ))

        # Line: selected metric
        fig.add_trace(go.Scatter(
            name      = f"{label} — {metric_line}",
            x         = daily['date'],
            y         = daily[line_col],
            mode      = 'lines+markers',
            line      = dict(color=color, width=2, dash='dot'),
            marker    = dict(size=6),
            yaxis     = 'y2',
            hovertemplate = (
                f"<b>{label}</b><br>"
                f"Date: %{{x}}<br>"
                f"{metric_line}: %{{y:.{line_dec}f}}{line_suffix}<extra></extra>"
            ),
        ))

    fig.update_layout(
        plot_bgcolor  = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)",
        font          = dict(color=TEXT_SECONDARY, size=12),
        height        = 380,
        margin        = dict(l=10, r=10, t=20, b=10),
        legend        = dict(
            orientation = "h",
            yanchor     = "bottom",
            y           = 1.02,
            xanchor     = "right",
            x           = 1,
            font        = dict(size=11),
        ),
        barmode       = 'stack',
        xaxis         = dict(
            gridcolor   = '#1E2335',
            showgrid    = True,
            tickformat  = "%d %b",
        ),
        yaxis         = dict(
            title       = "Spend (Rp)",
            gridcolor   = '#1E2335',
            showgrid    = True,
            tickprefix  = "Rp ",
            tickformat  = ",.0f",
        ),
        yaxis2        = dict(
            title       = metric_line,
            overlaying  = "y",
            side        = "right",
            showgrid    = False,
            ticksuffix  = line_suffix if line_suffix == '%' else '',
            tickprefix  = 'Rp ' if 'Rp' in metric_line else '',
        ),
    )

    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════
# AI INSIGHTS — Revenue Engineering
# ══════════════════════════════════════════════════════════════════

def generate_revenue_insights(df: pd.DataFrame) -> dict:
    """Generate AI insights untuk semua section Revenue Engineering."""
    if df.empty:
        return {}

    cache_key = (
        f"revenue_insights_"
        f"{len(df)}_"
        f"{str(df['date'].min())[:10]}_"
        f"{str(df['date'].max())[:10]}_"
        f"{sorted(df['portfolio'].unique().tolist())}"
    )

    if st.session_state.get('rev_insight_cache_key') == cache_key:
        return st.session_state.get('revenue_insights', {})

    try:
        from services.ai_service import RevenueInsightGenerator
        with st.spinner('Generating AI insights...'):
            generator = RevenueInsightGenerator(df)
            insights  = generator.generate_all()
        st.session_state['revenue_insights']     = insights
        st.session_state['rev_insight_cache_key'] = cache_key
        return insights
    except Exception as e:
        st.warning(f"AI insights unavailable: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════

def show_revenue_engineering():
    """Main entry point untuk Revenue Engineering module."""

    # ── Header ────────────────────────────────────────────────────
    _html(
        '<div style="background:linear-gradient(135deg,#1B1F2B 0%,#13161F 100%);border:1px solid #2D3348;border-radius:12px;padding:20px 28px;margin-bottom:28px;position:relative;overflow:hidden;">'
        '<div style="position:absolute;top:0;right:0;width:220px;height:100%;background:radial-gradient(ellipse at 80% 50%,rgba(0,212,255,0.07) 0%,transparent 70%);pointer-events:none;"></div>'
        '<div style="display:flex;align-items:center;gap:16px;margin-bottom:14px;">'
        '<div style="width:4px;height:52px;border-radius:4px;background:linear-gradient(180deg,#00D4FF 0%,transparent 100%);box-shadow:0 0 14px rgba(0,212,255,0.6);flex-shrink:0;"></div>'
        '<div>'
        '<div style="font-size:10px;font-weight:700;letter-spacing:2.5px;color:#00D4FF;text-transform:uppercase;font-family:monospace;margin-bottom:5px;opacity:0.9;">MODULE 01 &nbsp;·&nbsp; FINANCIAL TERMINAL</div>'
        '<div style="font-size:26px;font-weight:800;color:#FFFFFF;line-height:1.2;letter-spacing:-0.3px;">💹 Paid Ads Campaign Performance</div>'
        '<div style="font-size:13px;color:#8892A0;margin-top:5px;line-height:1.5;">Ad Performance &amp; Spend Efficiency across all Portfolios</div>'
        '</div>'
        '</div>'
        '<div style="height:1px;background:linear-gradient(to right,rgba(0,212,255,0.35),transparent);"></div>'
        '</div>'
    )

    # ── Load data dulu sebelum filter ────────────────────────────
    loader = DataLoader(portfolio='all')
    df_raw = loader.load_revenue_data()

    if df_raw.empty:
        st.warning("Data ads belum tersedia.")
        return

    df = prepare_ads_data(df_raw)

    # ── Filters row 1: Date + Portfolio + Stage ──────────────────
    col_f1, col_f2, col_f3, col_f4 = st.columns([1.5, 1.5, 2, 2])

    # Ambil min/max date dari data aktual
    min_date = df['date'].min().date() if not df.empty else (datetime.now() - timedelta(days=30)).date()
    max_date = df['date'].max().date() if not df.empty else datetime.now().date()

    with col_f1:
        since_date = st.date_input(
            "From",
            value     = min_date,
            min_value = min_date,
            max_value = max_date,
            key       = "rev_since_date",
        )
    with col_f2:
        until_date = st.date_input(
            "To",
            value     = max_date,
            min_value = min_date,
            max_value = max_date,
            key       = "rev_until_date",
        )

    portfolios = ['All Portfolios'] + sorted(df['portfolio'].unique().tolist())
    with col_f3:
        selected_port = st.selectbox(
            "Portfolio",
            portfolios,
            key="rev_portfolio"
        )
    with col_f4:
        selected_stage = st.selectbox(
            "Funnel Stage",
            ["All Stages", "TOF", "MOF", "BOF", "RET"],
            key="rev_funnel_stage"
        )

    # ── Filters row 2: Campaign Status ───────────────────────────
    # Ambil status yang tersedia dari data
    has_status   = 'status' in df.columns and df['status'].nunique() > 0
    status_options = ['All Status']
    if has_status:
        # Normalize status dari Meta API: ACTIVE, PAUSED, ARCHIVED, DELETED
        available_statuses = sorted(df['status'].dropna().unique().tolist())
        status_options += available_statuses

    col_s1, col_s2 = st.columns([2, 4])
    with col_s1:
        selected_status = st.selectbox(
            "Campaign Status",
            status_options,
            key="rev_campaign_status"
        )

    # ── Tampilkan date range yang dipilih ─────────────────────────
    since_dt = pd.Timestamp(since_date)
    until_dt = pd.Timestamp(until_date)
    n_days   = (until_date - since_date).days + 1

    # # Hitung total campaign per status untuk info
    # if has_status:
    #     status_counts = df['status'].value_counts().to_dict()
    #     status_info   = " · ".join([
    #         f'<span style="color:{"#00FF88" if s == "ACTIVE" else "#FF3B5C" if s in ["PAUSED","ARCHIVED"] else "#8892A0"};">'
    #         f'{s}: {c}</span>'
    #         for s, c in status_counts.items()
    #     ])
    #     date_banner_extra = f' &nbsp;|&nbsp; {status_info}'
    # else:
    #     date_banner_extra = ''

    st.markdown(
        f'<div style="margin:8px 0 16px 0;padding:8px 14px;background:rgba(0,212,255,0.05);'
        f'border:1px solid rgba(0,212,255,0.15);border-radius:8px;font-size:12px;color:#8892A0;">'
        f'Showing data from <span style="color:#00D4FF;font-weight:600;">'
        f'{since_date.strftime("%d %b %Y")}</span> to '
        f'<span style="color:#00D4FF;font-weight:600;">'
        f'{until_date.strftime("%d %b %Y")}</span> '
        f'<span style="color:#5A6577;">({n_days} days)</span>'
        f'</div>',
        unsafe_allow_html=True
    )

    # ── Apply filters ─────────────────────────────────────────────
    df = df[(df['date'] >= since_dt) & (df['date'] <= until_dt)]

    if selected_port != 'All Portfolios':
        df = df[df['portfolio'] == selected_port]

    if selected_stage != 'All Stages':
        df = df[df['funnel_stage'] == selected_stage]

    if selected_status != 'All Status' and has_status:
        df = df[df['status'] == selected_status]

    if df.empty:
        st.info("Tidak ada data untuk filter yang dipilih.")
        return

    # ── Generate AI Insights ──────────────────────────────────────
    insights = generate_revenue_insights(df)

    # ── Section 1: North Star Ribbon ─────────────────────────────
    divider()
    render_north_star(df, selected_port)
    render_inline_insight(insights.get('north_star', {}))

    # ── Section 2: Campaign Terminal ──────────────────────────────
    divider()
    render_campaign_terminal(df)
    render_inline_insight(insights.get('campaign_terminal', {}))

    # ── Section 3: Context Graph ──────────────────────────────────
    divider()
    render_context_graph(df)
    render_inline_insight(insights.get('context_graph', {}))

    divider()
