"""
modules/organic_architecture.py
================================
Module 2: Organic Architecture — Brand Terminal
Updated untuk support:
- Real data dari DataLoader (meta_api.py + transformer.py)
- Multi-portfolio filter
- Date range filter
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

from utils.data_loader import DataLoader
from components.inline_insight import render_inline_insight, generate_organic_insights

# ══════════════════════════════════════════════════════════════════
# 1. THEME CONSTANTS
# ══════════════════════════════════════════════════════════════════

DARK_BG       = "#0E1117"
CARD_BG       = "#1B1F2B"
CARD_BORDER   = "#2D3348"
NEON_BLUE     = "#00D4FF"
NEON_GREEN    = "#00FF88"
NEON_PURPLE   = "#A855F7"
NEON_ORANGE   = "#FF6B35"
NEON_RED      = "#FF3B5C"
NEON_YELLOW   = "#FFD700"
TEXT_PRIMARY  = "#FFFFFF"
TEXT_SECONDARY= "#8892A0"
TEXT_MUTED    = "#5A6577"

PLATFORM_COLORS = {
    "Instagram": "#E1306C",
    "Facebook" : "#1877F2",
    "TikTok"   : "#00F2EA",
    "YouTube"  : "#FF0000",
    "LinkedIn" : "#0A66C2",
}

PLATFORM_ICONS = {
    "Instagram": "📸",
    "Facebook" : "📘",
    "TikTok"   : "🎵",
    "YouTube"  : "🎬",
    "LinkedIn" : "💼",
}

# ══════════════════════════════════════════════════════════════════
# 2. HELPERS
# ══════════════════════════════════════════════════════════════════

def load_module2_css():
    css_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'assets', 'module2_organic.css'
    )
    try:
        with open(css_path) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        try:
            with open('assets/module2_organic.css') as f:
                st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
        except FileNotFoundError:
            pass

def format_number(num):
    """Format large numbers: 1500 → 1.5K"""
    try:
        num = float(num)
    except (ValueError, TypeError):
        return str(num)
    if num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.1f}K"
    return str(int(num))

def _get_available_platforms(df: pd.DataFrame) -> list:
    """Ambil list platform yang ada di data."""
    if df.empty or 'platform' not in df.columns:
        return list(PLATFORM_COLORS.keys())
    return sorted(df['platform'].unique().tolist())

# ══════════════════════════════════════════════════════════════════
# 3. SECTION RENDERERS
# ══════════════════════════════════════════════════════════════════

def render_cross_channel_pulse(df: pd.DataFrame):
    st.markdown(
        '<div class="section-header">📈 Cross-Channel Pulse — Ticker Tape</div>',
        unsafe_allow_html=True
    )

    platforms = _get_available_platforms(df)
    if not platforms:
        st.info("Tidak ada data platform tersedia.")
        return

    cols = st.columns(len(platforms))

    for idx, platform in enumerate(platforms):
        platform_data = df[df['platform'] == platform].copy()
        if platform_data.empty:
            continue

        platform_data = platform_data.sort_values('date').reset_index(drop=True)

        # ── Cek apakah multi-portfolio ────────────────────────────
        portfolios    = platform_data['portfolio'].unique().tolist()
        is_multi_port = len(portfolios) > 1

        if is_multi_port:
            # ── Multi-portfolio: aggregate per tanggal dulu ───────
            # Sum followers tidak bisa langsung — ambil per portfolio
            # lalu sum nilai terakhir masing-masing
            current_followers = 0
            start_followers   = 0
            total_growth      = 0
            all_daily_growth  = []

            for portfolio in portfolios:
                pdf = platform_data[platform_data['portfolio'] == portfolio].sort_values('date')
                if pdf.empty:
                    continue
                current_followers += int(pdf['followers'].iloc[-1])
                start_followers   += int(pdf['followers'].iloc[0])
                total_growth      += int(pdf['follower_growth'].sum())
                all_daily_growth.extend(pdf['follower_growth'].tolist())

            avg_daily_growth = sum(all_daily_growth) / max(len(all_daily_growth), 1)

        else:
            # ── Single portfolio: langsung ambil ──────────────────
            current_followers = int(platform_data['followers'].iloc[-1])
            start_followers   = int(platform_data['followers'].iloc[0])
            total_growth      = int(platform_data['follower_growth'].sum())
            avg_daily_growth  = platform_data['follower_growth'].mean()
            avg_daily_growth  = avg_daily_growth if not pd.isna(avg_daily_growth) else 0

        # ── Growth % ──────────────────────────────────────────────
        growth_pct = (total_growth / max(start_followers, 1)) * 100

        # ── Format display ────────────────────────────────────────
        is_positive  = total_growth >= 0
        growth_arrow = "▲" if is_positive else "▼"
        growth_color = "#00FF88" if is_positive else "#FF3B5C"
        color        = PLATFORM_COLORS.get(platform, "#FFFFFF")
        icon         = PLATFORM_ICONS.get(platform, "📄")

        # ── Period info ───────────────────────────────────────────
        date_min   = platform_data['date'].min()
        date_max   = platform_data['date'].max()
        period_str = (
            f"{date_min.strftime('%d %b')} – {date_max.strftime('%d %b %Y')}"
            if hasattr(date_min, 'strftime')
            else f"{str(date_min)[:10]} – {str(date_max)[:10]}"
        )

        # ── Label portfolio ───────────────────────────────────────
        port_label = (
            f'<div style="font-size:9px;color:#5A6577;margin-top:2px;">'
            f'{len(portfolios)} portfolios combined</div>'
            if is_multi_port else
            f'<div style="font-size:9px;color:#5A6577;margin-top:2px;">'
            f'{portfolios[0]}</div>'
        )

        with cols[idx]:
            st.markdown(f"""
            <div class="ticker-card" style="border-top: 3px solid {color};">
                <div class="ticker-platform">{icon} {platform}</div>
                {port_label}
                <div class="metric-value">{current_followers:,}</div>
                <div style="font-size:13px;font-weight:600;color:{growth_color};margin-top:4px;">
                    {growth_arrow} {abs(total_growth):,}
                    <span style="font-size:11px;font-weight:400;">
                        ({growth_pct:+.1f}%)
                    </span>
                </div>
                <div style="font-size:11px;color:#5A6577;margin-top:4px;">
                    Avg {avg_daily_growth:+.0f}/day
                </div>
                <div style="font-size:10px;color:#3A4055;margin-top:2px;">
                    {period_str}
                </div>
            </div>
            """, unsafe_allow_html=True)


def content_library_section(df_content: pd.DataFrame, platform_filter: list):
    """
    View Mode 2: Content Library
    Thumbnail Grid of posts, sortable by Virality Score or Conversion Score.
    """
    st.markdown(
        '<div class="section-header">📚 CONTENT LIBRARY — POST PERFORMANCE</div>',
        unsafe_allow_html=True
    )

    content_df = df_content.copy()

    # Apply platform filter
    if platform_filter:
        content_df = content_df[content_df['platform'].isin(platform_filter)]

    if content_df.empty:
        st.info("Tidak ada post tersedia untuk filter yang dipilih.")
        return

    # ── Controls ──────────────────────────────────────────────────
    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 2, 1])

    with ctrl_col1:
        view_mode = st.selectbox(
            "👁️ View",
            ["Grid View", "Table View"],
            key="content_view_mode"
        )
    with ctrl_col2:
        sort_by = st.selectbox(
            "↕️ Sort By",
            ["Virality Score ↓", "Conversion Score ↓", "Views ↓", "Likes ↓", "Most Recent"],
            key="content_sort_by"
        )
    with ctrl_col3:
        type_options = ["All Types"] + sorted(content_df['content_type'].dropna().unique().tolist())
        content_type_filter = st.selectbox(
            "📄 Content Type",
            type_options,
            key="content_type_filter"
        )

    if content_type_filter != "All Types":
        content_df = content_df[content_df['content_type'] == content_type_filter]

    # ── Sort ──────────────────────────────────────────────────────
    sort_map = {
        "Virality Score ↓"  : ("virality_score", False),
        "Conversion Score ↓": ("conversion_score", False),
        "Views ↓"           : ("views", False),
        "Likes ↓"           : ("likes", False),
        "Most Recent"       : ("date", False),
    }
    sort_col, sort_asc = sort_map.get(sort_by, ("virality_score", False))
    content_df = content_df.sort_values(
        by=sort_col, ascending=sort_asc
    ).reset_index(drop=True)

    # ── Pagination ────────────────────────────────────────────────
    ITEMS_PER_PAGE = 9
    total_items    = len(content_df)
    total_pages    = max(1, (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)

    if "content_page" not in st.session_state:
        st.session_state.content_page = 1
    if st.session_state.content_page > total_pages:
        st.session_state.content_page = total_pages

    current_page = st.session_state.content_page
    start_idx    = (current_page - 1) * ITEMS_PER_PAGE
    end_idx      = min(start_idx + ITEMS_PER_PAGE, total_items)
    page_df      = content_df.iloc[start_idx:end_idx]

    st.markdown(f"""
    <div style="display:flex;justify-content:space-between;padding:8px 0;margin-bottom:12px;">
        <span style="font-size:12px;color:#8892A0;">
            Showing {start_idx+1}–{end_idx} of {total_items} posts
        </span>
        <span style="font-size:12px;color:#8892A0;">
            Page {current_page} of {total_pages}
        </span>
    </div>
    """, unsafe_allow_html=True)

    if view_mode == "Grid View":
        render_content_grid(page_df)
    else:
        render_content_table(page_df, sort_by)

    render_pagination(current_page, total_pages)


def render_content_grid(page_df: pd.DataFrame):
    """Render content sebagai visual grid cards."""
    cols_per_row = 3
    rows_needed  = (len(page_df) + cols_per_row - 1) // cols_per_row

    for row_idx in range(rows_needed):
        cols = st.columns(cols_per_row)
        for col_idx in range(cols_per_row):
            item_idx = row_idx * cols_per_row + col_idx
            if item_idx >= len(page_df):
                break

            post     = page_df.iloc[item_idx]
            platform = post.get('platform', 'Unknown')
            color    = PLATFORM_COLORS.get(platform, "#FFFFFF")
            icon     = PLATFORM_ICONS.get(platform, "📄")

            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            gradient = f"linear-gradient(135deg, rgba({r},{g},{b},0.3) 0%, rgba({r},{g},{b},0.08) 100%)"

            vs       = float(post.get('virality_score', 0))
            cs       = float(post.get('conversion_score', 0))
            vs_color = NEON_GREEN if vs >= 3.0 else (NEON_YELLOW if vs >= 1.5 else NEON_RED)
            cs_color = NEON_GREEN if cs >= 3.0 else (NEON_YELLOW if cs >= 1.5 else NEON_RED)

            # Format date (before thumbnail so it can be embedded)
            date_val = post.get('date', '')
            if hasattr(date_val, 'strftime'):
                date_str = date_val.strftime('%d %b %Y')
            else:
                date_str = str(date_val)[:10]

            title = str(post.get('title', 'Untitled Post'))[:60]

            # Thumbnail or placeholder using CSS classes
            thumbnail = post.get('thumbnail', '')
            if thumbnail and str(thumbnail) not in ['nan', '']:
                thumb_html = f'''
                <div class="card-thumbnail">
                    <img src="{thumbnail}" style="width:100%;height:100%;object-fit:cover;border-radius:12px 12px 0 0;">
                    <span class="card-thumbnail-platform" style="background:{color};">{icon} {platform}</span>
                    <span class="card-thumbnail-date">{date_str}</span>
                </div>'''
            else:
                thumb_html = f'''
                <div class="card-thumbnail" style="background:{gradient};">
                    <span class="card-thumbnail-icon">{icon}</span>
                    <span class="card-thumbnail-type">{platform}</span>
                    <span class="card-thumbnail-platform" style="background:{color};">{icon} {platform}</span>
                    <span class="card-thumbnail-date">{date_str}</span>
                </div>'''

            with cols[col_idx]:
                st.markdown(f"""
                <div class="content-card">
                    {thumb_html}
                    <div class="card-body">
                        <div class="card-title">{title}</div>
                        <div class="card-metrics">
                            <div class="card-metric-item">
                                <div class="card-metric-value">{format_number(post.get('views',0))}</div>
                                <div class="card-metric-label">Views</div>
                            </div>
                            <div class="card-metric-item">
                                <div class="card-metric-value">{format_number(post.get('likes',0))}</div>
                                <div class="card-metric-label">Likes</div>
                            </div>
                            <div class="card-metric-item">
                                <div class="card-metric-value">{format_number(post.get('shares',0))}</div>
                                <div class="card-metric-label">Shares</div>
                            </div>
                        </div>
                        <div class="card-metrics">
                            <div class="card-metric-item">
                                <div class="card-metric-value">{format_number(post.get('comments',0))}</div>
                                <div class="card-metric-label">Comments</div>
                            </div>
                            <div class="card-metric-item">
                                <div class="card-metric-value">{format_number(post.get('saves',0))}</div>
                                <div class="card-metric-label">Saves</div>
                            </div>
                            <div class="card-metric-item">
                                <div class="card-metric-value">{format_number(post.get('link_clicks',0))}</div>
                                <div class="card-metric-label">Clicks</div>
                            </div>
                        </div>
                        <div class="card-scores">
                            <div class="score-badge score-badge-virality" style="color:{vs_color};">
                                🔥 Virality: {vs:.1f}%
                            </div>
                            <div class="score-badge score-badge-conversion" style="color:{cs_color};">
                                🎯 Conv: {cs:.1f}%
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)


def render_content_table(page_df: pd.DataFrame, current_sort: str):
    """Render content table — batch rows untuk avoid truncation & layout break."""

    sort_active_map = {
        "Virality Score ↓"  : "virality_score",
        "Conversion Score ↓": "conversion_score",
        "Views ↓"           : "views",
        "Likes ↓"           : "likes",
        "Most Recent"       : None,
    }
    active_sort_col = sort_active_map.get(current_sort)

    def metric_cell(val):
        try:
            v = int(float(val)) if val and str(val) not in ['nan','None',''] else 0
        except (TypeError, ValueError):
            v = 0
        if v == 0:
            return '<td style="text-align:right;color:#2d3748;padding:9px 12px;">—</td>'
        return f'<td style="text-align:right;font-size:14px;font-weight:600;color:#f1f5f9;padding:9px 12px;">{format_number(v)}</td>'

    def score_cell(val):
        try:
            v = float(val)
        except (TypeError, ValueError):
            v = 0.0
        if v >= 3.0:
            color, bg = '#00ff88', 'rgba(0,255,136,.12)'
        elif v >= 1.5:
            color, bg = '#ffd700', 'rgba(255,215,0,.12)'
        elif v > 0:
            color, bg = '#ff6b6b', 'rgba(255,107,107,.12)'
        else:
            return '<td style="text-align:right;padding:9px 12px;"><span style="color:#2d3748;font-size:12px;">—</span></td>'
        return (f'<td style="text-align:right;padding:9px 12px;">'
                f'<span style="display:inline-block;padding:3px 9px;border-radius:20px;'
                f'font-size:12px;font-weight:700;color:{color};background:{bg};">'
                f'{v:.1f}%</span></td>')

    def build_row(post) -> str:
        """Build 1 row HTML string."""
        platform  = str(post.get('platform', 'Unknown'))
        color     = PLATFORM_COLORS.get(platform, '#888')
        icon      = PLATFORM_ICONS.get(platform, '📄')
        thumbnail = str(post.get('thumbnail', ''))
        title     = str(post.get('title', 'Untitled Post'))[:80]
        ctype     = str(post.get('content_type', ''))
        permalink = str(post.get('permalink', ''))

        date_val = post.get('date', '')
        date_str = date_val.strftime('%d %b %Y') if hasattr(date_val, 'strftime') \
                   else str(date_val)[:10]

        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)

        is_valid_thumb = (thumbnail and thumbnail not in ['nan','','None']
                          and thumbnail.startswith('http'))
        thumb_html = (
            f'<img src="{thumbnail}" style="width:48px;height:48px;'
            f'border-radius:8px;object-fit:cover;display:block;">'
            if is_valid_thumb else
            f'<div style="width:48px;height:48px;border-radius:8px;'
            f'background:rgba({r},{g},{b},.15);display:flex;align-items:center;'
            f'justify-content:center;font-size:20px;">{icon}</div>'
        )

        badge = (
            f'<span style="display:inline-flex;align-items:center;gap:3px;'
            f'padding:2px 7px;border-radius:20px;font-size:10px;font-weight:600;'
            f'margin-bottom:4px;background:rgba({r},{g},{b},.18);color:{color};">'
            f'{icon} {platform}</span>'
        )

        is_valid_link = (permalink and permalink not in ['nan','','None']
                         and permalink.startswith('http'))
        link_cell = (
            f'<td style="padding:9px 12px;">'
            f'<a href="{permalink}" target="_blank" style="color:#38bdf8;'
            f'text-decoration:none;font-size:11px;opacity:.7;">&#8599; Open</a></td>'
            if is_valid_link else '<td style="padding:9px 12px;"></td>'
        )

        return f"""
        <tr style="border-bottom:1px solid #1a2035;">
            <td style="padding:8px 8px 8px 14px;vertical-align:middle;">{thumb_html}</td>
            <td style="min-width:200px;max-width:300px;padding:9px 12px;vertical-align:middle;">
                {badge}
                <div style="font-size:13px;font-weight:500;color:#f1f5f9;
                            line-height:1.4;word-break:break-word;">{title}</div>
                <div style="font-size:10px;color:#4b5563;margin-top:2px;">
                    {ctype}&nbsp;·&nbsp;{date_str}
                </div>
            </td>
            {metric_cell(post.get('views',0))}
            {metric_cell(post.get('likes',0))}
            {metric_cell(post.get('comments',0))}
            {metric_cell(post.get('shares',0))}
            {metric_cell(post.get('saves',0))}
            {score_cell(post.get('virality_score',0))}
            {score_cell(post.get('conversion_score',0))}
            {link_cell}
        </tr>"""

    # ── Build header ──────────────────────────────────────────────
    TH = ('padding:11px 12px;text-align:left;font-size:10px;font-weight:600;'
          'color:#6b7280;text-transform:uppercase;letter-spacing:.8px;white-space:nowrap;')

    def th(label, key=None, align='left'):
        col = 'color:#38bdf8;' if key and key == active_sort_col else ''
        arr = ' ↓' if key and key == active_sort_col else ''
        return f'<th style="{TH}{col}text-align:{align};">{label}{arr}</th>'

    header = f"""
    <tr style="background:#111827;border-bottom:2px solid #1f2937;">
        <th style="{TH}width:52px;padding-left:14px;"></th>
        {th('Content')}
        {th('Views','views','right')}
        {th('Likes','likes','right')}
        {th('Comments','comments','right')}
        {th('Shares','shares','right')}
        {th('Saves','saves','right')}
        {th('Virality','virality_score','right')}
        {th('Conv.','conversion_score','right')}
        <th style="{TH}"></th>
    </tr>"""

    # ── Build semua rows sekaligus ────────────────────────────────
    all_rows = ''.join(build_row(post) for _, post in page_df.iterrows())

    # ── Render dalam 1 st.markdown() call ────────────────────────
    st.markdown(f"""
    <div style="border-radius:10px;border:1px solid #1f2937;
                background:#0d1117;overflow:hidden;">
        <div style="overflow-x:auto;">
            <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <thead>{header}</thead>
                <tbody>{all_rows}</tbody>
            </table>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_pagination(current_page: int, total_pages: int):
    """Render pagination controls."""
    col_prev, col_info, col_next = st.columns([1, 2, 1])

    with col_prev:
        if current_page > 1:
            if st.button("← Previous", key="page_prev", use_container_width=True):
                st.session_state.content_page = current_page - 1
                st.rerun()
        else:
            st.button("← Previous", key="page_prev_disabled",
                      disabled=True, use_container_width=True)

    with col_info:
        page_cols  = st.columns(min(total_pages, 7))
        if total_pages <= 7:
            page_range = range(1, total_pages + 1)
        elif current_page <= 4:
            page_range = range(1, 8)
        elif current_page >= total_pages - 3:
            page_range = range(total_pages - 6, total_pages + 1)
        else:
            page_range = range(current_page - 3, current_page + 4)

        for idx, page_num in enumerate(page_range):
            if idx < len(page_cols):
                with page_cols[idx]:
                    btn_type = "primary" if page_num == current_page else "secondary"
                    if st.button(str(page_num), key=f"page_{page_num}",
                                 type=btn_type, use_container_width=True):
                        st.session_state.content_page = page_num
                        st.rerun()

    with col_next:
        if current_page < total_pages:
            if st.button("Next →", key="page_next", use_container_width=True):
                st.session_state.content_page = current_page + 1
                st.rerun()
        else:
            st.button("Next →", key="page_next_disabled",
                      disabled=True, use_container_width=True)


def render_metrics_stacks(df: pd.DataFrame, df_content: pd.DataFrame = None):
    """
    Section 2: Metric Stack — Depth & Traffic
    ER, SoV, PCR, Consistency Score per platform.
    Fallback ke content_library untuk platform yang impressions = 0.
    """
    st.markdown(
        '<div class="section-header">📊 METRIC STACK — DEPTH & TRAFFIC</div>',
        unsafe_allow_html=True
    )

    if df.empty:
        st.info("Tidak ada data tersedia.")
        return

    platforms  = _get_available_platforms(df)
    benchmarks = {"er": 5.0, "sov": 2.0, "pcr": 3.0, "cs": 80.0}

    # ── Pre-aggregate content_library per platform ────────────────
    # Dipakai sebagai fallback jika impressions = 0
    content_agg = {}
    if df_content is not None and not df_content.empty:
        for platform in platforms:
            pc = df_content[df_content['platform'] == platform]
            if not pc.empty:
                content_agg[platform] = {
                    'likes'   : pc['likes'].sum(),
                    'comments': pc['comments'].sum(),
                    'shares'  : pc['shares'].sum(),
                    'saves'   : pc['saves'].sum(),
                    'views'   : pc['views'].sum(),
                }

    platform_metrics = []
    for platform in platforms:
        pdf           = df[df['platform'] == platform]
        p_impressions = pdf['impressions'].sum()
        p_link_clicks = pdf['link_clicks'].sum()
        p_profile_vis = pdf['profile_visits'].sum()
        p_posts       = pdf['posts_published'].sum()
        p_days        = max(pdf['date'].nunique(), 1)
        p_goal        = pdf['posts_goal_weekly'].iloc[0] / 7 * p_days if len(pdf) > 0 else 0

        # ── Pilih sumber data engagement ─────────────────────────
        using_fallback = False
        if p_impressions > 0:
            # Page insights aktif — pakai organic_data
            p_likes    = pdf['likes'].sum()
            p_comments = pdf['comments'].sum()
            p_shares   = pdf['shares'].sum()
            p_saves    = pdf['saves'].sum()
            denominator = p_impressions
            data_source = 'Page Insights'
        else:
            # Fallback ke content_library
            ca = content_agg.get(platform, {})
            p_likes    = ca.get('likes', 0)
            p_comments = ca.get('comments', 0)
            p_shares   = ca.get('shares', 0)
            p_saves    = ca.get('saves', 0)
            # Untuk denominator ER, pakai total views dari content
            # (sum impressions/views per post sebagai proxy)
            denominator   = ca.get('views', 0)
            using_fallback = True
            data_source    = 'Content Library'

        # ── Hitung metrics ────────────────────────────────────────
        er  = (p_likes + p_comments + p_shares) / max(denominator, 1) * 100
        sov = (p_saves + p_shares) / max(denominator, 1) * 100
        pcr = p_link_clicks / max(p_profile_vis, 1) * 100
        cs  = p_posts / max(p_goal, 1) * 100

        platform_metrics.append({
            "platform"      : platform,
            "er"            : er,
            "sov"           : sov,
            "pcr"           : pcr,
            "cs"            : cs,
            "using_fallback": using_fallback,
            "data_source"   : data_source,
            "denominator"   : denominator,
        })

    cols = st.columns(len(platform_metrics))

    def get_status(value, benchmark):
        if value >= benchmark:
            return NEON_GREEN, "▲ On Track"
        elif value >= benchmark * 0.7:
            return NEON_YELLOW, "● Near Target"
        return NEON_RED, "▼ Below Target"

    for idx, pm in enumerate(platform_metrics):
        platform = pm["platform"]
        color    = PLATFORM_COLORS.get(platform, "#FFFFFF")
        icon     = PLATFORM_ICONS.get(platform, "📄")

        er_color,  er_status  = get_status(pm["er"],  benchmarks["er"])
        sov_color, sov_status = get_status(pm["sov"], benchmarks["sov"])
        pcr_color, pcr_status = get_status(pm["pcr"], benchmarks["pcr"])
        cs_color,  cs_status  = get_status(pm["cs"],  benchmarks["cs"])

        # Badge sumber data
        source_badge = (
            f'<span style="font-size:9px;padding:2px 6px;border-radius:10px;'
            f'background:rgba(255,215,0,.15);color:#FFD700;font-weight:600;">'
            f'via Content Library</span>'
            if pm["using_fallback"] else
            f'<span style="font-size:9px;padding:2px 6px;border-radius:10px;'
            f'background:rgba(0,255,136,.15);color:#00FF88;font-weight:600;">'
            f'via Page Insights</span>'
        )

        with cols[idx]:
            st.markdown(f"""
            <div class="metric-stack-card">
                <div class="metric-stack-card-header" style="border-bottom:2px solid {color};">
                    <span class="metric-stack-card-icon" style="background:{color};">{icon}</span>
                    <span class="metric-stack-card-title">{platform}</span>
                    <span style="margin-left:auto;">{source_badge}</span>
                </div>
                <div class="metric-stack-card-body">
                    <div class="metric-stack-item">
                        <div class="metric-stack-label">Engagement Rate (ER)</div>
                        <div class="metric-value">{pm["er"]:.2f}%</div>
                        <div class="metric-stack-status" style="color:{er_color};">{er_status}</div>
                    </div>
                    <div class="metric-stack-item">
                        <div class="metric-stack-label">Share of Voice (SoV)</div>
                        <div class="metric-value">{pm["sov"]:.2f}%</div>
                        <div class="metric-stack-status" style="color:{sov_color};">{sov_status}</div>
                    </div>
                    <div class="metric-stack-item">
                        <div class="metric-stack-label">Profile Conversion Rate</div>
                        <div class="metric-value">{pm["pcr"]:.2f}%</div>
                        <div class="metric-stack-status" style="color:{pcr_color};">{pcr_status}</div>
                    </div>
                    <div class="metric-stack-item">
                        <div class="metric-stack-label">Consistency Score</div>
                        <div class="metric-value">{pm["cs"]:.2f}%</div>
                        <div class="metric-stack-status" style="color:{cs_color};">{cs_status}</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)


def render_engagement_funnel(df: pd.DataFrame):
    """Visualization A: Engagement Funnel — Reach → Interaction → Click"""
    st.markdown(
        '<div class="section-header">🔄 ENGAGEMENT FUNNEL — REACH → INTERACTION → CLICK</div>',
        unsafe_allow_html=True
    )

    if df.empty:
        st.info("Tidak ada data tersedia.")
        return

    total_reach       = int(df['impressions'].sum())
    total_interaction = int((df['likes'] + df['comments'] + df['shares']).sum())
    total_clicks      = int(df['link_clicks'].sum())

    r_to_i = total_interaction / max(total_reach, 1) 
    i_to_c = total_clicks / max(total_interaction, 1) 
    r_to_c = total_clicks / max(total_reach, 1) 

    col_funnel, col_rates = st.columns([3, 1])

    with col_funnel:
        fig = go.Figure()
        stages = ["👁️ Reach (Impressions)", "💬 Interaction (Engagement)", "🔗 Click (Link Clicks)"]
        values = [total_reach, total_interaction, total_clicks]
        colors = [NEON_BLUE, NEON_PURPLE, NEON_GREEN]

        fig.add_trace(go.Funnel(
            y=stages, x=values,
            textposition="auto",
            textinfo="value+percent initial",
            texttemplate="%{value:,}<br>(%{percentInitial:.1%})",
            textfont=dict(color=TEXT_PRIMARY, size=14),
            marker=dict(color=colors, line=dict(width=0)),
            connector=dict(
                line=dict(color=DARK_BG, width=2),
                fillcolor="rgba(45,51,72,1)"
            ),
        ))
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color=TEXT_SECONDARY, size=12),
            height=280,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_rates:
        for label, value, color in [
            ("Reach → Interaction", f"{r_to_i:.2f}%", NEON_BLUE),
            ("Interaction → Click", f"{i_to_c:.2f}%", NEON_PURPLE),
            ("Total Reach → Click", f"{r_to_c:.2f}%", NEON_ORANGE),
        ]:
            st.markdown(f"""
            <div style="background:linear-gradient(135deg,#1B1F2B 0%,#222838 100%);
                        border:1px solid #2D3348;border-left:3px solid {color};
                        border-radius:8px;padding:14px 16px;margin-bottom:10px;">
                <div style="font-size:9px;color:#8892A0;text-transform:uppercase;
                            letter-spacing:1px;">{label}</div>
                <div style="font-size:20px;font-weight:700;color:{color};">{value}</div>
            </div>
            """, unsafe_allow_html=True)

    # Per-platform breakdown
    platforms = _get_available_platforms(df)
    rate_cols = st.columns(len(platforms))

    for idx, platform in enumerate(platforms):
        pdf    = df[df["platform"] == platform]
        p_r    = pdf["impressions"].sum()
        p_i    = (pdf["likes"] + pdf["comments"] + pdf["shares"] + pdf["saves"]).sum()
        p_c    = pdf["link_clicks"].sum()
        r_i    = p_i / max(p_r, 1)
        i_c    = p_c / max(p_i, 1)
        color  = PLATFORM_COLORS.get(platform, "#FFFFFF")
        icon   = PLATFORM_ICONS.get(platform, "📄")

        with rate_cols[idx]:
            st.markdown(f"""
            <div style="background:linear-gradient(135deg,#1B1F2B 0%,#222838 100%);
                        border:1px solid #2D3348;border-top:2px solid {color};
                        border-radius:8px;padding:14px;text-align:center;">
                <div style="font-size:12px;color:#8892A0;margin-bottom:8px;">
                    {icon} {platform}
                </div>
                <div style="display:flex;justify-content:space-around;">
                    <div>
                        <div style="font-size:20px;font-weight:700;color:{NEON_BLUE};">
                            {r_i:.1f}%</div>
                        <div style="font-size:11px;color:#5A6577;">R→I Rate</div>
                    </div>
                    <div style="width:1px;background:#2D3348;"></div>
                    <div>
                        <div style="font-size:20px;font-weight:700;color:{NEON_ORANGE};">
                            {i_c:.1f}%</div>
                        <div style="font-size:11px;color:#5A6577;">I→C Rate</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)


def render_content_leaderboard(df_content: pd.DataFrame, platform_filter: list):
    """
    Visualization B: Content Leaderboard
    Top 3 Posts: Most Shared, Most Commented, Most Clicked
    """
    st.markdown(
        '<div class="section-header">🏅 CONTENT LEADERBOARD — TOP PERFORMERS</div>',
        unsafe_allow_html=True
    )

    content_df = df_content.copy()
    if platform_filter:
        content_df = content_df[content_df["platform"].isin(platform_filter)]

    if content_df.empty:
        st.info("Tidak ada konten tersedia untuk filter yang dipilih.")
        return

    if content_df.empty or len(content_df) < 1:
        st.info("Belum ada data post tersedia.")
        return

    most_shared    = content_df.loc[content_df["shares"].idxmax()]
    most_commented = content_df.loc[content_df["comments"].idxmax()]
    most_clicked   = content_df.loc[content_df["link_clicks"].idxmax()]

    winners = [
        {
            "badge": "🏆", "badge_label": "MOST SHARED",
            "badge_subtitle": "Brand Awareness Winner",
            "post": most_shared, "highlight_metric": "shares",
            "highlight_value": most_shared["shares"],
            "accent_color": NEON_YELLOW,
            "rank_bg": "linear-gradient(135deg,rgba(255,215,0,0.12) 0%,rgba(255,215,0,0.03) 100%)",
            "border_color": "rgba(255,215,0,0.4)",
        },
        {
            "badge": "💬", "badge_label": "MOST COMMENTED",
            "badge_subtitle": "Community Winner",
            "post": most_commented, "highlight_metric": "comments",
            "highlight_value": most_commented["comments"],
            "accent_color": NEON_PURPLE,
            "rank_bg": "linear-gradient(135deg,rgba(168,85,247,0.12) 0%,rgba(168,85,247,0.03) 100%)",
            "border_color": "rgba(168,85,247,0.4)",
        },
        {
            "badge": "🔗", "badge_label": "MOST CLICKED",
            "badge_subtitle": "Traffic Winner",
            "post": most_clicked, "highlight_metric": "link_clicks",
            "highlight_value": most_clicked["link_clicks"],
            "accent_color": NEON_ORANGE,
            "rank_bg": "linear-gradient(135deg,rgba(255,107,53,0.12) 0%,rgba(255,107,53,0.03) 100%)",
            "border_color": "rgba(255,107,53,0.4)",
        },
    ]

    cols = st.columns(3)
    for idx, winner in enumerate(winners):
        post      = winner["post"]
        platform  = post.get("platform", "Unknown")
        p_color   = PLATFORM_COLORS.get(platform, "#444")
        p_icon    = PLATFORM_ICONS.get(platform, "📄")
        accent    = winner["accent_color"]
        h_label   = winner["highlight_metric"].replace("_", " ").title()

        date_val  = post.get('date', '')
        date_str  = date_val.strftime('%d %b %Y') if hasattr(date_val, 'strftime') else str(date_val)[:10]
        title     = str(post.get('title', 'Untitled Post'))[:50]

        with cols[idx]:
            st.markdown(f"""
            <div style="background:{winner['rank_bg']};border:1px solid {winner['border_color']};
                        border-radius:12px 12px 0 0;padding:16px 20px;text-align:center;">
                <div style="font-size:36px;margin-bottom:4px;">{winner['badge']}</div>
                <div style="font-size:12px;font-weight:700;color:{accent};
                            text-transform:uppercase;letter-spacing:2px;">{winner['badge_label']}</div>
                <div style="font-size:10px;color:#8892A0;margin-top:2px;">{winner['badge_subtitle']}</div>
            </div>
            <div style="background:rgba(0,0,0,0.15);border-left:1px solid {winner['border_color']};
                        border-right:1px solid {winner['border_color']};padding:14px 20px 0 20px;">
                <div style="display:flex;align-items:center;gap:10px;">
                    <div style="width:36px;height:36px;border-radius:8px;background:{p_color};
                                display:flex;align-items:center;justify-content:center;
                                font-size:18px;flex-shrink:0;">{p_icon}</div>
                    <div>
                        <div style="font-size:12px;font-weight:600;color:#FFF;">{title}</div>
                        <div style="font-size:9px;color:#5A6577;margin-top:2px;">
                            {platform} · {post.get('content_type','')} · {date_str}
                        </div>
                    </div>
                </div>
            </div>
            <div style="background:rgba(0,0,0,0.15);border-left:1px solid {winner['border_color']};
                        border-right:1px solid {winner['border_color']};padding:8px 20px;">
                <div style="text-align:center;padding:12px 0;background:rgba(0,0,0,0.25);border-radius:8px;">
                    <div style="font-size:28px;font-weight:700;color:{accent};">
                        {format_number(winner['highlight_value'])}</div>
                    <div style="font-size:9px;color:#8892A0;text-transform:uppercase;letter-spacing:1px;">
                        {h_label}</div>
                </div>
            </div>
            <div style="background:rgba(0,0,0,0.15);border-left:1px solid {winner['border_color']};
                        border-right:1px solid {winner['border_color']};border-bottom:1px solid {winner['border_color']};
                        border-radius:0 0 12px 12px;padding:8px 20px 14px 20px;">
                <table style="width:100%;border-collapse:separate;border-spacing:4px 0;">
                    <tr>
                        <td style="text-align:center;padding:8px 4px;background:rgba(0,0,0,0.2);
                                   border-radius:6px;width:33%;">
                            <div style="font-size:16px;font-weight:600;color:#FFF;">
                                {format_number(post.get('views',0))}</div>
                            <div style="font-size:10px;color:#5A6577;">Views</div>
                        </td>
                        <td style="text-align:center;padding:8px 4px;background:rgba(0,0,0,0.2);
                                   border-radius:6px;width:33%;">
                            <div style="font-size:16px;font-weight:600;color:#FFF;">
                                {format_number(post.get('likes',0))}</div>
                            <div style="font-size:10px;color:#5A6577;">Likes</div>
                        </td>
                        <td style="text-align:center;padding:8px 4px;background:rgba(0,0,0,0.2);
                                   border-radius:6px;width:33%;">
                            <div style="font-size:16px;font-weight:600;color:#FFF;">
                                {format_number(post.get('saves',0))}</div>
                            <div style="font-size:10px;color:#5A6577;">Saves</div>
                        </td>
                    </tr>
                </table>
                <table style="width:100%;border-collapse:separate;border-spacing:4px 0;margin-top:6px;">
                    <tr>
                        <td style="text-align:center;padding:5px 0;border-radius:6px;
                                   background:rgba(168,85,247,0.15);font-size:10px;
                                   font-weight:600;color:#A855F7;width:50%;">
                            🔥 Viral: {float(post.get('virality_score',0)):.1f}%</td>
                        <td style="text-align:center;padding:5px 0;border-radius:6px;
                                   background:rgba(0,212,255,0.15);font-size:10px;
                                   font-weight:600;color:#00D4FF;width:50%;">
                            🎯 Conv: {float(post.get('conversion_score',0)):.1f}%</td>
                    </tr>
                </table>
            </div>
            """, unsafe_allow_html=True)


# def render_ai_brain(df: pd.DataFrame, platform_filter: list):
#     """Section 4: AI Brain — The Community Manager"""
#     st.markdown(
#         '<div class="section-header">🧠 AI BRAIN — THE ADVISOR</div>',
#         unsafe_allow_html=True
#     )

#     insights = _generate_placeholder_insights(df, platform_filter)

#     st.markdown(f"""
#     <div style="display:flex;align-items:center;justify-content:space-between;
#                 padding:10px 16px;background:linear-gradient(135deg,#1B1F2B 0%,#222838 100%);
#                 border:1px solid #2D3348;border-radius:8px;margin-bottom:16px;">
#         <div style="display:flex;align-items:center;gap:8px;">
#             <div style="width:8px;height:8px;border-radius:50%;background:{NEON_GREEN};"></div>
#             <span style="font-size:11px;color:#8892A0;text-transform:uppercase;letter-spacing:1px;">
#                 AI Engine Status: Active</span>
#         </div>
#         <div style="font-size:10px;color:#5A6577;">
#             Last Analysis: Just now · Powered by OpenAI</div>
#     </div>
#     """, unsafe_allow_html=True)

#     for insight in insights:
#         _render_insight_card(insight)

#     st.markdown("<br>", unsafe_allow_html=True)
#     col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
#     with col_btn2:
#         if st.button("🧠 Generate New Insights", key="generate_ai_insights",
#                      use_container_width=True, type="primary"):
#             st.info("🔌 OpenAI API integration pending.")


# def _generate_placeholder_insights(df: pd.DataFrame, platform_filter: list) -> list:
#     """Generate placeholder insights dari data real."""
#     insights  = []
#     platforms = _get_available_platforms(df)

#     # Logic A: Platform dengan ER terendah
#     platform_er = {}
#     for p in platforms:
#         pdf = df[df["platform"] == p]
#         er  = (pdf["likes"].sum() + pdf["comments"].sum() + pdf["shares"].sum()) / \
#               max(pdf["impressions"].sum(), 1) * 100
#         platform_er[p] = er

#     if platform_er:
#         worst_p  = min(platform_er, key=platform_er.get)
#         worst_er = platform_er[worst_p]
#         topics   = {"Instagram": "Pricing", "Facebook": "Response Time",
#                     "TikTok": "Shipping", "YouTube": "Product Quality"}
#         topic    = topics.get(worst_p, "Service")

#         insights.append({
#             "logic_id": "A", "logic_name": "Sentiment Guard", "icon": "🛡️",
#             "severity": "warning" if worst_er < 5.0 else "info",
#             "title": f"Negative sentiment spike detected on {worst_p} regarding '{topic}'",
#             "body": f"Comment analysis shows {worst_er:.1f}% engagement rate dengan increasing "
#                     f"negative mentions around '{topic}' in the last 7 days.",
#             "recommendation": f"Address '{topic}' concerns in Stories. Post transparent Q&A "
#                               f"about your {topic.lower()} process.",
#             "accent_color": NEON_YELLOW if worst_er < 5.0 else NEON_BLUE,
#         })

#     # Logic B: Platform dengan pertumbuhan terbaik
#     platform_growth = {
#         p: df[df["platform"] == p]["follower_growth"].sum()
#         for p in platforms
#     }
#     if platform_growth:
#         best_p      = max(platform_growth, key=platform_growth.get)
#         best_growth = platform_growth[best_p]
#         trends      = {
#             "Instagram": ("Audio Track 'Espresso'", "Reel"),
#             "Facebook" : ("Format 'Behind The Scenes'", "Video"),
#             "TikTok"   : ("Sound 'APT. — ROSÉ'", "Short Video"),
#             "YouTube"  : ("Format 'Day in My Life'", "Short"),
#         }
#         trend_name, trend_fmt = trends.get(best_p, ("Trending format", "Post"))

#         insights.append({
#             "logic_id": "B", "logic_name": "Trend Spotter", "icon": "📈",
#             "severity": "success",
#             "title": f"{trend_name} is trending on {best_p}",
#             "body": f"Your audience on {best_p} (+{best_growth:,.0f} new followers) "
#                     f"is primed for this type of content. Early adopters seeing 2-3x normal reach.",
#             "recommendation": f"Use for next {trend_fmt}. Post within 48 hours to catch the wave.",
#             "accent_color": NEON_GREEN,
#         })

#     # Logic C: Platform dengan saves tinggi tapi reach rendah
#     platform_gap = {
#         p: {
#             "impressions": df[df["platform"] == p]["impressions"].sum(),
#             "save_rate"  : df[df["platform"] == p]["saves"].sum() /
#                            max(df[df["platform"] == p]["impressions"].sum(), 1) * 100,
#         }
#         for p in platforms
#     }
#     if platform_gap:
#         target_p  = min(platform_gap, key=lambda x: platform_gap[x]["impressions"])
#         gap_data  = platform_gap[target_p]

#         insights.append({
#             "logic_id": "C", "logic_name": "SEO Assist", "icon": "🔍",
#             "severity": "warning",
#             "title": f"High retention but low reach on {target_p} posts",
#             "body": f"Save rate on {target_p} is {gap_data['save_rate']:.1f}% (people love the content), "
#                     f"but impressions are only {format_number(gap_data['impressions'])}.",
#             "recommendation": "Switch generic hashtags for niche SEO keywords. "
#                               "Add keyword-rich captions and alt-text for discoverability.",
#             "accent_color": NEON_ORANGE,
#         })

#     return insights


# def _render_insight_card(insight: dict):
#     """Render satu AI insight card."""
#     severity_config = {
#         "critical": {"bg": "rgba(255,59,92,0.08)",  "border": "rgba(255,59,92,0.35)",
#                      "label_bg": "rgba(255,59,92,0.2)",  "label_color": NEON_RED,    "label": "CRITICAL"},
#         "warning" : {"bg": "rgba(255,215,0,0.06)",  "border": "rgba(255,215,0,0.3)",
#                      "label_bg": "rgba(255,215,0,0.15)", "label_color": NEON_YELLOW, "label": "WARNING"},
#         "success" : {"bg": "rgba(0,255,136,0.06)",  "border": "rgba(0,255,136,0.25)",
#                      "label_bg": "rgba(0,255,136,0.15)","label_color": NEON_GREEN,  "label": "POSITIVE"},
#         "info"    : {"bg": "rgba(0,212,255,0.06)",  "border": "rgba(0,212,255,0.25)",
#                      "label_bg": "rgba(0,212,255,0.15)","label_color": NEON_BLUE,   "label": "INFO"},
#     }
#     sev    = severity_config.get(insight["severity"], severity_config["info"])
#     accent = insight["accent_color"]

#     st.markdown(f"""
#     <div style="background:{sev['bg']};border:1px solid {sev['border']};
#                 border-radius:12px 12px 0 0;padding:14px 20px;">
#         <div style="display:flex;align-items:center;justify-content:space-between;">
#             <div style="display:flex;align-items:center;gap:10px;">
#                 <span style="font-size:22px;">{insight['icon']}</span>
#                 <div>
#                     <div style="font-size:14px;font-weight:600;color:#FFF;">{insight['title']}</div>
#                     <div style="font-size:11px;color:#5A6577;margin-top:2px;">
#                         Logic {insight['logic_id']}: {insight['logic_name']}</div>
#                 </div>
#             </div>
#             <div style="padding:3px 10px;border-radius:20px;background:{sev['label_bg']};
#                         font-size:9px;font-weight:700;color:{sev['label_color']};
#                         text-transform:uppercase;letter-spacing:1px;">{sev['label']}</div>
#         </div>
#     </div>
#     <div style="background:{sev['bg']};border-left:1px solid {sev['border']};
#                 border-right:1px solid {sev['border']};padding:0 20px 12px 20px;">
#         <div style="font-size:13px;color:#C0C7D0;line-height:1.7;padding:10px 14px;
#                     background:rgba(0,0,0,0.15);border-radius:8px;margin-top:8px;">
#             <span style="font-size:9px;color:#5A6577;text-transform:uppercase;
#                          letter-spacing:1px;display:block;margin-bottom:6px;">📊 Analysis</span>
#             {insight['body']}
#         </div>
#     </div>
#     <div style="background:{sev['bg']};border:1px solid {sev['border']};border-top:none;
#                 border-radius:0 0 12px 12px;padding:0 20px 14px 20px;">
#         <div style="font-size:13px;color:#C0C7D0;line-height:1.7;padding:10px 14px;
#                     background:rgba(0,0,0,0.1);border-radius:8px;border-left:3px solid {accent};">
#             <span style="font-size:9px;color:{accent};text-transform:uppercase;
#                          letter-spacing:1px;display:block;margin-bottom:6px;">💡 Recommendation</span>
#             {insight['recommendation']}
#         </div>
#     </div>
#     <div style='height:12px;'></div>
#     """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
# 4. MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════

def show_organic_architecture():
    load_module2_css()

    st.markdown("""
    <div style="background:linear-gradient(135deg,#1B1F2B 0%,#13161F 100%);border:1px solid #2D3348;border-radius:12px;padding:20px 28px;margin-bottom:28px;position:relative;overflow:hidden;">
        <div style="position:absolute;top:0;right:0;width:220px;height:100%;background:radial-gradient(ellipse at 80% 50%,rgba(0,255,136,0.06) 0%,transparent 70%);pointer-events:none;"></div>
        <div style="display:flex;align-items:center;gap:16px;margin-bottom:14px;">
            <div style="width:4px;height:52px;border-radius:4px;background:linear-gradient(180deg,#00FF88 0%,transparent 100%);box-shadow:0 0 14px rgba(0,255,136,0.55);flex-shrink:0;"></div>
            <div>
                <div style="font-size:10px;font-weight:700;letter-spacing:2.5px;color:#00FF88;text-transform:uppercase;font-family:monospace;margin-bottom:5px;opacity:0.9;">MODULE 02 &nbsp;·&nbsp; BRAND TERMINAL</div>
                <div style="font-size:26px;font-weight:800;color:#FFFFFF;line-height:1.2;letter-spacing:-0.3px;">📱 Organic Architecture</div>
                <div style="font-size:13px;color:#8892A0;margin-top:5px;line-height:1.5;">Brand Resonance, Community Loyalty &amp; Traffic Contribution</div>
            </div>
        </div>
        <div style="height:1px;background:linear-gradient(to right,rgba(0,255,136,0.35),transparent);"></div>
    </div>
    """, unsafe_allow_html=True)

    # ── Filters ────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns([2, 2, 2])

    with col_f1:
        date_range = st.selectbox(
            "📅 Time Range",
            ["Last 7 Days", "Last 14 Days", "Last 30 Days"],
            index=2,
            key="organic_date_range"
        )

    _loader_temp         = DataLoader(portfolio='all')
    available_portfolios = _loader_temp.get_portfolios()
    portfolio_options    = ['All Portfolios'] + available_portfolios

    with col_f2:
        selected_portfolio = st.selectbox(
            "🏢 Portfolio",
            portfolio_options,
            index=0,
            key="organic_portfolio_filter"
        )

    # ── Load Data ──────────────────────────────────────────────────
    days_map      = {"Last 7 Days": 7, "Last 14 Days": 14, "Last 30 Days": 30}
    days          = days_map.get(date_range, 30)
    portfolio_val = 'all' if selected_portfolio == 'All Portfolios' else selected_portfolio

    # ⚠️ Clear cache dulu kalau portfolio berubah
    # supaya DataLoader tidak return data portfolio lama
    if st.session_state.get('last_portfolio') != portfolio_val:
        DataLoader.refresh_cache()
        st.session_state['last_portfolio'] = portfolio_val

    loader     = DataLoader(portfolio=portfolio_val)
    organic_df = loader.load_organic_data(days=days)
    content_df = loader.load_content_library(days=days)

    # ── Validasi Data ──────────────────────────────────────────────
    if organic_df.empty:
        st.warning("""
        ⚠️ Data organic belum tersedia.
        Jalankan dulu: `notebooks/update_data.ipynb`
        """)
        return

    available_platforms = _get_available_platforms(organic_df)

    with col_f3:
        platform_filter = st.multiselect(
            "🌐 Platform",
            available_platforms,
            default=available_platforms,
            key="organic_platform_filter"
        )

    # ── Apply filter platform ke semua data ───────────────────────
    # Dilakukan SEBELUM render apapun
    if platform_filter:
        organic_filtered = organic_df[organic_df['platform'].isin(platform_filter)]
        content_filtered = content_df[content_df['platform'].isin(platform_filter)] \
                           if not content_df.empty else content_df
    else:
        organic_filtered = organic_df
        content_filtered = content_df

    # ✅ Baru — pass content_df juga
    organic_insights = generate_organic_insights(organic_filtered, content_filtered)
    divider = '<div style="height:1px;background:linear-gradient(to right,transparent,#2D3348,transparent);margin:24px 0;"></div>'

    # ── Section 1A: Cross-Channel Pulse ───────────────────────────
    # ✅ Pakai organic_filtered, bukan organic_df
    st.markdown(divider, unsafe_allow_html=True)
    render_cross_channel_pulse(organic_filtered)
    render_inline_insight(organic_insights.get("cross_channel", {}))

    # ── Section 1B: Content Library ───────────────────────────────
    st.markdown(divider, unsafe_allow_html=True)
    content_library_section(content_filtered, platform_filter)
    render_inline_insight(organic_insights.get("content_library", {}))
    

    # ── Section 2: Metric Stacks ───────────────────────────────────
    st.markdown(divider, unsafe_allow_html=True)
    render_metrics_stacks(organic_filtered, df_content=content_filtered)
    render_inline_insight(organic_insights.get("metric_stack", {}))

    # ── Section 3A: Engagement Funnel ─────────────────────────────
    st.markdown(divider, unsafe_allow_html=True)
    render_engagement_funnel(organic_filtered)
    render_inline_insight(organic_insights.get("engagement_funnel", {}))

    # ── Section 3B: Content Leaderboard ───────────────────────────
    st.markdown(divider, unsafe_allow_html=True)
    render_content_leaderboard(content_filtered, platform_filter)
    render_inline_insight(organic_insights.get("leaderboard", {}))

    # # ── Section 4: AI Brain ────────────────────────────────────────
    # st.markdown(divider, unsafe_allow_html=True)
    # render_ai_brain(organic_filtered, platform_filter)

    # st.markdown(divider, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# 5. STANDALONE TEST MODE
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    st.set_page_config(
        page_title="Marktivo Growth OS — Organic Architecture",
        page_icon="📱",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    show_organic_architecture()
