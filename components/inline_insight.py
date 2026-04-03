"""
components/inline_insight.py
=============================
Render AI insight card di bawah setiap section dashboard.
"""

import streamlit as st


# ══════════════════════════════════════════════════════════════════
# RENDER: render_inline_insight()
# ══════════════════════════════════════════════════════════════════

def render_inline_insight(insight: dict):
    """
    Render 1 AI insight card di bawah section.
    Semua inline style — tidak ada CSS class external.
    """
    if not insight or not isinstance(insight, dict):
        return

    ins_text = insight.get('insight', '')
    rec_text = insight.get('recommendation', '')
    severity = insight.get('severity', 'info')

    if not ins_text or ins_text == 'N/A':
        return

    severity_map = {
        'success' : {
            'bg': 'rgba(0,255,136,0.05)', 'border': 'rgba(0,255,136,0.25)',
            'label_bg': 'rgba(0,255,136,0.15)', 'label_color': '#00FF88',
            'label': 'POSITIVE', 'icon': '&#9989;', 'rec_border': '#00FF88',
        },
        'warning' : {
            'bg': 'rgba(255,215,0,0.05)', 'border': 'rgba(255,215,0,0.25)',
            'label_bg': 'rgba(255,215,0,0.15)', 'label_color': '#FFD700',
            'label': 'WARNING', 'icon': '&#9888;', 'rec_border': '#FFD700',
        },
        'critical': {
            'bg': 'rgba(255,59,92,0.05)', 'border': 'rgba(255,59,92,0.25)',
            'label_bg': 'rgba(255,59,92,0.15)', 'label_color': '#FF3B5C',
            'label': 'CRITICAL', 'icon': '&#128680;', 'rec_border': '#FF3B5C',
        },
        'info'    : {
            'bg': 'rgba(0,212,255,0.05)', 'border': 'rgba(0,212,255,0.2)',
            'label_bg': 'rgba(0,212,255,0.12)', 'label_color': '#00D4FF',
            'label': 'INSIGHT', 'icon': '&#129504;', 'rec_border': '#00D4FF',
        },
    }

    s = severity_map.get(severity, severity_map['info'])

    # Satu st.markdown() call — semua inline
    st.markdown(
        f'<div style="margin:12px 0 0 0;border-radius:10px;border:1px solid {s["border"]};background:{s["bg"]};overflow:hidden;">'

        # Header
        f'<div style="display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-bottom:1px solid {s["border"]};">'
        f'<div style="display:flex;align-items:center;gap:8px;">'
        f'<span style="font-size:14px;">{s["icon"]}</span>'
        f'<span style="font-size:11px;font-weight:600;color:#8892A0;text-transform:uppercase;letter-spacing:1px;">AI Analysis</span>'
        f'</div>'
        f'<span style="padding:2px 8px;border-radius:20px;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:1px;background:{s["label_bg"]};color:{s["label_color"]};">{s["label"]}</span>'
        f'</div>'

        # Insight
        f'<div style="padding:12px 16px 0 16px;">'
        f'<div style="font-size:9px;color:#5A6577;text-transform:uppercase;letter-spacing:1px;margin-bottom:5px;">Analysis</div>'
        f'<div style="font-size:13px;color:#C0C7D0;line-height:1.7;">{ins_text}</div>'
        f'</div>'

        # Recommendation
        f'<div style="padding:10px 16px 14px 16px;">'
        f'<div style="border-left:3px solid {s["rec_border"]};padding:10px 12px;border-radius:0 6px 6px 0;background:rgba(0,0,0,0.15);">'
        f'<div style="font-size:9px;color:{s["rec_border"]};text-transform:uppercase;letter-spacing:1px;margin-bottom:5px;font-weight:600;">Recommendation</div>'
        f'<div style="font-size:13px;color:#C0C7D0;line-height:1.7;">{rec_text}</div>'
        f'</div>'
        f'</div>'

        f'</div>',
        unsafe_allow_html=True
    )


# ══════════════════════════════════════════════════════════════════
# HELPER: generate_organic_insights()
# Dipanggil dari organic_architecture.py
# ══════════════════════════════════════════════════════════════════

def generate_organic_insights(organic_df, content_df=None) -> dict:
    """
    Generate semua AI insights untuk Module 2.
    Pakai session_state cache supaya tidak re-generate tiap render.

    Args:
        organic_df : DataFrame dari load_organic_data()
        content_df : DataFrame dari load_content_library()

    Return: dict {section: insight_dict}
    """
    import pandas as pd

    if organic_df is None or organic_df.empty:
        return {}

    if content_df is None:
        content_df = pd.DataFrame()

    # ── Cache key dari shape + date range data ────────────────────
    cache_key = (
        f"organic_insights_"
        f"{len(organic_df)}_"
        f"{str(organic_df['date'].min())[:10]}_"
        f"{str(organic_df['date'].max())[:10]}_"
        f"{sorted(organic_df['portfolio'].unique().tolist())}"
    )

    # Return dari cache kalau data belum berubah
    if st.session_state.get('insight_cache_key') == cache_key:
        return st.session_state.get('organic_insights', {})

    # ── Generate insights ─────────────────────────────────────────
    try:
        from services.ai_service import OrganicInsightGenerator

        with st.spinner('🧠 Generating AI insights...'):
            generator = OrganicInsightGenerator(organic_df, content_df)
            insights  = generator.generate_all()

        # Simpan ke session_state
        st.session_state['organic_insights']  = insights
        st.session_state['insight_cache_key'] = cache_key

        return insights

    except Exception as e:
        st.warning(f"AI insights unavailable: {e}")
        return {}
