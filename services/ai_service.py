"""
services/ai_service.py
======================
AI Insight Generator untuk Marktivo Dashboard.
Menggunakan OpenAI GPT-4o-mini untuk generate insights per section.

Usage:
    from services.ai_service import OrganicInsightGenerator

    generator = OrganicInsightGenerator(organic_df, content_df)
    insights  = generator.generate_all()
"""

import os
import json
import hashlib
import streamlit as st
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

MODEL      = 'gpt-4o-mini'
MAX_TOKENS = 600

def _get_openai_key() -> str:
    """Baca API key — support Streamlit Cloud secrets & lokal .env."""
    # 1. Streamlit secrets (Streamlit Cloud)
    try:
        key = st.secrets.get("OPENAI_API_KEY", "")
        if key:
            return key
    except Exception:
        pass
    # 2. Environment variable / .env (lokal)
    return os.getenv("OPENAI_API_KEY", "")


# ══════════════════════════════════════════════════════════════════
# HELPER: Format data ringkas untuk prompt
# ══════════════════════════════════════════════════════════════════

def _fmt_num(n):
    """Format number jadi readable."""
    try:
        n = float(n)
        if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
        if n >= 1_000    : return f"{n/1_000:.1f}K"
        return str(int(n))
    except: return "0"

def _summarize_organic(df: pd.DataFrame) -> dict:
    """Ringkas organic_df jadi dict untuk prompt."""
    if df.empty:
        return {}

    summary = {}
    for platform in df['platform'].unique():
        pdf = df[df['platform'] == platform]

        # Followers
        followers_start = int(pdf.sort_values('date')['followers'].iloc[0])
        followers_end   = int(pdf.sort_values('date')['followers'].iloc[-1])
        follower_growth = int(pdf['follower_growth'].sum())

        # Engagement totals
        total_impressions = int(pdf['impressions'].sum())
        total_likes       = int(pdf['likes'].sum())
        total_comments    = int(pdf['comments'].sum())
        total_shares      = int(pdf['shares'].sum())
        total_saves       = int(pdf['saves'].sum())
        total_views       = int(pdf['views'].sum())
        total_link_clicks = int(pdf['link_clicks'].sum())
        total_profile_vis = int(pdf['profile_visits'].sum())
        total_posts       = int(pdf['posts_published'].sum())

        # Rates
        total_eng    = total_likes + total_comments + total_shares
        er           = round(total_eng / max(total_impressions, 1) * 100, 2)
        sov          = round((total_saves + total_shares) / max(total_impressions, 1) * 100, 2)
        pcr          = round(total_link_clicks / max(total_profile_vis, 1) * 100, 2)
        days         = max(pdf['date'].nunique(), 1)
        goal_weekly  = pdf['posts_goal_weekly'].iloc[0] if len(pdf) > 0 else 4
        consistency  = round(total_posts / max(goal_weekly / 7 * days, 1) * 100, 1)

        summary[platform] = {
            'followers_start' : followers_start,
            'followers_end'   : followers_end,
            'follower_growth' : follower_growth,
            'impressions'     : total_impressions,
            'views'           : total_views,
            'likes'           : total_likes,
            'comments'        : total_comments,
            'shares'          : total_shares,
            'saves'           : total_saves,
            'link_clicks'     : total_link_clicks,
            'profile_visits'  : total_profile_vis,
            'posts_published' : total_posts,
            'er_pct'          : er,
            'sov_pct'         : sov,
            'pcr_pct'         : pcr,
            'consistency_pct' : consistency,
            'days'            : days,
        }

    return summary


def _summarize_content(df: pd.DataFrame) -> dict:
    """Ringkas content_library jadi dict untuk prompt."""
    if df.empty:
        return {}

    summary = {}
    for platform in df['platform'].unique():
        pc = df[df['platform'] == platform]

        # Top posts
        top_views    = pc.nlargest(3, 'views')[['title','views','likes','shares','saves','content_type']].to_dict('records')
        top_likes    = pc.nlargest(3, 'likes')[['title','likes','content_type']].to_dict('records')
        top_viral    = pc.nlargest(3, 'virality_score')[['title','virality_score','content_type']].to_dict('records')

        # Content type breakdown
        type_counts  = pc['content_type'].value_counts().to_dict()

        # Avg metrics
        avg_views    = round(pc['views'].mean(), 0)
        avg_likes    = round(pc['likes'].mean(), 1)
        avg_saves    = round(pc['saves'].mean(), 1)
        avg_virality = round(pc['virality_score'].mean(), 2)

        summary[platform] = {
            'total_posts'  : len(pc),
            'type_counts'  : type_counts,
            'avg_views'    : avg_views,
            'avg_likes'    : avg_likes,
            'avg_saves'    : avg_saves,
            'avg_virality' : avg_virality,
            'top_views'    : top_views[:3],
            'top_likes'    : top_likes[:3],
            'top_viral'    : top_viral[:3],
        }

    return summary


def _make_cache_key(data: dict, section: str) -> str:
    """Generate cache key dari data + section."""
    raw = json.dumps(data, sort_keys=True, default=str) + section
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# ══════════════════════════════════════════════════════════════════
# CLASS: OrganicInsightGenerator
# ══════════════════════════════════════════════════════════════════

class OrganicInsightGenerator:
    """
    Generate AI insights untuk Module 2 — Organic Architecture.
    Per section: cross_channel, metric_stack, content_library,
                 engagement_funnel, leaderboard.
    """

    def __init__(self, organic_df: pd.DataFrame, content_df: pd.DataFrame):
        self.organic_df  = organic_df
        self.content_df  = content_df
        self.org_summary = _summarize_organic(organic_df)
        self.cnt_summary = _summarize_content(content_df)

        # Date range info
        if not organic_df.empty and 'date' in organic_df.columns:
            self.since = str(organic_df['date'].min())[:10]
            self.until = str(organic_df['date'].max())[:10]
        else:
            self.since = self.until = 'N/A'

    def _call_gpt(self, system_prompt: str, user_prompt: str,
                  cache_key: str) -> dict:
        """
        Call OpenAI API dengan caching di session_state.
        Return: dict {insight, recommendation, severity}
        """
        state_key = f'ai_insight_{cache_key}'

        if state_key in st.session_state:
            return st.session_state[state_key]

        # Baca key fresh setiap call — support Streamlit secrets
        api_key = _get_openai_key()
        if not api_key:
            fallback = {
                'insight'        : 'OpenAI API key not configured.',
                'recommendation' : 'Add OPENAI_API_KEY to Streamlit secrets or .env file.',
                'severity'       : 'warning',
            }
            st.session_state[state_key] = fallback
            return fallback

        try:
            client   = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model          = MODEL,
                max_tokens     = MAX_TOKENS,
                messages       = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user"  , "content": user_prompt},
                ],
                response_format= {"type": "json_object"},
            )

            raw    = response.choices[0].message.content
            result = json.loads(raw)

            for key in ['insight', 'recommendation', 'severity']:
                if key not in result:
                    result[key] = 'N/A'

            st.session_state[state_key] = result
            return result

        except Exception as e:
            fallback = {
                'insight'        : f'Unable to generate insight: {str(e)}',
                'recommendation' : 'Please check your OpenAI API key and try again.',
                'severity'       : 'info',
            }
            st.session_state[state_key] = fallback
            return fallback

    # ──────────────────────────────────────────────────────────────
    # SYSTEM PROMPT — shared base
    # ──────────────────────────────────────────────────────────────

    @property
    def _base_system(self) -> str:
        return """You are a senior social media strategist and data analyst for Marktivo, 
a marketing agency. You analyze social media performance data and provide sharp, 
actionable insights for business owners.

Your response must be a valid JSON object with exactly these keys:
- "insight": A direct, data-driven observation. Be specific with numbers. 2-4 sentences.
- "recommendation": A concrete, prioritized action plan. Use numbered steps if multiple actions. 2-5 sentences.  
- "severity": One of: "success" (performing well), "warning" (needs attention), "critical" (urgent action needed), "info" (neutral observation)

Rules:
- Always reference specific numbers from the data
- Be direct and actionable — no vague advice
- Use business language, not technical jargon
- Focus on what matters most for growth
- English only"""

    # ──────────────────────────────────────────────────────────────
    # SECTION 1: Cross-Channel Pulse
    # ──────────────────────────────────────────────────────────────

    def generate_cross_channel(self) -> dict:
        """Insight untuk Cross-Channel Pulse (follower growth)."""
        cache_key = _make_cache_key(self.org_summary, 'cross_channel')

        lines = [f"Period: {self.since} to {self.until}\n"]
        for platform, data in self.org_summary.items():
            lines.append(
                f"{platform}:\n"
                f"  Followers: {data['followers_start']:,} → {data['followers_end']:,} "
                f"(net: {data['follower_growth']:+,} over {data['days']} days)\n"
                f"  Avg daily growth: {data['follower_growth']/max(data['days'],1):+.1f}/day\n"
                f"  Impressions: {_fmt_num(data['impressions'])}\n"
                f"  Posts published: {data['posts_published']}\n"
            )

        user_prompt = (
            "Analyze the cross-platform follower growth performance:\n\n"
            + "\n".join(lines) +
            "\nFocus on: which platform is growing fastest, any concerning drops, "
            "and what's driving the growth (or lack thereof)."
        )

        return self._call_gpt(self._base_system, user_prompt, cache_key)

    # ──────────────────────────────────────────────────────────────
    # SECTION 2: Metric Stack
    # ──────────────────────────────────────────────────────────────

    def generate_metric_stack(self) -> dict:
        """Insight untuk Metric Stack (ER, SoV, PCR, Consistency)."""
        cache_key = _make_cache_key(self.org_summary, 'metric_stack')

        benchmarks = {'er': 5.0, 'sov': 2.0, 'pcr': 3.0, 'consistency': 80.0}
        lines      = [f"Period: {self.since} to {self.until}\n",
                      f"Benchmarks: ER>{benchmarks['er']}%, SoV>{benchmarks['sov']}%, "
                      f"PCR>{benchmarks['pcr']}%, Consistency>{benchmarks['consistency']}%\n"]

        for platform, data in self.org_summary.items():
            er_vs  = "ABOVE" if data['er_pct']          >= benchmarks['er']          else "BELOW"
            sv_vs  = "ABOVE" if data['sov_pct']         >= benchmarks['sov']         else "BELOW"
            pc_vs  = "ABOVE" if data['pcr_pct']         >= benchmarks['pcr']         else "BELOW"
            cs_vs  = "ABOVE" if data['consistency_pct'] >= benchmarks['consistency'] else "BELOW"

            lines.append(
                f"{platform}:\n"
                f"  Engagement Rate    : {data['er_pct']:.2f}% ({er_vs} benchmark)\n"
                f"  Share of Voice     : {data['sov_pct']:.2f}% ({sv_vs} benchmark)\n"
                f"  Profile Conv. Rate : {data['pcr_pct']:.2f}% ({pc_vs} benchmark)\n"
                f"  Consistency Score  : {data['consistency_pct']:.1f}% ({cs_vs} benchmark)\n"
                f"  Total engagement   : {_fmt_num(data['likes']+data['comments']+data['shares'])}\n"
            )

        user_prompt = (
            "Analyze the engagement metrics vs benchmarks:\n\n"
            + "\n".join(lines) +
            "\nFocus on: which metrics are most off-target, root cause analysis, "
            "and the highest-impact action to improve performance."
        )

        return self._call_gpt(self._base_system, user_prompt, cache_key)

    # ──────────────────────────────────────────────────────────────
    # SECTION 3: Content Library
    # ──────────────────────────────────────────────────────────────

    def generate_content_library(self) -> dict:
        """Insight untuk Content Library (top posts, content mix)."""
        cache_key = _make_cache_key(self.cnt_summary, 'content_library')

        lines = [f"Period: {self.since} to {self.until}\n"]
        for platform, data in self.cnt_summary.items():
            top_v = data['top_views']
            top_viral = data['top_viral']

            lines.append(
                f"{platform}:\n"
                f"  Total posts: {data['total_posts']}\n"
                f"  Content mix: {data['type_counts']}\n"
                f"  Avg views: {_fmt_num(data['avg_views'])} | "
                f"Avg likes: {data['avg_likes']} | "
                f"Avg saves: {data['avg_saves']} | "
                f"Avg virality: {data['avg_virality']}%\n"
                f"  Top by views: {top_v[0]['title'][:50] if top_v else 'N/A'} "
                f"({_fmt_num(top_v[0]['views']) if top_v else 0} views, "
                f"type: {top_v[0]['content_type'] if top_v else 'N/A'})\n"
                f"  Top viral: {top_viral[0]['title'][:50] if top_viral else 'N/A'} "
                f"(virality: {top_viral[0]['virality_score'] if top_viral else 0}%)\n"
            )

        user_prompt = (
            "Analyze content performance and content mix:\n\n"
            + "\n".join(lines) +
            "\nFocus on: what content type/format is winning, "
            "what patterns make top posts successful, "
            "and specific content strategy recommendations."
        )

        return self._call_gpt(self._base_system, user_prompt, cache_key)

    # ──────────────────────────────────────────────────────────────
    # SECTION 4: Engagement Funnel
    # ──────────────────────────────────────────────────────────────

    def generate_engagement_funnel(self) -> dict:
        """Insight untuk Engagement Funnel (Reach → Interaction → Click)."""
        cache_key = _make_cache_key(self.org_summary, 'engagement_funnel')

        lines = [f"Period: {self.since} to {self.until}\n"]
        for platform, data in self.org_summary.items():
            total_eng = data['likes'] + data['comments'] + data['shares']
            r_to_i    = round(total_eng / max(data['impressions'], 1) * 100, 2)
            i_to_c    = round(data['link_clicks'] / max(total_eng, 1) * 100, 2)
            r_to_c    = round(data['link_clicks'] / max(data['impressions'], 1) * 100, 3)

            lines.append(
                f"{platform}:\n"
                f"  Reach (impressions)  : {_fmt_num(data['impressions'])}\n"
                f"  Interaction (eng)    : {_fmt_num(total_eng)} "
                f"→ R-to-I rate: {r_to_i}%\n"
                f"  Clicks (link clicks) : {_fmt_num(data['link_clicks'])} "
                f"→ I-to-C rate: {i_to_c}%\n"
                f"  Total R-to-C rate    : {r_to_c}%\n"
                f"  Profile visits       : {_fmt_num(data['profile_visits'])}\n"
            )

        user_prompt = (
            "Analyze the engagement funnel from reach to click:\n\n"
            + "\n".join(lines) +
            "\nFocus on: where the biggest drop-off is in the funnel, "
            "which platform converts best, "
            "and specific tactics to improve the weakest funnel stage."
        )

        return self._call_gpt(self._base_system, user_prompt, cache_key)

    # ──────────────────────────────────────────────────────────────
    # SECTION 5: Content Leaderboard
    # ──────────────────────────────────────────────────────────────

    def generate_leaderboard(self) -> dict:
        """Insight untuk Content Leaderboard (top performers)."""
        cache_key = _make_cache_key(self.cnt_summary, 'leaderboard')

        lines = [f"Period: {self.since} to {self.until}\n"]
        for platform, data in self.cnt_summary.items():
            tv = data['top_views']
            tl = data['top_likes']
            tr = data['top_viral']

            lines.append(
                f"{platform} Top Performers:\n"
                f"  Most viewed  : {tv[0]['title'][:60] if tv else 'N/A'} "
                f"({_fmt_num(tv[0]['views']) if tv else 0} views, "
                f"{tv[0]['likes'] if tv else 0} likes, "
                f"type: {tv[0]['content_type'] if tv else 'N/A'})\n"
                f"  Most liked   : {tl[0]['title'][:60] if tl else 'N/A'} "
                f"({tl[0]['likes'] if tl else 0} likes)\n"
                f"  Most viral   : {tr[0]['title'][:60] if tr else 'N/A'} "
                f"(virality: {tr[0]['virality_score'] if tr else 0}%)\n"
            )

        user_prompt = (
            "Analyze the top performing content this period:\n\n"
            + "\n".join(lines) +
            "\nFocus on: what makes these posts winners (format, topic, timing), "
            "patterns to replicate, "
            "and how to turn these wins into a repeatable content strategy."
        )

        return self._call_gpt(self._base_system, user_prompt, cache_key)

    # ──────────────────────────────────────────────────────────────
    # MASTER: generate_all()
    # ──────────────────────────────────────────────────────────────

    def generate_all(self) -> dict:
        """
        Generate semua insights sekaligus.
        Return: dict dengan key per section.
        """
        return {
            'cross_channel'   : self.generate_cross_channel(),
            'metric_stack'    : self.generate_metric_stack(),
            'content_library' : self.generate_content_library(),
            'engagement_funnel': self.generate_engagement_funnel(),
            'leaderboard'     : self.generate_leaderboard(),
        }


# ══════════════════════════════════════════════════════════════════
# CLASS: RevenueInsightGenerator
# Generate AI insights untuk Module 1 — Revenue Engineering
# ══════════════════════════════════════════════════════════════════

class RevenueInsightGenerator:
    """
    Generate AI insights untuk Module 1 — Revenue Engineering.
    Per section: north_star, campaign_terminal, context_graph.
    """

    def __init__(self, ads_df: pd.DataFrame):
        self.df     = ads_df.copy()

        if not ads_df.empty and 'date' in ads_df.columns:
            self.since = str(ads_df['date'].min())[:10]
            self.until = str(ads_df['date'].max())[:10]
        else:
            self.since = self.until = 'N/A'

    @property
    def _base_system(self) -> str:
        return """You are a senior performance marketing strategist specializing in 
lead generation for property businesses in Indonesia. You analyze Meta Ads data 
and provide sharp, actionable insights for business owners.

Context: This is a property/real estate lead gen business. There are no direct 
purchases tracked — success is measured by clicks, CTR, CPM, and CPC efficiency.
Campaigns are categorized as TOF (Awareness) or BOF (Lead Gen).

Your response must be a valid JSON object with exactly these keys:
- "insight": Direct, data-driven observation with specific numbers. 2-4 sentences.
- "recommendation": Concrete action steps. Use numbered list if multiple. 2-5 sentences.
- "severity": "success" | "warning" | "critical" | "info"

Rules:
- Always reference specific numbers from the data
- Be direct — no vague advice
- Focus on spend efficiency and lead quality
- Indonesian property market context
- English only"""

    def _call_gpt(self, user_prompt: str, cache_key: str) -> dict:
        state_key = f'rev_ai_{cache_key}'
        if state_key in st.session_state:
            return st.session_state[state_key]

        api_key = _get_openai_key()
        if not api_key:
            fallback = {
                'insight'        : 'OpenAI API key not configured.',
                'recommendation' : 'Add OPENAI_API_KEY to Streamlit secrets or .env file.',
                'severity'       : 'warning',
            }
            st.session_state[state_key] = fallback
            return fallback

        try:
            client   = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model          = MODEL,
                max_tokens     = MAX_TOKENS,
                messages       = [
                    {"role": "system", "content": self._base_system},
                    {"role": "user"  , "content": user_prompt},
                ],
                response_format= {"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            for key in ['insight','recommendation','severity']:
                if key not in result: result[key] = 'N/A'
            st.session_state[state_key] = result
            return result
        except Exception as e:
            fallback = {
                'insight'        : f'Unable to generate insight: {str(e)}',
                'recommendation' : 'Check your OpenAI API key.',
                'severity'       : 'info',
            }
            st.session_state[state_key] = fallback
            return fallback

    def _summarize(self) -> dict:
        """Ringkas df jadi dict untuk prompt."""
        df = self.df

        total_spend  = df['spend'].sum()
        total_impr   = df['impressions'].sum()
        total_clicks = df['clicks'].sum()
        avg_ctr      = total_clicks / max(total_impr, 1) * 100
        avg_cpm      = total_spend  / max(total_impr, 1) * 1000
        avg_cpc      = total_spend  / max(total_clicks, 1)

        # Per campaign
        camp = df.groupby(['campaign_name','funnel_stage']).agg(
            spend=('spend','sum'),
            impressions=('impressions','sum'),
            clicks=('clicks','sum'),
        ).reset_index()
        camp['ctr'] = camp['clicks'] / camp['impressions'].clip(lower=1) * 100
        camp['cpm'] = camp['spend']  / camp['impressions'].clip(lower=1) * 1000
        camp['cpc'] = camp['spend']  / camp['clicks'].clip(lower=1)

        # Daily trend
        daily = df.groupby('date').agg(
            spend=('spend','sum'),
            clicks=('clicks','sum'),
            impressions=('impressions','sum'),
        ).reset_index()
        daily['ctr'] = daily['clicks'] / daily['impressions'].clip(lower=1) * 100

        best_day  = daily.loc[daily['ctr'].idxmax()]  if not daily.empty else None
        worst_day = daily.loc[daily['ctr'].idxmin()]  if not daily.empty else None

        return {
            'period'       : f"{self.since} to {self.until}",
            'total_spend'  : total_spend,
            'total_impr'   : total_impr,
            'total_clicks' : total_clicks,
            'avg_ctr'      : avg_ctr,
            'avg_cpm'      : avg_cpm,
            'avg_cpc'      : avg_cpc,
            'campaigns'    : camp.to_dict('records'),
            'best_day_ctr' : float(best_day['ctr']) if best_day is not None else 0,
            'best_day_date': str(best_day['date'])[:10] if best_day is not None else 'N/A',
            'worst_day_ctr': float(worst_day['ctr']) if worst_day is not None else 0,
            'portfolios'   : df['portfolio'].unique().tolist(),
        }

    def generate_north_star(self) -> dict:
        s         = self._summarize()
        cache_key = _make_cache_key(s, 'rev_north_star')

        camp_lines = '\n'.join([
            f"  - {c['campaign_name']} ({c['funnel_stage']}): "
            f"Spend Rp {c['spend']:,.0f} | CTR {c['ctr']:.2f}% | "
            f"CPM Rp {c['cpm']:,.0f} | CPC Rp {c['cpc']:,.0f}"
            for c in s['campaigns']
        ])

        prompt = f"""Analyze overall ad performance KPIs:

Period: {s['period']}
Portfolios: {s['portfolios']}

Overall:
  Total Spend   : Rp {s['total_spend']:,.0f}
  Impressions   : {s['total_impr']:,}
  Clicks        : {s['total_clicks']:,}
  Avg CTR       : {s['avg_ctr']:.2f}% (benchmark: >1.5% for property ads)
  Avg CPM       : Rp {s['avg_cpm']:,.0f}
  Avg CPC       : Rp {s['avg_cpc']:,.0f}

Campaigns:
{camp_lines}

Best day CTR  : {s['best_day_ctr']:.2f}% on {s['best_day_date']}
Worst day CTR : {s['worst_day_ctr']:.2f}%

Focus on: overall spend efficiency, CTR vs benchmark, 
which portfolio/campaign is performing best/worst."""

        return self._call_gpt(prompt, cache_key)

    def generate_campaign_terminal(self) -> dict:
        s         = self._summarize()
        cache_key = _make_cache_key(s, 'rev_campaign_terminal')

        tof_camps = [c for c in s['campaigns'] if c['funnel_stage'] == 'TOF']
        bof_camps = [c for c in s['campaigns'] if c['funnel_stage'] == 'BOF']

        tof_spend = sum(c['spend'] for c in tof_camps)
        bof_spend = sum(c['spend'] for c in bof_camps)
        tof_pct   = tof_spend / max(s['total_spend'], 1) * 100
        bof_pct   = bof_spend / max(s['total_spend'], 1) * 100

        prompt = f"""Analyze funnel stage performance and budget allocation:

Period: {s['period']}

Budget Split:
  TOF (Awareness): Rp {tof_spend:,.0f} ({tof_pct:.1f}% of total spend)
  BOF (Lead Gen) : Rp {bof_spend:,.0f} ({bof_pct:.1f}% of total spend)

TOF Campaigns:
{chr(10).join([f"  - {c['campaign_name']}: CTR {c['ctr']:.2f}% | CPM Rp {c['cpm']:,.0f} | Spend Rp {c['spend']:,.0f}" for c in tof_camps]) or '  None'}

BOF Campaigns:
{chr(10).join([f"  - {c['campaign_name']}: CTR {c['ctr']:.2f}% | CPC Rp {c['cpc']:,.0f} | Spend Rp {c['spend']:,.0f}" for c in bof_camps]) or '  None'}

Context: For property lead gen, TOF should build awareness (lower CTR ok),
BOF should drive leads (higher CTR expected, >2%).

Focus on: budget allocation efficiency, which campaigns to scale/cut,
TOF vs BOF balance for property marketing."""

        return self._call_gpt(prompt, cache_key)

    def generate_context_graph(self) -> dict:
        s     = self._summarize()
        df    = self.df
        cache_key = _make_cache_key(s, 'rev_context_graph')

        daily = df.groupby('date').agg(
            spend=('spend','sum'),
            clicks=('clicks','sum'),
            impressions=('impressions','sum'),
        ).reset_index()
        daily['ctr'] = daily['clicks'] / daily['impressions'].clip(lower=1) * 100
        daily['cpc'] = daily['spend']  / daily['clicks'].clip(lower=1)

        trend_lines = '\n'.join([
            f"  {str(r['date'])[:10]}: Spend Rp {r['spend']:,.0f} | "
            f"CTR {r['ctr']:.2f}% | CPC Rp {r['cpc']:,.0f}"
            for _, r in daily.iterrows()
        ])

        prompt = f"""Analyze the daily spend and CTR trend:

Period: {s['period']}
Daily data:
{trend_lines}

Overall avg CTR: {s['avg_ctr']:.2f}%
Best day: {s['best_day_date']} ({s['best_day_ctr']:.2f}% CTR)
Worst day: {s['worst_day_ctr']:.2f}% CTR

Focus on: spend consistency, CTR volatility, 
any concerning trends (dropping CTR with increasing spend = waste),
best days to run ads for property audience."""

        return self._call_gpt(prompt, cache_key)

    def generate_all(self) -> dict:
        return {
            'north_star'       : self.generate_north_star(),
            'campaign_terminal': self.generate_campaign_terminal(),
            'context_graph'    : self.generate_context_graph(),
        }
