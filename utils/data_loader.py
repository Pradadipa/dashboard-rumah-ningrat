"""
utils/data_loader.py
====================
Hybrid Data Loader — fetch langsung dari Meta API, cache di session.

Flow:
    User klik module
        ↓
    Cek st.cache_data (TTL 3 jam)
        ↓ cache hit → langsung render
        ↓ cache miss → fetch Meta API → transform → cache → render

Usage:
    from utils.data_loader import DataLoader

    loader     = DataLoader(portfolio='all', days=30)
    organic_df = loader.load_organic_data()
    content_df = loader.load_content_library()
    ads_df     = loader.load_revenue_data()
"""

import os
import sys
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

# Load .env untuk development lokal
# Di Streamlit Cloud, credentials dibaca dari st.secrets
load_dotenv()

CACHE_TTL = 3 * 3600  # 3 jam dalam detik


# ══════════════════════════════════════════════════════════════════
# HELPERS: Baca credentials
# ══════════════════════════════════════════════════════════════════

def _get_secret(key: str) -> str:
    """Baca secret — support Streamlit Cloud & .env lokal."""
    try:
        val = st.secrets.get(key, "")
        if val:
            return str(val)
    except Exception:
        pass
    return os.getenv(key, "")


def _get_portfolio_configs() -> list:
    """
    Baca semua portfolio config dari secrets/.env.
    Return: list of dict [{num, name, access_token, ...}]
    """
    configs = []
    for num in [1, 2]:
        name         = _get_secret(f'PORTFOLIO_{num}_NAME')
        access_token = _get_secret(f'PORTFOLIO_{num}_ACCESS_TOKEN')
        if name and access_token:
            configs.append({
                'num'           : num,
                'name'          : name,
                'app_id'        : _get_secret(f'PORTFOLIO_{num}_META_APP_ID'),
                'app_secret'    : _get_secret(f'PORTFOLIO_{num}_META_APP_SECRET'),
                'access_token'  : access_token,
                'ad_account_id' : _get_secret(f'PORTFOLIO_{num}_AD_ACCOUNT_ID'),
                'fb_page_id'    : _get_secret(f'PORTFOLIO_{num}_FB_PAGE_ID'),
                'ig_account_id' : _get_secret(f'PORTFOLIO_{num}_IG_ACCOUNT_ID'),
            })
    return configs


def _date_range(days: int) -> tuple:
    """Return (since, until) string untuk N hari terakhir."""
    until = datetime.now()
    since = until - timedelta(days=days)
    return since.strftime('%Y-%m-%d'), until.strftime('%Y-%m-%d')


# ══════════════════════════════════════════════════════════════════
# CORE: Fetch + Transform per portfolio
# ══════════════════════════════════════════════════════════════════

def _fetch_one_portfolio(config: dict, since: str, until: str,
                          posts_goal_weekly: int = 4) -> dict:
    """
    Fetch semua data dari 1 portfolio via Meta API.
    Return: dict {ig_organic, ig_posts, fb_organic, fb_posts, ads}
    """
    from connectors.meta_api import MetaConnector
    connector = MetaConnector(portfolio_num=config['num'])
    return connector.fetch_all(
        since             = since,
        until             = until,
        posts_goal_weekly = posts_goal_weekly,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _fetch_and_transform_all(
    portfolio_filter : str,
    since            : str,
    until            : str,
    posts_goal_weekly: int = 4,
) -> dict:
    """
    Fetch + transform semua portfolio, cache hasilnya.
    Dipanggil per module — cache shared antar module.

    Return: dict {
        'organic'         : DataFrame,
        'content_library' : DataFrame,
        'ads'             : DataFrame,
        'portfolios'      : list[str],
        'fetched_at'      : str,
    }
    """
    from data.transformer import DataTransformer

    configs = _get_portfolio_configs()
    if not configs:
        return {
            'organic'        : pd.DataFrame(),
            'content_library': pd.DataFrame(),
            'ads'            : pd.DataFrame(),
            'portfolios'     : [],
            'fetched_at'     : datetime.now().isoformat(),
        }

    # Filter portfolio jika bukan 'all'
    if portfolio_filter != 'all':
        configs = [c for c in configs if c['name'] == portfolio_filter]

    all_raw     = {}
    transformer = DataTransformer(output_dir=None)  # tidak simpan ke CSV

    for config in configs:
        try:
            raw = _fetch_one_portfolio(config, since, until, posts_goal_weekly)
            all_raw[config['num']] = raw
        except Exception as e:
            st.warning(f"Portfolio {config['name']} fetch error: {e}")
            all_raw[config['num']] = None

    # Transform semua portfolio jadi 1 unified DataFrame
    results = transformer.transform_all_portfolios(all_raw, save=False)

    return {
        'organic'        : results.get('organic',          pd.DataFrame()),
        'content_library': results.get('content_library',  pd.DataFrame()),
        'ads'            : _load_ads_fallback(since, until, portfolio_filter),
        'portfolios'     : [c['name'] for c in configs],
        'fetched_at'     : datetime.now().isoformat(),
    }


def _fetch_ads_from_api(since: str, until: str, portfolio_filter: str) -> pd.DataFrame:
    """
    Fetch ads data langsung dari Meta Ads API.
    Dipanggil saat CSV tidak ada atau sengaja direct fetch.
    """
    try:
        from connectors.meta_api import MetaConnector
        configs = _get_portfolio_configs()
        if portfolio_filter != 'all':
            configs = [c for c in configs if c['name'] == portfolio_filter]
        if not configs:
            return pd.DataFrame()

        dfs = []
        for config in configs:
            try:
                connector = MetaConnector(portfolio_num=config['num'])
                ads_df    = connector.ads.fetch_insights(since, until)
                if not ads_df.empty:
                    dfs.append(ads_df)
            except Exception as e:
                st.warning(f"Ads fetch error ({config['name']}): {e}")
                continue

        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        return df.sort_values(['date', 'portfolio']).reset_index(drop=True)

    except Exception as e:
        st.warning(f"Ads API error: {e}")
        return pd.DataFrame()


def _fetch_tiktok_content(since: str, until: str,
                           portfolio_filter: str) -> pd.DataFrame:
    """
    Fetch TikTok content dari Google Sheets.
    Filter by date range dan portfolio.
    Return empty DataFrame kalau gagal (non-blocking).
    """
    try:
        from connectors.tiktok_sheets import TikTokSheetsConnector
        connector = TikTokSheetsConnector()
        df        = connector.fetch()

        if df.empty:
            st.warning("TikTok: Data kosong dari Google Sheets.")
            return pd.DataFrame()

        # Filter date — pastikan semua dalam Timestamp
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])

            if df.empty:
                st.warning("TikTok: Semua baris memiliki date tidak valid.")
                return pd.DataFrame()

            since_dt  = pd.Timestamp(since)
            until_dt  = pd.Timestamp(until)
            df_filtered = df[(df['date'] >= since_dt) & (df['date'] <= until_dt)]

            if df_filtered.empty:
                st.info(
                    f"TikTok: {len(df)} baris ditemukan di sheet, tapi tidak ada "
                    f"yang masuk range {since} s/d {until}. "
                    f"Date di sheet: {df['date'].min().date()} s/d {df['date'].max().date()}"
                )
                return pd.DataFrame()

            df = df_filtered

        # Filter portfolio
        if portfolio_filter != 'all' and 'portfolio' in df.columns:
            df_port = df[df['portfolio'] == portfolio_filter]
            if df_port.empty:
                st.info(
                    f"TikTok: Tidak ada data untuk portfolio '{portfolio_filter}'. "
                    f"Portfolio di sheet: {df['portfolio'].unique().tolist()}"
                )
                return pd.DataFrame()
            df = df_port

        return df.reset_index(drop=True)

    except Exception as e:
        # Tampilkan error supaya bisa di-debug
        st.warning(f"TikTok fetch error: {type(e).__name__}: {e}")
        return pd.DataFrame()


def _fetch_tiktok_organic(since: str, until: str,
                           portfolio_filter: str) -> pd.DataFrame:
    """
    Fetch TikTok organic data dari Google Sheets.
    Non-blocking — return empty DataFrame kalau gagal.
    """
    try:
        from connectors.tiktok_organic_sheets import TikTokOrganicConnector
        connector = TikTokOrganicConnector()
        df        = connector.fetch()

        if df.empty:
            return pd.DataFrame()

        # Filter date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            since_dt = pd.Timestamp(since)
            until_dt = pd.Timestamp(until)
            df_filtered = df[(df['date'] >= since_dt) & (df['date'] <= until_dt)]

            if df_filtered.empty:
                st.info(
                    f"TikTok Organic: {len(df)} baris ditemukan tapi tidak ada "
                    f"yang masuk range {since} s/d {until}. "
                    f"Range di sheet: {df['date'].min().date()} s/d {df['date'].max().date()}"
                )
                return pd.DataFrame()
            df = df_filtered

        # Filter portfolio
        if portfolio_filter != 'all' and 'portfolio' in df.columns:
            df_port = df[df['portfolio'] == portfolio_filter]
            if df_port.empty:
                st.info(
                    f"TikTok Organic: Tidak ada data untuk portfolio '{portfolio_filter}'. "
                    f"Portfolio di sheet: {df['portfolio'].unique().tolist()}"
                )
                return pd.DataFrame()
            df = df_port

        return df.reset_index(drop=True)

    except Exception as e:
        st.warning(f"TikTok Organic fetch error: {type(e).__name__}: {e}")
        return pd.DataFrame()


def _load_ads_fallback(since: str, until: str, portfolio_filter: str) -> pd.DataFrame:
    """
    Load ads data — fetch dari Meta Ads API langsung.
    Fallback ke CSV lokal kalau API gagal.
    """
    # 1. Coba fetch dari API dulu
    df = _fetch_ads_from_api(since, until, portfolio_filter)
    if not df.empty:
        return df

    # 2. Fallback ke CSV kalau API gagal
    from pathlib import Path
    raw_paths = [
        Path('data/raw/all_meta_ads.csv'),
        Path('data/processed/revenue_data.csv'),
    ]
    for path in raw_paths:
        if path.exists():
            try:
                df = pd.read_csv(path)
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                if portfolio_filter != 'all' and 'portfolio' in df.columns:
                    df = df[df['portfolio'] == portfolio_filter]
                since_dt = pd.Timestamp(since)
                until_dt = pd.Timestamp(until)
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                df = df[(df['date'] >= since_dt) & (df['date'] <= until_dt)]
                return df.reset_index(drop=True)
            except Exception:
                continue

    return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════
# CLASS: DataLoader
# ══════════════════════════════════════════════════════════════════

class DataLoader:
    """
    Hybrid data loader:
    - Fetch dari Meta API langsung (tidak perlu CSV lokal)
    - Cache hasil di st.cache_data selama 3 jam
    - Fetch per module saat pertama kali dibuka
    - Loading spinner + progress text saat fetch
    """

    def __init__(self, portfolio: str = 'all', days: int = 30,
                 posts_goal_weekly: int = 4):
        """
        Args:
            portfolio         : 'all' atau nama portfolio spesifik
            days              : N hari terakhir yang di-fetch
            posts_goal_weekly : Target post per minggu
        """
        self.portfolio         = portfolio
        self.days              = days
        self.posts_goal_weekly = posts_goal_weekly
        self.since, self.until = _date_range(days)

    def _get_data(self) -> dict:
        """
        Ambil data — dari cache kalau ada, fetch kalau tidak.
        Tampilkan spinner saat fetch berlangsung.
        """
        # Key cache unik per kombinasi portfolio + date range
        cache_key = f"data_{self.portfolio}_{self.since}_{self.until}"

        # Cek session_state cache dulu (lebih cepat dari st.cache_data check)
        if cache_key in st.session_state:
            cached = st.session_state[cache_key]
            fetched_at = datetime.fromisoformat(cached.get('fetched_at', '2000-01-01'))
            age_hours  = (datetime.now() - fetched_at).total_seconds() / 3600
            if age_hours < 3:
                return cached

        # Cache miss — fetch dari API dengan progress indicator
        with st.spinner(''):
            # Progress container
            progress_placeholder = st.empty()

            def update_progress(msg: str):
                progress_placeholder.markdown(
                    f'<div style="padding:12px 16px;background:rgba(0,212,255,0.05);'
                    f'border:1px solid rgba(0,212,255,0.2);border-radius:8px;'
                    f'font-size:13px;color:#8892A0;">'
                    f'<span style="color:#00D4FF;">&#9650;</span> {msg}</div>',
                    unsafe_allow_html=True
                )

            update_progress('Connecting to Meta API...')

            try:
                configs = _get_portfolio_configs()
                if not configs:
                    progress_placeholder.empty()
                    return self._empty_result()

                port_filter = self.portfolio
                if port_filter != 'all':
                    configs = [c for c in configs if c['name'] == port_filter]

                from data.transformer import DataTransformer
                transformer = DataTransformer(output_dir=None)
                all_raw     = {}

                for i, config in enumerate(configs):
                    update_progress(
                        f'Fetching {config["name"]} '
                        f'({i+1}/{len(configs)}) — Instagram organic...'
                    )
                    try:
                        raw = _fetch_one_portfolio(
                            config, self.since, self.until, self.posts_goal_weekly
                        )
                        all_raw[config['num']] = raw
                    except Exception as e:
                        st.warning(f"Portfolio {config['name']}: {e}")
                        all_raw[config['num']] = None

                update_progress('Transforming data...')
                results = transformer.transform_all_portfolios(all_raw, save=False)

                update_progress('Fetching TikTok organic from Google Sheets...')
                tiktok_organic = _fetch_tiktok_organic(
                    self.since, self.until, self.portfolio
                )
                meta_organic   = results.get('organic', pd.DataFrame())
                if not tiktok_organic.empty:
                    if not meta_organic.empty and 'date' in meta_organic.columns:
                        meta_organic['date'] = pd.to_datetime(meta_organic['date'], errors='coerce')
                    tiktok_organic['date'] = pd.to_datetime(tiktok_organic['date'], errors='coerce')
                    merged_organic = pd.concat(
                        [meta_organic, tiktok_organic], ignore_index=True
                    ).sort_values(['date', 'platform']).reset_index(drop=True)
                    results['organic'] = merged_organic

                update_progress('Fetching Meta Ads insights...')
                ads_df = _fetch_ads_from_api(self.since, self.until, self.portfolio)
                if ads_df.empty:
                    update_progress('Loading ads data from local cache...')
                    ads_df = _load_ads_fallback(self.since, self.until, self.portfolio)

                update_progress('Fetching TikTok data from Google Sheets...')
                tiktok_df      = _fetch_tiktok_content(
                    self.since, self.until, self.portfolio
                )
                meta_content   = results.get('content_library', pd.DataFrame())
                if not tiktok_df.empty:
                    # Pastikan date sudah datetime sebelum concat & sort
                    for _df in [meta_content, tiktok_df]:
                        if not _df.empty and 'date' in _df.columns:
                            _df['date'] = pd.to_datetime(_df['date'], errors='coerce')
                    content_library = pd.concat(
                        [meta_content, tiktok_df], ignore_index=True
                    ).sort_values('date', ascending=False).reset_index(drop=True)
                else:
                    content_library = meta_content
                    if not content_library.empty and 'date' in content_library.columns:
                        content_library['date'] = pd.to_datetime(
                            content_library['date'], errors='coerce'
                        )

                data = {
                    'organic'        : results.get('organic',  pd.DataFrame()),
                    'content_library': content_library,
                    'ads'            : ads_df,
                    'portfolios'     : [c['name'] for c in configs],
                    'fetched_at'     : datetime.now().isoformat(),
                }

                # Simpan ke session_state
                st.session_state[cache_key] = data
                progress_placeholder.empty()
                return data

            except Exception as e:
                progress_placeholder.empty()
                st.error(f'Failed to fetch data: {e}')
                return self._empty_result()

    def _empty_result(self) -> dict:
        return {
            'organic'        : pd.DataFrame(),
            'content_library': pd.DataFrame(),
            'ads'            : pd.DataFrame(),
            'portfolios'     : [],
            'fetched_at'     : datetime.now().isoformat(),
        }

    def _filter_portfolio(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter DataFrame berdasarkan portfolio yang dipilih."""
        if df.empty or self.portfolio == 'all':
            return df
        if 'portfolio' in df.columns:
            return df[df['portfolio'] == self.portfolio].reset_index(drop=True)
        return df

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: load methods
    # ──────────────────────────────────────────────────────────────

    def load_organic_data(self, days: Optional[int] = None) -> pd.DataFrame:
        """Load organic data — fetch dari API kalau belum di-cache."""
        data = self._get_data()
        df   = self._filter_portfolio(data.get('organic', pd.DataFrame()))
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            if days:
                cutoff = pd.Timestamp(datetime.now() - timedelta(days=days))
                df = df[df['date'] >= cutoff]
        return df.sort_values('date').reset_index(drop=True) if not df.empty else df

    def load_content_library(self, days: Optional[int] = None) -> pd.DataFrame:
        """Load content library — fetch dari API kalau belum di-cache."""
        data = self._get_data()
        df   = self._filter_portfolio(data.get('content_library', pd.DataFrame()))
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            if days:
                cutoff = pd.Timestamp(datetime.now() - timedelta(days=days))
                df = df[df['date'] >= cutoff]
            for col in ['virality_score', 'conversion_score']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df.sort_values('date', ascending=False).reset_index(drop=True) if not df.empty else df

    def load_revenue_data(self, days: Optional[int] = None) -> pd.DataFrame:
        """Load ads data — CSV kalau ada, Meta API kalau tidak."""
        data = self._get_data()
        df   = self._filter_portfolio(data.get('ads', pd.DataFrame()))
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            if days:
                cutoff = pd.Timestamp(datetime.now() - timedelta(days=days))
                df = df[df['date'] >= cutoff]
            for col in ['spend', 'impressions', 'clicks', 'cpm', 'cpc', 'ctr']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df.sort_values('date').reset_index(drop=True) if not df.empty else df

    def get_portfolios(self) -> list:
        """Ambil list semua portfolio yang tersedia."""
        configs = _get_portfolio_configs()
        return [c['name'] for c in configs]

    @staticmethod
    def refresh_cache():
        """Clear semua cache — paksa fetch ulang dari API."""
        # Clear session_state cache
        keys_to_delete = [k for k in st.session_state if k.startswith('data_')]
        for k in keys_to_delete:
            del st.session_state[k]
        # Clear st.cache_data
        st.cache_data.clear()
