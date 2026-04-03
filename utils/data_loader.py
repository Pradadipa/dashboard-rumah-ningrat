"""
utils/data_loader.py
====================
Centralized data loading untuk semua module dashboard.
Updated untuk support:
- Multi-portfolio (Portfolio 1 & 2)
- Real data dari meta_api.py + transformer.py
- Cache management
- Graceful fallback ke dummy data

Usage:
    from utils.data_loader import DataLoader

    loader = DataLoader(portfolio='all')        # semua portfolio
    loader = DataLoader(portfolio='Portfolio 1') # 1 portfolio saja

    organic_df = loader.load_organic_data()
    content_df = loader.load_content_library()
    ads_df     = loader.load_revenue_data()
"""

import os
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional


# ══════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════

DATA_DIR       = Path("data/processed")
RAW_DIR        = Path("data/raw")

# Mapping file → kolom date
DATE_COLS = {
    'organic_data.csv'       : 'date',
    'content_library.csv'    : 'date',
    'revenue_data.csv'       : 'date',
    'cohort_data.csv'        : 'acquisition_date',
    'funnel_data.csv'        : 'date',
    'funnel_module3_data.csv': 'date',
    'funnel_by_device.csv'   : 'date',
    'funnel_by_source.csv'   : 'date',
    'page_speed_data.csv'    : 'date',
    'traffic_data.csv'       : 'date',
    'revops_data.csv'        : 'date',
    'social_data.csv'        : 'date',
}


# ══════════════════════════════════════════════════════════════════
# CLASS: DataLoader
# ══════════════════════════════════════════════════════════════════

class DataLoader:
    """
    Centralized data loader dengan support:
    - Multi-portfolio filter
    - Cache management
    - Graceful error handling
    """

    def __init__(self, portfolio: str = 'all'):
        """
        Args:
            portfolio : 'all' untuk semua portfolio,
                        atau nama portfolio spesifik
                        contoh: 'Rumah Ningrat Subang'
        """
        self.data_dir  = DATA_DIR
        self.raw_dir   = RAW_DIR
        self.portfolio = portfolio

    # ──────────────────────────────────────────────────────────────
    # PRIVATE: _load()
    # Core loader dengan error handling & portfolio filter
    # ──────────────────────────────────────────────────────────────

    def _load(self, filename: str,
              date_col: Optional[str] = None,
              filter_portfolio: bool = True) -> pd.DataFrame:
        """
        Load CSV dari data/processed/ dengan error handling.

        Args:
            filename         : nama file CSV
            date_col         : kolom tanggal yang perlu diparse
            filter_portfolio : apply portfolio filter atau tidak

        Return: DataFrame atau DataFrame kosong jika error
        """
        path = self.data_dir / filename

        if not path.exists():
            return pd.DataFrame()

        try:
            df = pd.read_csv(path)

            # Parse date column
            col = date_col or DATE_COLS.get(filename)
            if col and col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

            # Apply portfolio filter
            if filter_portfolio and self.portfolio != 'all':
                if 'portfolio' in df.columns:
                    df = df[df['portfolio'] == self.portfolio].reset_index(drop=True)

            return df

        except Exception as e:
            st.warning(f"⚠️ Error loading {filename}: {e}")
            return pd.DataFrame()

    # ──────────────────────────────────────────────────────────────
    # PRIVATE: _filter_date()
    # Filter DataFrame berdasarkan date range
    # ──────────────────────────────────────────────────────────────

    def _filter_date(self, df: pd.DataFrame,
                     date_col: str = 'date',
                     days: Optional[int] = None,
                     since: Optional[str] = None,
                     until: Optional[str] = None) -> pd.DataFrame:
        """
        Filter DataFrame berdasarkan date range.

        Args:
            days  : N hari terakhir (opsional)
            since : 'YYYY-MM-DD' start date (opsional)
            until : 'YYYY-MM-DD' end date (opsional)
        """
        if df.empty or date_col not in df.columns:
            return df

        if days:
            cutoff = datetime.now() - timedelta(days=days)
            df = df[df[date_col] >= cutoff]
        if since:
            df = df[df[date_col] >= pd.to_datetime(since)]
        if until:
            df = df[df[date_col] <= pd.to_datetime(until)]

        return df.reset_index(drop=True)

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: Module 2 — Organic Architecture
    # ──────────────────────────────────────────────────────────────

    @st.cache_data(ttl=3600)  # cache 1 jam
    def load_organic_data(_self,
                          days: Optional[int] = None,
                          since: Optional[str] = None,
                          until: Optional[str] = None) -> pd.DataFrame:
        """
        Load organic_data.csv untuk Organic Architecture module.

        Kolom: date, platform, portfolio, followers, follower_growth,
               impressions, profile_visits, link_clicks, views,
               likes, comments, shares, saves,
               posts_published, posts_goal_weekly

        Args:
            days  : filter N hari terakhir
            since : filter start date 'YYYY-MM-DD'
            until : filter end date 'YYYY-MM-DD'
        """
        df = _self._load('organic_data.csv', date_col='date')

        if df.empty:
            return df

        return _self._filter_date(df, 'date', days, since, until)

    @st.cache_data(ttl=3600)
    def load_content_library(_self,
                             days: Optional[int] = None,
                             since: Optional[str] = None,
                             until: Optional[str] = None) -> pd.DataFrame:
        """
        Load content_library.csv untuk Content Library section.

        Kolom: date, platform, portfolio, title, content_type,
               views, likes, comments, shares, saves, link_clicks,
               virality_score, conversion_score, permalink, thumbnail
        """
        df = _self._load('content_library.csv', date_col='date')

        if df.empty:
            return df

        # Pastikan score columns numeric
        for col in ['virality_score', 'conversion_score']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return _self._filter_date(df, 'date', days, since, until)

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: Module 1 — Revenue Engineering
    # ──────────────────────────────────────────────────────────────

    @st.cache_data(ttl=3600)
    def load_revenue_data(_self,
                          days: Optional[int] = None,
                          since: Optional[str] = None,
                          until: Optional[str] = None) -> pd.DataFrame:
        """
        Load ads data untuk Revenue Engineering module.
        Prioritas: data/processed/revenue_data.csv
        Fallback : data/raw/all_meta_ads.csv
        """
        # Coba processed dulu
        df = _self._load('all_meta_ads.csv', date_col='date')

        # Fallback ke raw all_meta_ads.csv
        if df.empty:
            raw_path = _self.raw_dir / 'all_meta_ads.csv'
            if raw_path.exists():
                try:
                    df = pd.read_csv(raw_path)
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    if _self.portfolio != 'all' and 'portfolio' in df.columns:
                        df = df[df['portfolio'] == _self.portfolio].reset_index(drop=True)
                except Exception as e:
                    st.warning(f"Error loading all_meta_ads.csv: {e}")
                    return pd.DataFrame()

        if df.empty:
            return df

        # Pastikan numeric columns
        numeric_cols = ['spend', 'impressions', 'reach', 'clicks',
                        'cpm', 'cpc', 'ctr', 'purchases', 'revenue', 'roas', 'cpa']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return _self._filter_date(df, 'date', days, since, until)

    @st.cache_data(ttl=3600)
    def load_cohort_data(_self) -> pd.DataFrame:
        """Load cohort_data.csv untuk LTV Cohort Analysis."""
        return _self._load('cohort_data.csv',
                           date_col='acquisition_date',
                           filter_portfolio=False)

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: Module 3 — CRO Terminal
    # ──────────────────────────────────────────────────────────────

    @st.cache_data(ttl=3600)
    def load_funnel_data(_self) -> pd.DataFrame:
        """Load funnel_data.csv untuk CRO Terminal."""
        return _self._load('funnel_data.csv', date_col='date',
                           filter_portfolio=False)

    @st.cache_data(ttl=3600)
    def load_funnel_module3_data(_self) -> pd.DataFrame:
        """Load funnel_module3_data.csv untuk CRO Terminal."""
        return _self._load('funnel_module3_data.csv', date_col='date',
                           filter_portfolio=False)

    @st.cache_data(ttl=3600)
    def load_funnel_by_device(_self) -> pd.DataFrame:
        """Load funnel_by_device.csv untuk CRO Terminal."""
        return _self._load('funnel_by_device.csv', date_col='date',
                           filter_portfolio=False)

    @st.cache_data(ttl=3600)
    def load_funnel_by_source(_self) -> pd.DataFrame:
        """Load funnel_by_source.csv untuk CRO Terminal."""
        return _self._load('funnel_by_source.csv', date_col='date',
                           filter_portfolio=False)

    @st.cache_data(ttl=3600)
    def load_page_speed_data(_self) -> pd.DataFrame:
        """Load page_speed_data.csv untuk CRO Terminal."""
        return _self._load('page_speed_data.csv', date_col='date',
                           filter_portfolio=False)

    @st.cache_data(ttl=3600)
    def load_traffic_data(_self) -> pd.DataFrame:
        """Load traffic_data.csv untuk CRO Terminal."""
        return _self._load('traffic_data.csv', date_col='date',
                           filter_portfolio=False)

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: Module 4 — RevOps
    # ──────────────────────────────────────────────────────────────

    @st.cache_data(ttl=3600)
    def load_revops_data(_self) -> pd.DataFrame:
        """Load revops_data.csv untuk RevOps module."""
        return _self._load('revops_data.csv', date_col='date',
                           filter_portfolio=False)

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: get_portfolios()
    # Ambil list semua portfolio yang tersedia
    # ──────────────────────────────────────────────────────────────

    def get_portfolios(self) -> list:
        """
        Ambil list semua portfolio yang tersedia di data.
        Dipakai untuk dropdown/toggle di Streamlit sidebar.

        Return: list nama portfolio, contoh:
                ['Rumah Ningrat Subang', 'Rumah Ningrat Pejambon']
        """
        path = self.data_dir / 'organic_data.csv'
        if not path.exists():
            return []

        try:
            df = pd.read_csv(path, usecols=['portfolio'])
            return sorted(df['portfolio'].dropna().unique().tolist())
        except Exception:
            return []

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: refresh_cache()
    # Clear semua cache — dipanggil setelah fetch data baru
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def refresh_cache():
        """
        Clear semua Streamlit cache.
        Panggil ini setelah fetch & transform data baru
        supaya dashboard menampilkan data terbaru.

        Usage:
            if st.button('🔄 Refresh Data'):
                DataLoader.refresh_cache()
                st.rerun()
        """
        st.cache_data.clear()
        st.success('✅ Cache cleared — data refreshed!')

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: get_data_status()
    # Cek status semua file data
    # ──────────────────────────────────────────────────────────────

    def get_data_status(self) -> dict:
        """
        Cek status semua file data — tersedia atau tidak.
        Dipakai untuk debug dan status indicator di sidebar.

        Return: dict {filename: {exists, rows, last_modified}}
        """
        files = list(DATE_COLS.keys())
        status = {}

        for filename in files:
            path = self.data_dir / filename
            if path.exists():
                try:
                    df            = pd.read_csv(path, nrows=1)
                    full_df       = pd.read_csv(path)
                    last_modified = datetime.fromtimestamp(
                        path.stat().st_mtime
                    ).strftime('%Y-%m-%d %H:%M')
                    status[filename] = {
                        'exists'       : True,
                        'rows'         : len(full_df),
                        'last_modified': last_modified,
                    }
                except Exception as e:
                    status[filename] = {
                        'exists': True,
                        'rows'  : 0,
                        'error' : str(e),
                    }
            else:
                status[filename] = {
                    'exists': False,
                    'rows'  : 0,
                }

        return status

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: load_all()
    # Load semua data sekaligus — untuk preloading
    # ──────────────────────────────────────────────────────────────

    def load_all(self,
                 days: Optional[int] = 30,
                 since: Optional[str] = None,
                 until: Optional[str] = None) -> dict:
        """
        Load semua dataset sekaligus.

        Return: dict {
            'organic'         : DataFrame,
            'content_library' : DataFrame,
            'revenue'         : DataFrame,
            'cohort'          : DataFrame,
            'funnel'          : DataFrame,
            'revops'          : DataFrame,
        }
        """
        return {
            'organic'         : self.load_organic_data(days, since, until),
            'content_library' : self.load_content_library(days, since, until),
            'revenue'         : self.load_revenue_data(days, since, until),
            'cohort'          : self.load_cohort_data(),
            'funnel'          : self.load_funnel_data(),
            'revops'          : self.load_revops_data(),
        }
