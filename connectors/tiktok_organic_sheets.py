"""
connectors/tiktok_organic_sheets.py
=====================================
Fetch TikTok organic data dari Google Sheets.
Data direkap manual karena TikTok API belum tersedia.

Format kolom mengikuti organic_data.csv:
    date, platform, portfolio, followers, follower_growth,
    impressions, profile_visits, link_clicks, views,
    likes, comments, shares, saves,
    posts_published, posts_goal_weekly

Usage:
    from connectors.tiktok_organic_sheets import TikTokOrganicConnector

    connector = TikTokOrganicConnector()
    df = connector.fetch()
"""

import pandas as pd
import streamlit as st
from pathlib import Path


# ── Constants — UPDATE INI SETELAH DAPAT LINK ─────────────────────
SHEET_URL  = "https://docs.google.com/spreadsheets/d/1M1INAc6hc4LpRgVQanh10Hoq6QckKb2-bWrl13PfWz0"
SHEET_NAME = "Organic Titkok"
PLATFORM   = "TikTok"

# Kolom wajib sesuai organic_data.csv
REQUIRED_COLS = [
    'date', 'platform', 'portfolio', 'followers', 'follower_growth',
    'impressions', 'profile_visits', 'link_clicks', 'views',
    'likes', 'comments', 'shares', 'saves',
    'posts_published', 'posts_goal_weekly',
]

COL_DEFAULTS = {
    'followers'        : 0,
    'follower_growth'  : 0,
    'impressions'      : 0,
    'profile_visits'   : 0,
    'link_clicks'      : 0,
    'views'            : 0,
    'likes'            : 0,
    'comments'         : 0,
    'shares'           : 0,
    'saves'            : 0,
    'posts_published'  : 0,
    'posts_goal_weekly': 4,
}

NUMERIC_COLS = [
    'followers', 'follower_growth', 'impressions', 'profile_visits',
    'link_clicks', 'views', 'likes', 'comments', 'shares', 'saves',
    'posts_published', 'posts_goal_weekly',
]


def _get_gspread_client():
    """Buat gspread client — support service account lokal & Streamlit Cloud."""
    import gspread
    from google.oauth2.service_account import Credentials

    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly',
    ]

    # 1. Streamlit secrets (Cloud)
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception:
        pass

    # 2. File lokal
    candidates = [
        'credentials/key.json',
        'credentials/google_credentials.json',
        'key.json',
    ]
    for path in candidates:
        if Path(path).exists():
            creds = Credentials.from_service_account_file(path, scopes=SCOPES)
            return gspread.authorize(creds)

    raise FileNotFoundError(
        "Google credentials tidak ditemukan. "
        "Pastikan file ada di credentials/key.json"
    )


class TikTokOrganicConnector:
    """Fetch TikTok organic data dari Google Sheets."""

    def __init__(self, sheet_url: str = SHEET_URL, sheet_name: str = SHEET_NAME):
        self.sheet_url  = sheet_url
        self.sheet_name = sheet_name

    def fetch(self) -> pd.DataFrame:
        """
        Fetch dan normalisasi data dari Google Sheets.
        Return: DataFrame format organic_data siap di-concat.
        """
        try:
            client    = _get_gspread_client()
            sheet     = client.open_by_url(self.sheet_url)
            worksheet = sheet.worksheet(self.sheet_name)
            records   = worksheet.get_all_records()

            if not records:
                return pd.DataFrame()

            df = pd.DataFrame(records)
            return self._normalize(df)

        except Exception as e:
            st.warning(f"TikTok Organic Sheets error: {type(e).__name__}: {e}")
            return pd.DataFrame()

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalisasi ke format organic_data.csv."""
        if df.empty:
            return df

        # Lowercase & strip kolom
        df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]

        # Pastikan platform = TikTok
        df['platform'] = PLATFORM

        # Isi kolom yang tidak ada
        for col, default in COL_DEFAULTS.items():
            if col not in df.columns:
                df[col] = default

        # Convert date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])

        if df.empty:
            st.warning("TikTok Organic: Semua baris memiliki date tidak valid.")
            return pd.DataFrame()

        # Convert numeric — handle format angka dengan koma
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(',', '', regex=False)
                    .str.strip()
                )
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Pastikan portfolio ada
        if 'portfolio' not in df.columns or df['portfolio'].isna().all():
            df['portfolio'] = 'Unknown'

        # Return hanya kolom yang diperlukan
        for col in REQUIRED_COLS:
            if col not in df.columns:
                df[col] = COL_DEFAULTS.get(col, 0)

        return df[REQUIRED_COLS].reset_index(drop=True)
