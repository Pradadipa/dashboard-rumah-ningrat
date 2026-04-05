"""
connectors/tiktok_sheets.py
============================
Fetch TikTok content library data dari Google Sheets.
Data direkap manual karena TikTok API belum tersedia.

Format kolom mengikuti content_library.csv:
    date, platform, portfolio, title, content_type,
    views, likes, comments, shares, saves, link_clicks,
    virality_score, conversion_score, reach,
    total_plays, avg_watch_sec, video_id, permalink, thumbnail

Usage:
    from connectors.tiktok_sheets import TikTokSheetsConnector

    connector = TikTokSheetsConnector()
    df = connector.fetch()
"""

import os
import json
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime


# ── Constants ─────────────────────────────────────────────────────
SHEET_URL      = "https://docs.google.com/spreadsheets/d/10ePc5nCeZDZfQwGzwqQaqfOvxudFW3lfsDwpu3e-pUM"
SHEET_NAME     = "Content Tiktok"
PLATFORM_NAME  = "TikTok"

# Kolom wajib ada di content_library
REQUIRED_COLS = [
    'date', 'platform', 'portfolio', 'title', 'content_type',
    'views', 'likes', 'comments', 'shares', 'saves', 'link_clicks',
    'virality_score', 'conversion_score', 'reach',
    'total_plays', 'avg_watch_sec', 'video_id', 'permalink', 'thumbnail',
]

# Default value kalau kolom tidak ada di sheet
COL_DEFAULTS = {
    'saves'           : 0,
    'link_clicks'     : 0,
    'reach'           : 0,
    'total_plays'     : 0,
    'avg_watch_sec'   : 0.0,
    'video_id'        : '',
    'permalink'       : '',
    'thumbnail'       : '',
    'virality_score'  : 0.0,
    'conversion_score': 0.0,
}


def _get_credentials_path() -> str:
    """Cari credentials file di beberapa lokasi."""
    candidates = [
        'credentials/key.json',
        'credentials/google_credentials.json',
        'credentials/service_account.json',
        'key.json',
    ]
    for path in candidates:
        if Path(path).exists():
            return path
    return None


def _get_gspread_client():
    """
    Buat gspread client — support service account (lokal & Cloud).
    Di Streamlit Cloud, credentials bisa disimpan di st.secrets.
    """
    import gspread
    from google.oauth2.service_account import Credentials

    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly',
    ]

    # 1. Coba dari Streamlit secrets (untuk Streamlit Cloud)
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception:
        pass

    # 2. Coba dari file credentials lokal
    creds_path = _get_credentials_path()
    if creds_path:
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        return gspread.authorize(creds)

    raise FileNotFoundError(
        "Google credentials tidak ditemukan. "
        "Pastikan file ada di credentials/key.json "
        "atau tambahkan gcp_service_account ke Streamlit secrets."
    )


class TikTokSheetsConnector:
    """
    Fetch TikTok content data dari Google Sheets.
    Merge dengan Meta content_library untuk unified dashboard.
    """

    def __init__(self, sheet_url: str = SHEET_URL, sheet_name: str = SHEET_NAME):
        self.sheet_url  = sheet_url
        self.sheet_name = sheet_name

    def fetch(self) -> pd.DataFrame:
        """
        Fetch data dari Google Sheets dan normalkan ke format content_library.
        Return: DataFrame siap di-concat dengan Meta content_library.
        """
        try:
            client    = _get_gspread_client()
            sheet     = client.open_by_url(self.sheet_url)
            worksheet = sheet.worksheet(self.sheet_name)
            records   = worksheet.get_all_records()

            if not records:
                return pd.DataFrame()

            df = pd.DataFrame(records)
            df = self._normalize(df)
            return df

        except Exception as e:
            st.warning(f"TikTok Sheets fetch error: {e}")
            return pd.DataFrame()

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalisasi DataFrame dari Google Sheets ke format content_library.
        Handle: kolom tidak lengkap, tipe data, nilai kosong.
        """
        if df.empty:
            return df

        # Lowercase semua nama kolom & strip whitespace
        df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]

        # Pastikan platform = TikTok
        df['platform'] = PLATFORM_NAME

        # Isi kolom yang tidak ada dengan default
        for col, default in COL_DEFAULTS.items():
            if col not in df.columns:
                df[col] = default

        # Convert date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])

        # Convert numeric columns
        numeric_cols = [
            'views', 'likes', 'comments', 'shares', 'saves',
            'link_clicks', 'reach', 'total_plays', 'avg_watch_sec',
        ]
        for col in numeric_cols:
            if col in df.columns:
                # Handle format angka dengan koma (misal: "1,234")
                df[col] = df[col].astype(str).str.replace(',', '').str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Hitung virality_score & conversion_score kalau belum ada / semua 0
        if df['virality_score'].sum() == 0:
            df['virality_score'] = (
                (df['shares'] + df['saves']) / df['views'].clip(lower=1) * 100
            ).round(2)

        if df['conversion_score'].sum() == 0:
            df['conversion_score'] = (
                df['link_clicks'] / df['views'].clip(lower=1) * 100
            ).round(2)

        # content_type default ke 'Video' kalau tidak ada
        if 'content_type' not in df.columns or df['content_type'].isna().all():
            df['content_type'] = 'Video'

        # Pastikan portfolio ada
        if 'portfolio' not in df.columns or df['portfolio'].isna().all():
            df['portfolio'] = 'Unknown'

        # Return hanya kolom yang diperlukan (yang ada)
        cols_available = [c for c in REQUIRED_COLS if c in df.columns]
        df = df[cols_available]

        # Tambah kolom yang masih kurang
        for col in REQUIRED_COLS:
            if col not in df.columns:
                df[col] = COL_DEFAULTS.get(col, '')

        return df[REQUIRED_COLS].reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════
# STANDALONE TEST
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    connector = TikTokSheetsConnector()
    df        = connector.fetch()
    print(f"Rows fetched: {len(df)}")
    if not df.empty:
        print(df.dtypes)
        print(df.head(3))
