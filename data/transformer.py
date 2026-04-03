"""
data/transformer.py
===================
Transform raw data hasil fetch dari meta_api.py
menjadi format yang siap dipakai oleh Streamlit dashboard.

Responsibilities:
- Rename kolom agar match dengan dashboard
- Hitung derived metrics (virality_score, conversion_score, dll)
- Gabungkan data multi-portfolio
- Validasi & cleaning data

Usage:
    from data.transformer import DataTransformer

    transformer = DataTransformer()

    # Transform single portfolio
    organic_df = transformer.transform_organic(ig_df, fb_df)
    content_df = transformer.transform_content_library(ig_posts_df, fb_posts_df)

    # Transform semua sekaligus dari hasil fetch_all()
    ready_data = transformer.transform_all(raw_data)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

# ══════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'processed')

# Kolom final untuk organic_data.csv (dipakai di organic_architecture.py)
ORGANIC_FINAL_COLS = [
    'date', 'platform', 'portfolio',
    'followers', 'follower_growth',
    'impressions', 'profile_visits', 'link_clicks',
    'views', 'likes', 'comments', 'shares', 'saves',
    'posts_published', 'posts_goal_weekly',
]

# Kolom final untuk content_library.csv (dipakai di organic_architecture.py)
CONTENT_FINAL_COLS = [
    'date', 'platform', 'portfolio',
    'title', 'content_type',
    'views', 'likes', 'comments', 'shares', 'saves', 'link_clicks',
    'virality_score', 'conversion_score',
    # FB Video extra metrics
    'reach', 'total_plays', 'avg_watch_sec', 'video_id',
    'permalink', 'thumbnail',
]




# ══════════════════════════════════════════════════════════════════
# CLASS: DataTransformer
# ══════════════════════════════════════════════════════════════════

class DataTransformer:
    """
    Transform raw fetch results menjadi format siap pakai dashboard.
    """

    def __init__(self, output_dir: str = OUTPUT_DIR):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: transform_organic()
    # Input : ig_organic_df + fb_organic_df (dari IGConnector & FBConnector)
    # Output: organic_data.csv
    # ──────────────────────────────────────────────────────────────

    def transform_organic(self,
                          ig_df: pd.DataFrame,
                          fb_df: Optional[pd.DataFrame] = None,
                          save: bool = True) -> pd.DataFrame:
        """
        Gabungkan IG + FB organic data dan pastikan
        semua kolom match dengan dashboard.

        Args:
            ig_df : DataFrame dari IGConnector.fetch_organic()
            fb_df : DataFrame dari FBConnector.fetch_organic() (opsional)
            save  : Simpan ke data/processed/organic_data.csv

        Return: DataFrame siap pakai
        """
        dfs = []

        if ig_df is not None and not ig_df.empty:
            dfs.append(self._clean_organic(ig_df))

        if fb_df is not None and not fb_df.empty:
            dfs.append(self._clean_organic(fb_df))

        if not dfs:
            return pd.DataFrame(columns=ORGANIC_FINAL_COLS)

        df = pd.concat(dfs, ignore_index=True)
        df = self._finalize_organic(df)

        if save:
            path = os.path.join(self.output_dir, 'organic_data.csv')
            df.to_csv(path, index=False)
            print(f'  💾 Saved: {path} ({len(df)} rows)')

        return df

    def _clean_organic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean & standardize 1 organic DataFrame."""
        df = df.copy()

        # Pastikan kolom date dalam format datetime
        df['date'] = pd.to_datetime(df['date'])

        # Fill missing columns dengan 0
        required_numeric = [
            'followers', 'follower_growth', 'impressions',
            'profile_visits', 'link_clicks', 'views',
            'likes', 'comments', 'shares', 'saves',
            'posts_published', 'posts_goal_weekly',
        ]
        for col in required_numeric:
            if col not in df.columns:
                df[col] = 0
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Pastikan kolom string ada
        if 'portfolio' not in df.columns:
            df['portfolio'] = 'Unknown'
        if 'platform' not in df.columns:
            df['platform'] = 'Unknown'

        return df

    def _finalize_organic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final cleanup & sorting organic DataFrame."""
        df = df.sort_values(['date', 'platform', 'portfolio']).reset_index(drop=True)

        # Pastikan semua kolom final ada
        for col in ORGANIC_FINAL_COLS:
            if col not in df.columns:
                df[col] = 0

        return df[ORGANIC_FINAL_COLS]

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: transform_content_library()
    # Input : ig_posts_df + fb_posts_df
    # Output: content_library.csv
    # ──────────────────────────────────────────────────────────────

    def transform_content_library(self,
                                   ig_posts_df: pd.DataFrame,
                                   fb_posts_df: Optional[pd.DataFrame] = None,
                                   save: bool = True) -> pd.DataFrame:
        """
        Transform posts data menjadi content_library format.

        Perubahan utama:
        - `caption`    → `title`
        - `media_type` → `content_type`
        - Tambah `link_clicks` (set 0, tidak tersedia per post)
        - Hitung `virality_score`  = (shares + saves) / max(views, 1) * 100
        - Hitung `conversion_score`= link_clicks / max(views, 1) * 100

        Args:
            ig_posts_df : DataFrame dari IGConnector.fetch_posts()
            fb_posts_df : DataFrame dari FBConnector.fetch_posts() (opsional)
            save        : Simpan ke data/processed/content_library.csv

        Return: DataFrame siap pakai
        """
        dfs = []

        if ig_posts_df is not None and not ig_posts_df.empty:
            dfs.append(self._clean_posts(ig_posts_df))

        if fb_posts_df is not None and not fb_posts_df.empty:
            dfs.append(self._clean_posts(fb_posts_df))

        if not dfs:
            return pd.DataFrame(columns=CONTENT_FINAL_COLS)

        df = pd.concat(dfs, ignore_index=True)
        df = self._compute_content_scores(df)
        df = self._finalize_content(df)

        if save:
            # Simpan dengan thumbnail untuk internal use
            path = os.path.join(self.output_dir, 'content_library.csv')
            df.to_csv(path, index=False)
            print(f'  💾 Saved: {path} ({len(df)} rows)')

        return df

    def _clean_posts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean & standardize 1 posts DataFrame."""
        df = df.copy()

        # ── Rename kolom ──────────────────────────────────────
        rename_map = {
            'caption'   : 'title',
            'media_type': 'content_type',
        }
        df = df.rename(columns=rename_map)

        # ── Date ──────────────────────────────────────────────
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        # ── Content type mapping ───────────────────────────────
        # Standardize content type labels
        type_map = {
            'IMAGE'          : 'Image',
            'VIDEO'          : 'Video',
            'CAROUSEL_ALBUM' : 'Carousel',
            'REELS'          : 'Reel',
            'CAROUSEL'       : 'Carousel',
            'LINK'           : 'Link',
            'STATUS'         : 'Status',
        }
        if 'content_type' in df.columns:
            df['content_type'] = df['content_type'].map(type_map).fillna(df['content_type'])

        # ── Numeric columns ────────────────────────────────────
        numeric_cols = ['views', 'likes', 'comments', 'shares', 'saves']
        for col in numeric_cols:
            if col not in df.columns:
                df[col] = 0
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # ── link_clicks per post (tidak tersedia via API) ──────
        # Set 0 — akan di-update jika ada data dari UTM tracking
        if 'link_clicks' not in df.columns:
            df['link_clicks'] = 0

        # ── String columns ─────────────────────────────────────
        if 'portfolio' not in df.columns:
            df['portfolio'] = 'Unknown'
        if 'platform' not in df.columns:
            df['platform'] = 'Unknown'
        if 'title' not in df.columns:
            df['title'] = 'Untitled Post'
        if 'content_type' not in df.columns:
            df['content_type'] = 'Image'
        if 'permalink' not in df.columns:
            df['permalink'] = ''
        if 'thumbnail' not in df.columns:
            df['thumbnail'] = ''

        # Clean title
        df['title'] = df['title'].fillna('').str.strip()
        df['title'] = df['title'].replace('', 'Untitled Post')

        return df

    def _compute_content_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Hitung virality_score dan conversion_score.

        virality_score  = (shares + saves) / max(views, 1) * 100
                          → Mengukur seberapa viral konten (disebarkan & disimpan)
                          → Benchmark: >3.0 = bagus, >1.5 = oke, <1.5 = perlu improvement

        conversion_score = link_clicks / max(views, 1) * 100
                          → Mengukur seberapa efektif konten drive traffic
                          → Benchmark: >3.0 = bagus, >1.5 = oke, <1.5 = perlu improvement
        """
        df = df.copy()

        # Virality Score
        df['virality_score'] = (
            (df['shares'] + df['saves']) /
            df['views'].clip(lower=1) * 100
        ).round(2)

        # Conversion Score
        df['conversion_score'] = (
            df['link_clicks'] /
            df['views'].clip(lower=1) * 100
        ).round(2)

        # Cap scores at 100
        df['virality_score']   = df['virality_score'].clip(upper=100)
        df['conversion_score'] = df['conversion_score'].clip(upper=100)

        return df

    def _finalize_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final cleanup & sorting content DataFrame."""
        df = df.sort_values('date', ascending=False).reset_index(drop=True)

        # Kolom yang nilainya string kosong jika tidak ada
        str_cols = [
            'date', 'platform', 'portfolio',
            'title', 'content_type',
            'permalink', 'thumbnail',
            'video_id',   # FB video ID — kosong untuk IG
        ]

        # Kolom yang nilainya 0 jika tidak ada
        num_cols = [
            'views', 'likes', 'comments', 'shares', 'saves', 'link_clicks',
            'virality_score', 'conversion_score',
            'reach', 'total_plays', 'avg_watch_sec',  # FB video extras — 0 untuk IG
        ]

        for col in CONTENT_FINAL_COLS:
            if col not in df.columns:
                df[col] = '' if col in str_cols else 0
            elif col in num_cols:
                # Fill NaN dari merge dengan 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            elif col in str_cols:
                # Fill NaN dari merge dengan string kosong
                df[col] = df[col].fillna('')

        return df[CONTENT_FINAL_COLS]



    # ──────────────────────────────────────────────────────────────
    # PUBLIC: transform_all()
    # Input : raw_data dict dari MetaConnector.fetch_all()
    # Output: semua CSV di data/processed/
    # ──────────────────────────────────────────────────────────────

    def transform_all(self,
                      raw_data: dict,
                      save: bool = True) -> dict:
        """
        Transform semua raw data sekaligus dari hasil fetch_all().

        Args:
            raw_data : dict dari MetaConnector.fetch_all() atau
                       fetch_all_portfolios() — format:
                       {
                         'campaigns'  : DataFrame,
                         'ads'        : DataFrame,
                         'ig_organic' : DataFrame,
                         'ig_posts'   : DataFrame,
                         'fb_organic' : DataFrame,
                         'fb_posts'   : DataFrame,
                       }
            save : Simpan semua ke data/processed/

        Return: dict {
            'organic'         : DataFrame (organic_data.csv),
            'content_library' : DataFrame (content_library.csv),
            'ads'             : DataFrame (revenue_data.csv),
        }
        """
        print('\n🔄 Transforming raw data...')

        results = {}

        # ── 1. Organic Data (IG + FB gabungan) ────────────────
        print('\n  📊 [1/2] Organic data...')
        ig_organic = raw_data.get('ig_organic', pd.DataFrame())
        fb_organic = raw_data.get('fb_organic', pd.DataFrame())
        results['organic'] = self.transform_organic(
            ig_df=ig_organic,
            fb_df=fb_organic,
            save=save,
        )
        print(f"  ✅ organic_data.csv — {len(results['organic'])} rows")

        # ── 2. Content Library (IG + FB posts gabungan) ───────
        print('\n  📚 [2/2] Content library...')
        ig_posts = raw_data.get('ig_posts', pd.DataFrame())
        fb_posts = raw_data.get('fb_posts', pd.DataFrame())
        results['content_library'] = self.transform_content_library(
            ig_posts_df=ig_posts,
            fb_posts_df=fb_posts,
            save=save,
        )
        print(f"  ✅ content_library.csv — {len(results['content_library'])} rows")

        print('\n✅ Transform complete!')
        return results

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: transform_all_portfolios()
    # Input : hasil fetch_all_portfolios() — multi portfolio
    # Output: unified CSV di data/processed/
    # ──────────────────────────────────────────────────────────────

    def transform_all_portfolios(self,
                                  all_raw_data: dict,
                                  save: bool = True) -> dict:
        """
        Transform data dari semua portfolio sekaligus.
        Gabungkan semua portfolio menjadi 1 unified DataFrame per data type.

        Args:
            all_raw_data : dict dari fetch_all_portfolios()
                           format: {1: {...}, 2: {...}}
            save         : Simpan ke data/processed/

        Return: dict {
            'organic'         : DataFrame,
            'content_library' : DataFrame,
            'ads'             : DataFrame,
        }
        """
        print('\n🔄 Transforming all portfolios...')

        all_organic  = []
        all_content  = []

        for portfolio_num, raw_data in all_raw_data.items():
            if raw_data is None:
                print(f'  ⚠️  Portfolio {portfolio_num}: No data, skipping')
                continue

            print(f'\n  🏢 Portfolio {portfolio_num}...')

            # Transform tanpa save dulu
            transformed = self.transform_all(raw_data, save=False)

            if not transformed['organic'].empty:
                all_organic.append(transformed['organic'])
            if not transformed['content_library'].empty:
                all_content.append(transformed['content_library'])

        # Gabungkan semua portfolio
        results = {}

        results['organic'] = pd.concat(all_organic, ignore_index=True) \
            if all_organic else pd.DataFrame(columns=ORGANIC_FINAL_COLS)

        results['content_library'] = pd.concat(all_content, ignore_index=True) \
            if all_content else pd.DataFrame(columns=CONTENT_FINAL_COLS)

        # Sort final DataFrames
        if not results['organic'].empty:
            results['organic'] = results['organic'].sort_values(
                ['date', 'platform', 'portfolio']
            ).reset_index(drop=True)

        if not results['content_library'].empty:
            results['content_library'] = results['content_library'].sort_values(
                'date', ascending=False
            ).reset_index(drop=True)

        # Save unified CSVs
        if save:
            print('\n  💾 Saving unified CSVs...')
            filename_map = {
                'organic'         : 'organic_data.csv',
                'content_library' : 'content_library.csv',
            }
            for name, df in results.items():
                path = os.path.join(self.output_dir, filename_map[name])
                df.to_csv(path, index=False)
                print(f'  ✅ {filename_map[name]} ({len(df)} rows)')

        print('\n✅ All portfolios transformed!')
        return results

    # ──────────────────────────────────────────────────────────────
    # PUBLIC: validate()
    # Validasi hasil transform sebelum dipakai dashboard
    # ──────────────────────────────────────────────────────────────

    def validate(self, results: dict) -> dict:
        """
        Validasi hasil transform — pastikan semua kolom ada
        dan tidak ada data yang kosong total.

        Return: dict {name: {valid, issues, shape}}
        """
        checks = {
            'organic'         : ORGANIC_FINAL_COLS,
            'content_library' : CONTENT_FINAL_COLS,
        }

        report = {}
        print('\n🔍 Validating transformed data...')

        for name, required_cols in checks.items():
            df     = results.get(name, pd.DataFrame())
            issues = []

            if df.empty:
                issues.append('DataFrame is empty')
            else:
                # Cek kolom missing
                missing = [c for c in required_cols if c not in df.columns]
                if missing:
                    issues.append(f'Missing columns: {missing}')

                # Cek null values di kolom penting
                key_cols = ['date', 'platform'] if 'platform' in df.columns else ['date']
                for col in key_cols:
                    if col in df.columns and df[col].isnull().any():
                        issues.append(f'Null values in {col}')

            is_valid = len(issues) == 0
            report[name] = {
                'valid'  : is_valid,
                'issues' : issues,
                'shape'  : df.shape,
            }

            status = '✅' if is_valid else '❌'
            print(f'  {status} {name}: {df.shape} — {"OK" if is_valid else issues}')

        return report
