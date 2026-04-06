"""
connectors/meta_api.py
======================
Meta API Connector untuk Marktivo Dashboard.
Handles: Meta Ads, Instagram Organic, Facebook Page — untuk 2 portfolio.

Usage:
    from connectors.meta_api import MetaConnector

    connector = MetaConnector(portfolio_num=1)
    df_ads    = connector.ads.fetch_insights('2026-03-01', '2026-04-01')
    df_ig     = connector.ig.fetch_organic('2026-03-01', '2026-04-01')
    df_fb     = connector.fb.fetch_organic('2026-03-01', '2026-04-01')

    # Atau fetch semua sekaligus:
    all_data  = connector.fetch_all('2026-03-01', '2026-04-01')
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign

load_dotenv()

GRAPH_API_VERSION = 'v25.0'


# ══════════════════════════════════════════════════════════════════
# CLASS 1: MetaAPIConfig
# Handle credentials loading & token verification
# ══════════════════════════════════════════════════════════════════

class MetaAPIConfig:
    """Load dan validasi credentials untuk 1 portfolio."""

    def __init__(self, portfolio_num: int):
        self.num = portfolio_num
        prefix   = f'PORTFOLIO_{portfolio_num}'

        self.name           = os.getenv(f'{prefix}_NAME', f'Portfolio {portfolio_num}')
        self.app_id         = os.getenv(f'{prefix}_META_APP_ID')
        self.app_secret     = os.getenv(f'{prefix}_META_APP_SECRET')
        self.access_token   = os.getenv(f'{prefix}_ACCESS_TOKEN')
        self.ad_account_id  = os.getenv(f'{prefix}_AD_ACCOUNT_ID')
        self.fb_page_id     = os.getenv(f'{prefix}_FB_PAGE_ID')
        self.ig_account_id  = os.getenv(f'{prefix}_IG_ACCOUNT_ID')

        self._validate()

    def _validate(self):
        """Cek semua credentials tersedia."""
        required = {
            'app_id'       : self.app_id,
            'app_secret'   : self.app_secret,
            'access_token' : self.access_token,
            'ad_account_id': self.ad_account_id,
            'fb_page_id'   : self.fb_page_id,
            'ig_account_id': self.ig_account_id,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(
                f"Portfolio {self.num} missing credentials: {missing}\n"
                f"Cek file .env kamu."
            )

    def verify_token(self) -> dict:
        """
        Verifikasi apakah access token masih valid.
        Return: dict {is_valid, expires_at, scopes, app_name}
        """
        url    = 'https://graph.facebook.com/debug_token'
        params = {
            'input_token' : self.access_token,
            'access_token': f'{self.app_id}|{self.app_secret}',
        }
        data       = requests.get(url, params=params).json()
        token_data = data.get('data', {})

        return {
            'portfolio'  : self.name,
            'is_valid'   : token_data.get('is_valid', False),
            'expires_at' : token_data.get('expires_at', 0),
            'scopes'     : token_data.get('scopes', []),
            'app_name'   : token_data.get('application', 'Unknown'),
        }

    def init_facebook_sdk(self):
        """Initialize Facebook Business SDK."""
        FacebookAdsApi.init(
            app_id       = self.app_id,
            app_secret   = self.app_secret,
            access_token = self.access_token,
            api_version  = GRAPH_API_VERSION,
        )

    def __repr__(self):
        return f"MetaAPIConfig(portfolio={self.num}, name='{self.name}')"


# ══════════════════════════════════════════════════════════════════
# CLASS 2: MetaAdsConnector
# Fetch paid advertising data dari Meta Ads
# ══════════════════════════════════════════════════════════════════

class MetaAdsConnector:
    """Fetch Meta Ads data: campaigns & daily insights."""

    def __init__(self, config: MetaAPIConfig):
        self.config = config
        self.config.init_facebook_sdk()

    def fetch_campaigns(self) -> pd.DataFrame:
        """
        Fetch semua campaign dari ad account.
        Return: DataFrame [portfolio, campaign_id, campaign_name,
                           status, objective, created_time,
                           daily_budget, lifetime_budget]
        """
        account   = AdAccount(self.config.ad_account_id)
        campaigns = account.get_campaigns(fields=[
            Campaign.Field.id,
            Campaign.Field.name,
            Campaign.Field.status,
            Campaign.Field.objective,
            Campaign.Field.created_time,
            Campaign.Field.daily_budget,
            Campaign.Field.lifetime_budget,
        ])

        rows = []
        for c in campaigns:
            rows.append({
                'portfolio'      : self.config.name,
                'campaign_id'    : c.get('id'),
                'campaign_name'  : c.get('name'),
                'status'         : c.get('status'),
                'objective'      : c.get('objective'),
                'created_time'   : c.get('created_time', '')[:10],
                'daily_budget'   : float(c.get('daily_budget', 0)),
                'lifetime_budget': float(c.get('lifetime_budget', 0)),
            })

        return pd.DataFrame(rows)

    def fetch_insights(self, since: str, until: str) -> pd.DataFrame:
        """
        Fetch daily campaign insights dalam date range.

        Menggunakan Outbound Clicks (bukan All Clicks):
        - clicks   = outbound_clicks (klik yang keluar ke landing page / website)
        - ctr      = outbound_clicks_ctr (CTR berbasis outbound clicks)
        - cpc      = spend / outbound_clicks (dihitung manual, lebih akurat)

        Perbedaan All Clicks vs Outbound Clicks:
        - All Clicks   : termasuk klik ke profil, like button, expand gambar, dll
        - Outbound     : HANYA klik yang benar-benar keluar ke luar Facebook/Instagram
                         → lebih relevan untuk lead gen / properti

        Args:
            since: 'YYYY-MM-DD'
            until: 'YYYY-MM-DD'

        Return: DataFrame [portfolio, date, campaign_name, campaign_id,
                           impressions, reach, clicks, spend, cpm, cpc,
                           ctr, purchases, revenue, roas, cpa]
        """
        account = AdAccount(self.config.ad_account_id)
        params  = {
            'time_range'    : {'since': since, 'until': until},
            'level'         : 'campaign',
            'time_increment': 1,  # daily
        }
        fields = [
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.date_start,
            AdsInsights.Field.date_stop,
            AdsInsights.Field.impressions,
            AdsInsights.Field.reach,
            AdsInsights.Field.outbound_clicks,          # Outbound Clicks (bukan all clicks)
            AdsInsights.Field.outbound_clicks_ctr,      # CTR berbasis outbound clicks
            AdsInsights.Field.spend,
            AdsInsights.Field.cpm,
            AdsInsights.Field.actions,
            AdsInsights.Field.action_values,
            AdsInsights.Field.purchase_roas,
        ]
        insights = account.get_insights(fields=fields, params=params)

        rows = []
        for row in insights:
            actions       = row.get('actions', [])
            action_values = row.get('action_values', [])
            roas_list     = row.get('purchase_roas', [])

            # ── Outbound Clicks ──────────────────────────────────────
            # outbound_clicks adalah list: [{'action_type': 'outbound_click', 'value': 'N'}]
            outbound_list = row.get('outbound_clicks', [])
            clicks        = int(float(outbound_list[0]['value'])) if outbound_list else 0

            # ── Outbound CTR ─────────────────────────────────────────
            # outbound_clicks_ctr adalah list: [{'action_type': 'outbound_click', 'value': 'N'}]
            ctr_list = row.get('outbound_clicks_ctr', [])
            ctr      = round(float(ctr_list[0]['value']), 4) if ctr_list else 0.0

            # ── CPC Manual (Spend / Outbound Clicks) ─────────────────
            # Tidak bisa pakai field cpc bawaan Meta karena itu berbasis all clicks
            spend = float(row.get('spend', 0))
            cpc   = round(spend / clicks, 2) if clicks > 0 else 0.0

            # ── Purchase metrics ─────────────────────────────────────
            purchases = next((float(a['value']) for a in actions       if a['action_type'] == 'purchase'), 0)
            revenue   = next((float(a['value']) for a in action_values if a['action_type'] == 'purchase'), 0)
            roas      = float(roas_list[0]['value']) if roas_list else 0

            rows.append({
                'portfolio'    : self.config.name,
                'date'         : row.get('date_start'),
                'campaign_name': row.get('campaign_name'),
                'campaign_id'  : row.get('campaign_id'),
                'impressions'  : int(row.get('impressions', 0)),
                'reach'        : int(row.get('reach', 0)),
                'clicks'       : clicks,          # Outbound Clicks
                'spend'        : round(spend, 2),
                'cpm'          : round(float(row.get('cpm', 0)), 2),
                'cpc'          : cpc,             # Spend / Outbound Clicks
                'ctr'          : ctr,             # Outbound CTR
                'purchases'    : int(purchases),
                'revenue'      : round(revenue, 2),
                'roas'         : round(roas, 2),
                'cpa'          : round(spend / purchases, 2) if purchases > 0 else 0,
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])

            # ── Merge campaign status dari fetch_campaigns() ──────
            try:
                campaigns_df = self.fetch_campaigns()
                if not campaigns_df.empty and 'campaign_id' in campaigns_df.columns:
                    # Normalize ke string supaya type mismatch tidak terjadi
                    campaigns_df['campaign_id'] = campaigns_df['campaign_id'].astype(str)
                    df['campaign_id']            = df['campaign_id'].astype(str)
                    status_map = campaigns_df.set_index('campaign_id')['status'].to_dict()
                    df['status'] = df['campaign_id'].map(status_map).fillna('UNKNOWN')
                else:
                    df['status'] = 'UNKNOWN'
            except Exception as e:
                df['status'] = 'UNKNOWN'

        return df


# ══════════════════════════════════════════════════════════════════
# CLASS 3: IGConnector
# Fetch Instagram organic data
# ══════════════════════════════════════════════════════════════════

class IGConnector:
    """Fetch Instagram organic data: account metrics + content metrics."""

    def __init__(self, config: MetaAPIConfig):
        self.config = config
        self.token  = config.access_token
        self.ig_id  = config.ig_account_id

    def _get(self, endpoint: str, params: dict) -> dict:
        """Helper: GET request ke Graph API."""
        url              = f'https://graph.facebook.com/{GRAPH_API_VERSION}/{endpoint}'
        params['access_token'] = self.token
        return requests.get(url, params=params).json()

    def _fetch_post_insights(self, media_id: str, media_type: str) -> dict:
        """Fetch insights untuk 1 post."""
        if media_type in ['VIDEO', 'REELS']:
            metrics = ['views', 'likes', 'comments', 'shares', 'saved']
        else:
            metrics = ['impressions', 'likes', 'comments', 'shares', 'saved']

        data   = self._get(f'{media_id}/insights', {'metric': ','.join(metrics)})
        result = {'views': 0, 'likes': 0, 'comments': 0, 'shares': 0, 'saves': 0}

        for m in data.get('data', []):
            val = m.get('values', [{'value': 0}])[0].get('value', 0)
            if isinstance(val, dict):
                val = sum(val.values())
            name = m['name']
            if name == 'views'        : result['views']    = val
            elif name == 'impressions': result['views']    = val
            elif name == 'likes'      : result['likes']    = val
            elif name == 'comments'   : result['comments'] = val
            elif name == 'shares'     : result['shares']   = val
            elif name == 'saved'      : result['saves']    = val
        return result

    def _fetch_account_metrics(self, since: str, until: str) -> dict:
        """Fetch account-level daily metrics."""
        since_dt    = datetime.strptime(since, '%Y-%m-%d')
        until_dt    = datetime.strptime(until, '%Y-%m-%d')
        all_data    = {}
        chunk_start = since_dt

        while chunk_start < until_dt:
            chunk_end = min(chunk_start + timedelta(days=30), until_dt)
            data = self._get(f'{self.ig_id}/insights', {
                'metric': 'impressions,profile_views,website_clicks,follower_count',
                'period': 'day',
                'since' : int(chunk_start.timestamp()),
                'until' : int(chunk_end.timestamp()),
            })
            if 'error' not in data:
                for m in data.get('data', []):
                    for entry in m.get('values', []):
                        d = entry['end_time'][:10]
                        if d not in all_data:
                            all_data[d] = {}
                        all_data[d][m['name']] = entry.get('value', 0)
            chunk_start = chunk_end

        return all_data

    def fetch_organic(self, since: str, until: str,
                      posts_goal_weekly: int = 4) -> pd.DataFrame:
        """
        Fetch daily organic metrics.
        Gabungan account-level + content-level per hari.

        Return: DataFrame [portfolio, date, platform, followers,
                           follower_growth, impressions, profile_visits,
                           link_clicks, views, likes, comments, shares,
                           saves, posts_published, posts_goal_weekly]
        """
        # Account-level
        account_data      = self._fetch_account_metrics(since, until)
        current_followers = self._get(self.ig_id, {'fields': 'followers_count'}).get('followers_count', 0)

        # Content-level
        media_data = self._get(f'{self.ig_id}/media', {
            'fields': 'id,media_type,media_url,thumbnail_url,timestamp,caption,permalink',
            'since' : since, 'until': until, 'limit': 100,
        })
        media_list = media_data.get('data', [])

        daily_content = {}
        for media in media_list:
            media_id   = media['id']
            media_type = media.get('media_type', 'IMAGE')
            date_str   = media['timestamp'][:10]
            ins        = self._fetch_post_insights(media_id, media_type)

            if date_str not in daily_content:
                daily_content[date_str] = {
                    'views': 0, 'likes': 0, 'comments': 0,
                    'shares': 0, 'saves': 0, 'posts_published': 0,
                }
            for k in ['views', 'likes', 'comments', 'shares', 'saves']:
                daily_content[date_str][k] += ins[k]
            daily_content[date_str]['posts_published'] += 1
            time.sleep(0.3)

        # Build date range
        since_dt    = datetime.strptime(since, '%Y-%m-%d')
        until_dt    = datetime.strptime(until, '%Y-%m-%d')
        date_range  = [(since_dt + timedelta(days=i)).strftime('%Y-%m-%d')
                       for i in range((until_dt - since_dt).days + 1)]

        # Reconstruct followers timeline
        deltas      = {d: account_data.get(d, {}).get('follower_count', 0) for d in date_range}
        running     = current_followers - sum(deltas.values())
        followers_tl = {}
        for d in date_range:
            running += deltas.get(d, 0)
            followers_tl[d] = running

        rows = []
        for date in date_range:
            acc  = account_data.get(date, {})
            cont = daily_content.get(date, {})
            rows.append({
                'portfolio'        : self.config.name,
                'date'             : date,
                'platform'         : 'Instagram',
                'followers'        : followers_tl.get(date, current_followers),
                'follower_growth'  : deltas.get(date, 0),
                'impressions'      : acc.get('impressions', 0),
                'profile_visits'   : acc.get('profile_views', 0),
                'link_clicks'      : acc.get('website_clicks', 0),
                'views'            : cont.get('views', 0),
                'likes'            : cont.get('likes', 0),
                'comments'         : cont.get('comments', 0),
                'shares'           : cont.get('shares', 0),
                'saves'            : cont.get('saves', 0),
                'posts_published'  : cont.get('posts_published', 0),
                'posts_goal_weekly': posts_goal_weekly,
            })

        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)

    def fetch_posts(self, since: str, until: str) -> pd.DataFrame:
        """
        Fetch semua post beserta insights per post.

        Return: DataFrame [portfolio, platform, date, media_type,
                           caption, views, likes, comments, shares,
                           saves, permalink, thumbnail]
        """
        media_data = self._get(f'{self.ig_id}/media', {
            'fields': 'id,media_type,media_url,thumbnail_url,timestamp,caption,permalink',
            'since' : since, 'until': until, 'limit': 100,
        })
        media_list = media_data.get('data', [])

        rows = []
        for media in media_list:
            media_id   = media['id']
            media_type = media.get('media_type', 'IMAGE')
            ins        = self._fetch_post_insights(media_id, media_type)
            thumbnail  = (media.get('thumbnail_url') or media.get('media_url', '')) \
                         if media_type == 'VIDEO' else media.get('media_url', '')

            rows.append({
                'portfolio' : self.config.name,
                'platform'  : 'Instagram',
                'date'      : media['timestamp'][:10],
                'media_type': media_type,
                'caption'   : media.get('caption', '')[:80].replace('\n', ' '),
                'views'     : ins['views'],
                'likes'     : ins['likes'],
                'comments'  : ins['comments'],
                'shares'    : ins['shares'],
                'saves'     : ins['saves'],
                'permalink' : media.get('permalink', ''),
                'thumbnail' : thumbnail,
            })
            time.sleep(0.3)

        df = pd.DataFrame(rows)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date', ascending=False).reset_index(drop=True)
        return df


# ══════════════════════════════════════════════════════════════════
# CLASS 4: FBConnector
# Fetch Facebook Page organic data
# ══════════════════════════════════════════════════════════════════

class FBConnector:
    """Fetch Facebook Page organic data."""

    def __init__(self, config: MetaAPIConfig):
        self.config     = config
        self.user_token = config.access_token
        self.page_id    = config.fb_page_id
        self.page_token = self._get_page_token()

    def _get(self, endpoint: str, params: dict, token: str = None) -> dict:
        """Helper: GET request ke Graph API."""
        url              = f'https://graph.facebook.com/{GRAPH_API_VERSION}/{endpoint}'
        params['access_token'] = token or self.page_token
        return requests.get(url, params=params).json()

    def _get_page_token(self) -> str:
        """Tukar user token dengan page access token."""
        data = self._get(self.page_id,
                         {'fields': 'access_token,name'},
                         token=self.user_token)
        return data.get('access_token', self.user_token)

    def _check_insights_available(self) -> bool:
        """Cek apakah Page Insights API sudah aktif."""
        data = self._get(f'{self.page_id}/insights', {
            'metric': 'page_impressions',
            'period': 'day',
        })
        return 'error' not in data

    def fetch_organic(self, since: str, until: str,
                      posts_goal_weekly: int = 4) -> pd.DataFrame:
        """
        Fetch daily organic metrics Facebook Page.
        Output kolom identik dengan IGConnector.fetch_organic().

        Return: DataFrame [portfolio, date, platform, followers,
                           follower_growth, impressions, profile_visits,
                           link_clicks, views, likes, comments, shares,
                           saves, posts_published, posts_goal_weekly]
        """
        insights_available = self._check_insights_available()

        # Current fans
        page_data    = self._get(self.page_id, {'fields': 'fan_count,name'})
        current_fans = page_data.get('fan_count', 0)

        # Posts per day
        posts_data = self._get(f'{self.page_id}/posts', {
            'fields': 'id,created_time',
            'since' : since, 'until': until, 'limit': 100,
        })
        posts_per_day = {}
        for post in posts_data.get('data', []):
            d = post['created_time'][:10]
            posts_per_day[d] = posts_per_day.get(d, 0) + 1

        # Insights (jika tersedia)
        insights_data = {}
        if insights_available:
            since_dt    = datetime.strptime(since, '%Y-%m-%d')
            until_dt    = datetime.strptime(until, '%Y-%m-%d')
            chunk_start = since_dt
            METRICS     = ['page_impressions', 'page_impressions_unique',
                           'page_post_engagements', 'page_fan_adds_unique',
                           'page_fan_removes_unique', 'page_views_total']

            while chunk_start < until_dt:
                chunk_end = min(chunk_start + timedelta(days=30), until_dt)
                data = self._get(f'{self.page_id}/insights', {
                    'metric': ','.join(METRICS),
                    'period': 'day',
                    'since' : int(chunk_start.timestamp()),
                    'until' : int(chunk_end.timestamp()),
                })
                if 'error' not in data:
                    for m in data.get('data', []):
                        for entry in m.get('values', []):
                            d = entry['end_time'][:10]
                            if d not in insights_data:
                                insights_data[d] = {}
                            insights_data[d][m['name']] = entry.get('value', 0)
                chunk_start = chunk_end

        # Build date range
        since_dt   = datetime.strptime(since, '%Y-%m-%d')
        until_dt   = datetime.strptime(until, '%Y-%m-%d')
        date_range = [(since_dt + timedelta(days=i)).strftime('%Y-%m-%d')
                      for i in range((until_dt - since_dt).days + 1)]

        # Followers timeline
        if insights_available:
            deltas = {
                d: (insights_data.get(d, {}).get('page_fan_adds_unique', 0) -
                    insights_data.get(d, {}).get('page_fan_removes_unique', 0))
                for d in date_range
            }
            running      = current_fans - sum(deltas.values())
            followers_tl = {}
            for d in date_range:
                running += deltas.get(d, 0)
                followers_tl[d] = running
        else:
            deltas       = {d: 0 for d in date_range}
            followers_tl = {d: current_fans for d in date_range}

        rows = []
        for date in date_range:
            ins = insights_data.get(date, {})
            rows.append({
                'portfolio'        : self.config.name,
                'date'             : date,
                'platform'         : 'Facebook',
                'followers'        : followers_tl.get(date, current_fans),
                'follower_growth'  : deltas.get(date, 0),
                'impressions'      : ins.get('page_impressions', 0),
                'profile_visits'   : ins.get('page_views_total', 0),
                'link_clicks'      : 0,
                'views'            : ins.get('page_impressions_unique', 0),
                'likes'            : ins.get('page_post_engagements', 0),
                'comments'         : 0,
                'shares'           : 0,
                'saves'            : 0,
                'posts_published'  : posts_per_day.get(date, 0),
                'posts_goal_weekly': posts_goal_weekly,
            })

        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)

    def fetch_posts(self, since: str, until: str) -> pd.DataFrame:
        """
        Fetch semua post FB Page beserta metrics.

        Return: DataFrame [portfolio, platform, date, media_type,
                           caption, views, likes, comments, shares,
                           saves, permalink, thumbnail]
        """
        data  = self._get(f'{self.page_id}/posts', {
            'fields': ','.join([
                'id', 'message', 'story', 'created_time',
                'full_picture', 'permalink_url', 'attachments',
                'reactions.summary(true)',
                'comments.summary(true)',
                'shares',
            ]),
            'since': since, 'until': until, 'limit': 100,
        })

        type_map = {
            'photo': 'IMAGE', 'video_inline': 'VIDEO',
            'video_autoplay': 'VIDEO', 'album': 'CAROUSEL',
            'share': 'LINK',
        }

        rows = []
        for post in data.get('data', []):
            atts       = post.get('attachments', {}).get('data', [])
            att        = atts[0] if atts else {}
            media_type = type_map.get(att.get('type', 'photo'), 'IMAGE')
            caption    = post.get('message', post.get('story', ''))[:80].replace('\n', ' ')

            # ── Ambil Video ID dari attachments target ────────────
            # Video ID berbeda dari Post ID di Facebook
            # Post ID format : pageID_postID
            # Video ID format: angka murni, ada di att['target']['id']
            target     = att.get('target', {})
            video_id   = target.get('id', '') if media_type == 'VIDEO' else ''

            # ── Fetch video insights pakai Video ID ───────────────
            views          = 0
            reach          = 0
            avg_watch_ms   = 0
            total_plays    = 0

            if video_id:
                try:
                    vid_data  = self._get(video_id, {
                        'fields': 'video_insights,length'
                    })
                    video_ins = vid_data.get('video_insights', {}).get('data', [])

                    for ins in video_ins:
                        name = ins.get('name', '')
                        val  = ins.get('values', [{}])[0].get('value', 0)

                        # Views = play unik tanpa replay (paling akurat)
                        if name == 'blue_reels_play_count':
                            views = int(val) if isinstance(val, (int, float)) else 0

                        # Reach = orang yang lihat
                        elif name == 'post_impressions_unique':
                            reach = int(val) if isinstance(val, (int, float)) else 0

                        # Total plays termasuk replay
                        elif name == 'fb_reels_total_plays':
                            total_plays = int(val) if isinstance(val, (int, float)) else 0

                        # Rata-rata waktu tonton (ms → detik)
                        elif name == 'post_video_avg_time_watched':
                            avg_watch_ms = int(val) if isinstance(val, (int, float)) else 0

                    time.sleep(0.2)
                except Exception:
                    pass

            rows.append({
                'portfolio'     : self.config.name,
                'platform'      : 'Facebook',
                'date'          : post.get('created_time', '')[:10],
                'media_type'    : media_type,
                'caption'       : caption,
                'views'         : views,           # blue_reels_play_count
                'reach'         : reach,           # post_impressions_unique
                'total_plays'   : total_plays,     # fb_reels_total_plays (incl. replay)
                'avg_watch_sec' : round(avg_watch_ms / 1000, 1),  # ms → detik
                'likes'         : post.get('reactions', {}).get('summary', {}).get('total_count', 0),
                'comments'      : post.get('comments', {}).get('summary', {}).get('total_count', 0),
                'shares'        : post.get('shares', {}).get('count', 0),
                'saves'         : 0,
                'permalink'     : post.get('permalink_url', ''),
                'thumbnail'     : post.get('full_picture', ''),
                'video_id'      : video_id,
            })
            time.sleep(0.2)

        df = pd.DataFrame(rows)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date', ascending=False).reset_index(drop=True)
        return df


# ══════════════════════════════════════════════════════════════════
# CLASS 5: MetaConnector (Master)
# Entry point utama — dipakai di app.py dan Streamlit
# ══════════════════════════════════════════════════════════════════

class MetaConnector:
    """
    Master connector untuk 1 portfolio.
    Gabungkan MetaAdsConnector, IGConnector, FBConnector.

    Usage:
        connector = MetaConnector(portfolio_num=1)
        data      = connector.fetch_all('2026-03-01', '2026-04-01')
    """

    def __init__(self, portfolio_num: int):
        self.config = MetaAPIConfig(portfolio_num)
        self.ads    = MetaAdsConnector(self.config)
        self.ig     = IGConnector(self.config)
        self.fb     = FBConnector(self.config)

    def verify(self) -> dict:
        """Verifikasi token sebelum fetch."""
        return self.config.verify_token()

    def fetch_all(self, since: str, until: str,
                  posts_goal_weekly: int = 4,
                  export_path: str = None) -> dict:
        """
        Fetch semua data untuk portfolio ini.

        Args:
            since             : 'YYYY-MM-DD'
            until             : 'YYYY-MM-DD'
            posts_goal_weekly : target post per minggu
            export_path       : folder export CSV (opsional)
                                contoh: 'data/raw/portfolio_1'

        Return: dict {
            'campaigns'  : DataFrame,
            'ads'        : DataFrame,
            'ig_organic' : DataFrame,
            'ig_posts'   : DataFrame,
            'fb_organic' : DataFrame,
            'fb_posts'   : DataFrame,
        }
        """
        print(f'\n{"="*55}')
        print(f'🏢 Fetching: {self.config.name}')
        print(f'   Range   : {since} → {until}')
        print(f'{"="*55}')

        result = {}

        # ── Meta Ads ──
        print('\n📊 [1/3] Meta Ads...')
        try:
            result['campaigns'] = self.ads.fetch_campaigns()
            result['ads']       = self.ads.fetch_insights(since, until)
            print(f"  ✅ {len(result['campaigns'])} campaigns | {len(result['ads'])} daily rows")
        except Exception as e:
            print(f'  ❌ Meta Ads error: {e}')
            result['campaigns'] = pd.DataFrame()
            result['ads']       = pd.DataFrame()

        # ── Instagram ──
        print('\n📸 [2/3] Instagram...')
        try:
            result['ig_organic'] = self.ig.fetch_organic(since, until, posts_goal_weekly)
            result['ig_posts']   = self.ig.fetch_posts(since, until)
            print(f"  ✅ {len(result['ig_organic'])} days | {len(result['ig_posts'])} posts")
        except Exception as e:
            print(f'  ❌ Instagram error: {e}')
            result['ig_organic'] = pd.DataFrame()
            result['ig_posts']   = pd.DataFrame()

        # ── Facebook ──
        print('\n📘 [3/3] Facebook Page...')
        try:
            result['fb_organic'] = self.fb.fetch_organic(since, until, posts_goal_weekly)
            result['fb_posts']   = self.fb.fetch_posts(since, until)
            print(f"  ✅ {len(result['fb_organic'])} days | {len(result['fb_posts'])} posts")
        except Exception as e:
            print(f'  ❌ Facebook error: {e}')
            result['fb_organic'] = pd.DataFrame()
            result['fb_posts']   = pd.DataFrame()

        # ── Export CSV (opsional) ──
        if export_path:
            os.makedirs(export_path, exist_ok=True)
            file_map = {
                'campaigns' : 'meta_campaigns.csv',
                'ads'       : 'meta_ads.csv',
                'ig_organic': 'ig_organic.csv',
                'ig_posts'  : 'ig_posts.csv',
                'fb_organic': 'fb_organic.csv',
                'fb_posts'  : 'fb_posts.csv',
            }
            print(f'\n💾 Exporting to {export_path}/')
            for key, filename in file_map.items():
                df = result.get(key, pd.DataFrame())
                if not df.empty:
                    # Hapus kolom thumbnail sebelum export
                    df_export = df.drop(columns=['thumbnail'], errors='ignore')
                    path      = os.path.join(export_path, filename)
                    df_export.to_csv(path, index=False)
                    print(f'  ✅ {filename} ({len(df_export)} rows)')

        print(f'\n✅ Done: {self.config.name}')
        return result

    def __repr__(self):
        return f"MetaConnector(portfolio={self.config.num}, name='{self.config.name}')"


# ══════════════════════════════════════════════════════════════════
# HELPER: fetch_all_portfolios()
# Fetch semua portfolio sekaligus — untuk scheduler / cron job
# ══════════════════════════════════════════════════════════════════

def fetch_all_portfolios(since: str, until: str,
                          portfolio_nums: list = [1, 2],
                          posts_goal_weekly: int = 4,
                          export_base_path: str = 'data/raw') -> dict:
    """
    Fetch semua portfolio sekaligus dan export ke CSV.

    Usage:
        from connectors.meta_api import fetch_all_portfolios

        all_data = fetch_all_portfolios(
            since      = '2026-03-01',
            until      = '2026-04-01',
            export_base_path = 'data/raw'
        )

    Return: dict {1: {...}, 2: {...}}
    """
    all_results = {}

    for num in portfolio_nums:
        try:
            connector        = MetaConnector(portfolio_num=num)
            export_path      = os.path.join(export_base_path, f'portfolio_{num}')
            all_results[num] = connector.fetch_all(
                since             = since,
                until             = until,
                posts_goal_weekly = posts_goal_weekly,
                export_path       = export_path,
            )
        except Exception as e:
            print(f'\n❌ Portfolio {num} failed: {e}')
            all_results[num] = None

    # Merge unified CSVs
    _merge_all_portfolios(export_base_path, portfolio_nums)

    return all_results


def _merge_all_portfolios(base_path: str, portfolio_nums: list):
    """Gabungkan CSV semua portfolio jadi unified CSV."""
    file_types = [
        'meta_campaigns', 'meta_ads',
        'ig_organic', 'ig_posts',
        'fb_organic', 'fb_posts',
    ]
    print(f'\n🔗 Merging unified CSVs...')
    for ft in file_types:
        dfs = []
        for num in portfolio_nums:
            path = os.path.join(base_path, f'portfolio_{num}', f'{ft}.csv')
            if os.path.exists(path):
                dfs.append(pd.read_csv(path))
        if dfs:
            merged   = pd.concat(dfs, ignore_index=True)
            out_path = os.path.join(base_path, f'all_{ft}.csv')
            merged.to_csv(out_path, index=False)
            print(f'  ✅ all_{ft}.csv ({len(merged)} rows)')
