"""
Debug script: cek semua metric available untuk 1 Reels post
Jalankan: python debug_reels.py
"""
import requests
import os
from dotenv import load_dotenv

load_dotenv()

MEDIA_ID    = "18060199013389841"
ACCESS_TOKEN = os.getenv("PORTFOLIO_1_ACCESS_TOKEN")
API_VERSION  = "v25.0"

# ── Step 1: Cek media type dulu ────────────────────────────────
print("=" * 60)
print("STEP 1: Cek media info")
print("=" * 60)
url    = f"https://graph.facebook.com/{API_VERSION}/{MEDIA_ID}"
params = {"fields": "id,media_type,timestamp,caption", "access_token": ACCESS_TOKEN}
r      = requests.get(url, params=params).json()
print(f"Media ID   : {r.get('id')}")
print(f"Media Type : {r.get('media_type')}")
print(f"Timestamp  : {r.get('timestamp')}")
print(f"Caption    : {str(r.get('caption',''))[:60]}")

# ── Step 2: Test semua metric kandidat satu per satu ───────────
print("\n" + "=" * 60)
print("STEP 2: Test metric satu per satu")
print("=" * 60)

candidates = [
    "views",
    "video_views",
    "ig_reels_aggregated_all_plays_count",
    "ig_reels_video_view_total_time",
    "plays",
    "reach",
    "impressions",
    "likes",
    "comments",
    "shares",
    "saved",
    "total_interactions",
]

results = {}
for metric in candidates:
    url    = f"https://graph.facebook.com/{API_VERSION}/{MEDIA_ID}/insights"
    params = {"metric": metric, "access_token": ACCESS_TOKEN}
    r      = requests.get(url, params=params).json()

    if "error" in r:
        code = r["error"].get("code", "?")
        msg  = r["error"].get("message", "")[:60]
        print(f"  ❌ {metric:<45} → Error {code}: {msg}")
        results[metric] = None
    else:
        data = r.get("data", [])
        if data:
            val = data[0].get("values", [{}])[0].get("value", "N/A")
            # kalau val = dict (misal breakdown), ambil total
            if isinstance(val, dict):
                val = sum(val.values())
            print(f"  ✅ {metric:<45} → {val}")
            results[metric] = val
        else:
            print(f"  ⚠️  {metric:<45} → No data returned")
            results[metric] = 0

# ── Step 3: Summary ────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 3: Summary — metric yang berhasil")
print("=" * 60)
for k, v in results.items():
    if v is not None:
        print(f"  {k}: {v}")

print("\nDone! Bandingkan nilai di atas dengan Views=269 di Instagram UI.")
