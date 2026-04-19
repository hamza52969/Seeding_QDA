"""
Run this first to find which endpoint and params actually work on your machine.
It prints the raw status code + first 500 chars of every response so you can see
what the server is returning before any JSON parsing.
"""

import requests

BASE = "https://www.datafirst.uct.ac.za/dataportal/index.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.datafirst.uct.ac.za/dataportal/index.php/catalog",
}

CANDIDATES = [
    # ── NADA 5.x style ──────────────────────────────────────────────
    f"{BASE}/api/catalog",
    f"{BASE}/api/catalog?sk=NVivo&ps=5",
    f"{BASE}/api/catalog?q=NVivo&ps=5",

    # ── NADA 4.x style ──────────────────────────────────────────────
    f"{BASE}/api/catalog/search",
    f"{BASE}/api/catalog/search?sk=NVivo",
    f"{BASE}/api/catalog/search?q=NVivo",

    # ── Some NADA instances expose a /surveys endpoint ───────────────
    f"{BASE}/api/surveys",
    f"{BASE}/api/surveys?sk=NVivo",

    # ── Bare listing (no keyword) to see if catalog is accessible ────
    f"{BASE}/api/catalog?ps=3",
]

session = requests.Session()
session.headers.update(HEADERS)

for url in CANDIDATES:
    try:
        r = session.get(url, timeout=15)
        body = r.text[:500].strip()
        print(f"\n{'='*60}")
        print(f"URL    : {url}")
        print(f"Status : {r.status_code}")
        print(f"Content: {r.headers.get('Content-Type', 'N/A')}")
        print(f"Body   : {body[:300]}")
    except Exception as e:
        print(f"\nURL    : {url}")
        print(f"ERROR  : {e}")