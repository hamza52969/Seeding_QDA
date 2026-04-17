"""
QDArchive Seeding – Part 1: Data Acquisition Pipeline  (v2)
=============================================================
Repositories:
  1. Harvard Dataverse  (repository_id=1)  https://dataverse.harvard.edu
  2. Columbia Oral History DLC (repository_id=2) https://dlc.library.columbia.edu

DB schema follows SQLite_Meta_Data_Database_Schema.xlsx exactly.
QDA extensions follow QDA_File_Extensions_Formats_1_.xlsx exactly.

Usage:
  python pipeline.py --source all
  python pipeline.py --source harvard
  python pipeline.py --source columbia
  python pipeline.py --source all --dry-run          # metadata only, no downloads
  python pipeline.py --source all --dry-run --test   # 1 query, 3 results – fast smoke test

Dependencies:
  pip install requests tqdm
"""

import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from tqdm import tqdm

# ── Paths ───────────────────────────────────────────────────────────────────────

BASE_DIR  = Path("src")
FILES_DIR = BASE_DIR / "files"
DB_PATH   = BASE_DIR / "metadata2.db"
CSV_PATH  = BASE_DIR / "metadata2.csv"
LOG_PATH  = BASE_DIR / "pipeline2.log"

# ── Repository registry ─────────────────────────────────────────────────────────
# repository_id values match what the class assigns; add more repos here later.

REPO_REGISTRY = {
    1: {
        "name":   "Harvard Dataverse",
        "url":    "https://dataverse.harvard.edu",
        "folder": "harvard",
    },
    2: {
        "name":   "Columbia Oral History DLC",
        "url":    "https://dlc.library.columbia.edu",
        "folder": "columbia",
    },
}

# ── QDA file extensions (QDA_File_Extensions_Formats_1_.xlsx) ──────────────────

QDA_EXTENSIONS = {
    # REFI-QDA standard (also used by QDAcity)
    ".qdpx", ".qdc",
    # MAXQDA – all version variants
   # ".mqda", ".mqbac", ".mqtc", ".mqex", ".mqmtr",
   # ".mx24", ".mx24bac", ".mc24", ".mex24",
  #  ".mx22", ".mex22",
  #  ".mx20", ".mx18", ".mx12", ".mx11",
   # ".mx5", ".mx4", ".mx3", ".mx2", ".m2k",
  #  ".loa", ".sea", ".mtr", ".mod",
    # NVivo
  #  ".nvp", ".nvpx",
    # ATLAS.ti
  #  ".atlasproj", ".hpr7",
    # QDA Miner
   # ".ppj", ".pprj", ".qlt",
    # f4analyse
   # ".f4p",*/
    # Quirkos
    ".qpd",
}

# ── License normalisation (data_types sheet) ────────────────────────────────────
# Map common strings → the professor's enum.  If no match: store original string.

LICENSE_MAP = [
    ("cc0",             "CC0"),
    ("cc-by-sa",        "CC BY-SA"),
    ("cc by-sa",        "CC BY-SA"),
    ("cc-by-nc-nd",     "CC BY-NC-ND"),
    ("cc by-nc-nd",     "CC BY-NC-ND"),
    ("cc-by-nc",        "CC BY-NC"),
    ("cc by-nc",        "CC BY-NC"),
    ("cc-by-nd",        "CC BY-ND"),
    ("cc by-nd",        "CC BY-ND"),
    ("cc-by",           "CC BY"),
    ("cc by",           "CC BY"),
    ("odbl-1.0",        "ODbL-1.0"),
    ("odbl",            "ODbL"),
    ("odc-by-1.0",      "ODC-By-1.0"),
    ("odc-by",          "ODC-By"),
    ("pddl",            "PDDL"),
    ("public domain",   "CC0"),
          # treat public domain as CC0-equivalent
]

# PERSON_ROLE enum
PERSON_ROLES = {"UPLOADER", "AUTHOR", "OWNER", "OTHER", "UNKNOWN"}

# Harvard dataset-level search queries
HARVARD_QUERIES = [
  #  "qualitative interview",
   # "interview transcript",
    #"oral history transcript",
    #"focus group qualitative",
    "qdpx",
    #"ATLAS.ti",
   # "NVivo",
   # "MAXQDA",
    #"grounded theory",
    #"thematic analysis qualitative",
    #"ethnographic fieldnotes",
    #"qualitative data analysis",
]

# QDA file extension search terms — used with type=file to find datasets
# that CONTAIN these files (the parent dataset DOI is extracted from the result)
HARVARD_QDA_FILE_QUERIES = [
    # REFI-QDA standard
    "qdpx", "qdc",
    # MAXQDA
   # "mqda", "mx24", "mx22", "mx20", "mx18", "mx12", "mex24", "mex22",
    # NVivo
   # "nvp", "nvpx",
    # ATLAS.ti
   # "atlasproj", "hpr7",
    # QDA Miner
   # "ppj", "pprj",
    # f4analyse
   # "f4p",
    # Quirkos
    #"qpd",
]

# Open-licence whitelist used for skipping Harvard datasets
OPEN_LICENSE_KEYWORDS = [
    "cc0", "cc by", "cc-by", "creative commons", "public domain",
    "open data commons", "odc-by", "pddl", "odbl",
]

# Route DOIs to their actual host Dataverse to bypass Harvard API 404 errors
DATAVERSE_ROUTER = {
    "10.7910": "https://dataverse.harvard.edu", # Harvard
    "10.5683": "https://borealisdata.ca",       # Borealis (Canada)
    "10.17026": "https://dataverse.nl",         # DANS (Netherlands)
    "10.18710": "https://dataverse.no",         # DataverseNO (Norway)
    "10.34894": "https://dataverse.nl",         # DataverseNL
}

def resolve_dataverse_base(doi: str) -> str:
    for prefix, base_url in DATAVERSE_ROUTER.items():
        if prefix in doi:
            return base_url
    return "https://dataverse.harvard.edu" # Fallback to Harvard

REQUEST_DELAY  = 1.0
MAX_RETRIES    = 5
BACKOFF_FACTOR = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, mode="a"),
    ],
)
log = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────────────────────────

def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def normalize_license(raw: str) -> str:
    """Map a raw licence string to the professor's enum; fall back to raw."""
    if not raw:
        return ""
    lower = raw.lower().strip()
    for pattern, canonical in LICENSE_MAP:
        if pattern in lower:
            return canonical
    return raw.strip()


def is_open_license(raw: str) -> bool:
    lower = raw.lower()
    return any(k in lower for k in OPEN_LICENSE_KEYWORDS)


def safe_folder_name(text: str) -> str:
    return re.sub(r"[^\w.-]", "_", text)


# ── Database ─────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    query_string                TEXT,
    repository_id               INTEGER NOT NULL,
    repository_url              TEXT    NOT NULL,
    project_url                 TEXT    NOT NULL,
    version                     TEXT,
    title                       TEXT    NOT NULL,
    description                 TEXT,
    language                    TEXT,
    doi                         TEXT,
    upload_date                 TEXT,
    download_date               TEXT    NOT NULL,
    download_repository_folder  TEXT    NOT NULL,
    download_project_folder     TEXT    NOT NULL,
    download_version_folder     TEXT,
    download_method             TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    file_name   TEXT    NOT NULL,
    file_type   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS keywords (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    keyword     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS person_role (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    name        TEXT    NOT NULL,
    role        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS licenses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    license     TEXT    NOT NULL
);
"""

def open_db() -> sqlite3.Connection:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def project_exists(conn, repository_id: int, project_url: str) -> int | None:
    row = conn.execute(
        "SELECT id FROM projects WHERE repository_id=? AND project_url=?",
        (repository_id, project_url),
    ).fetchone()
    return row["id"] if row else None


def insert_project(conn, data: dict) -> int:
    cols = list(data.keys())
    conn.execute(
        f"INSERT INTO projects ({','.join(cols)}) VALUES ({','.join('?'*len(cols))})",
        list(data.values()),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def insert_keywords(conn, project_id: int, keywords: list[str]):
    for kw in keywords:
        kw = kw.strip()
        if kw:
            conn.execute(
                "INSERT INTO keywords (project_id, keyword) VALUES (?,?)",
                (project_id, kw),
            )
    conn.commit()


def insert_persons(conn, project_id: int, persons: list[dict]):
    """persons = [{'name': '...', 'role': 'AUTHOR'}, ...]"""
    for p in persons:
        role = p.get("role", "UNKNOWN").upper()
        if role not in PERSON_ROLES:
            role = "UNKNOWN"
        conn.execute(
            "INSERT INTO person_role (project_id, name, role) VALUES (?,?,?)",
            (project_id, p["name"], role),
        )
    conn.commit()


def insert_license(conn, project_id: int, raw_license: str):
    normalized = normalize_license(raw_license)
    if normalized:
        conn.execute(
            "INSERT INTO licenses (project_id, license) VALUES (?,?)",
            (project_id, normalized),
        )
        conn.commit()


def fix_existing_licenses(conn: sqlite3.Connection):
    """Normalize any raw/ugly license strings already in the DB."""
    rows = conn.execute("SELECT id, license FROM licenses").fetchall()
    updated = 0
    for row in rows:
        normalized = normalize_license(row["license"])
        if normalized != row["license"]:
            conn.execute("UPDATE licenses SET license=? WHERE id=?",
                         (normalized, row["id"]))
            updated += 1
    conn.commit()
    if updated:
        log.info(f"Normalized {updated} license strings in DB")


def insert_file(conn, project_id: int, file_name: str):
    ext = Path(file_name).suffix.lower().lstrip(".")
    conn.execute(
        "INSERT INTO files (project_id, file_name, file_type) VALUES (?,?,?)",
        (project_id, file_name, ext),
    )
    conn.commit()


# ── HTTP ─────────────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "QDArchiveBot/2.0 (SQ26 research data acquisition; "
        "FAU Erlangen; contact: your-email@fau.de)"
    ),
    "Accept": "application/json",
})


def get_json(url: str, params: dict | list = None) -> dict | None:
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)
            r = SESSION.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            if r.status_code == 429:
                wait = BACKOFF_FACTOR ** attempt * 5
                log.warning(f"Rate-limited – waiting {wait:.0f}s")
                time.sleep(wait)
            else:
                log.error(f"HTTP {r.status_code} for {url}: {e}")
                return None
        except Exception as e:
            wait = BACKOFF_FACTOR ** attempt
            log.warning(f"Request error ({e}), retry {attempt+1}/{MAX_RETRIES} in {wait:.0f}s")
            time.sleep(wait)
    return None


def download_file(url: str, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        log.debug(f"Already exists: {dest}")
        return True
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)
            r = SESSION.get(url, stream=True, timeout=120)
            r.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(65536):
                    fh.write(chunk)
            return True
        except Exception as e:
            wait = BACKOFF_FACTOR ** attempt
            log.warning(f"Download failed ({e}), retry {attempt+1}/{MAX_RETRIES} in {wait:.0f}s")
            time.sleep(wait)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# 1. HARVARD DATAVERSE  (repository_id = 1, download_method = API-CALL)
# ══════════════════════════════════════════════════════════════════════════════

HARVARD_BASE  = "https://dataverse.harvard.edu"
HARVARD_REPOID = 1
HARVARD_REPOURL = REPO_REGISTRY[1]["url"]
HARVARD_FOLDER  = REPO_REGISTRY[1]["folder"]


class HarvardDataversePipeline:
    PER_PAGE = 100

    # Per-query cap — prevents a single broad search term from running for hours.
    # 26k results × 1 s delay = 7 h. 2000 results × 1 s = ~33 min manageable.
    MAX_RESULTS_PER_QUERY = 50

    # ── search ───────────────────────────────────────────────────────────────────

    def search(self, query: str) -> list[dict]:
        """
        Return Harvard-only public dataset results.

          subtree=harvard      restricts to Harvard's own dataverse, not the
                               federated network (DANS, ICPSR, AUSSDA, etc.)
          fq=fileAccess:Public pre-filters restricted/embargoed datasets so
                               we never waste a metadata call on them.
        """
        results = []
        start = 0
        while True:
            data = get_json(f"{HARVARD_BASE}/api/search", params={
                "q":        query,
                "type":     "dataset",
               # "subtree":  "harvard",
                "per_page": self.PER_PAGE,
                "start":    start,
                "sort":     "date",
                "order":    "desc",
            })
            if not data:
                log.warning(f"  [Harvard] No response for query='{query}' start={start}")
                break
            if data.get("status") != "OK":
                log.warning(f"  [Harvard] Bad status='{data.get('status')}' msg='{data.get('message','')}' query='{query}'")
                break
            inner = data.get("data", {})
            items = inner.get("items", [])
            total = inner.get("total_count", 0)
            if start == 0 and total == 0:
                log.debug(f"  [Harvard] API inner keys: {list(inner.keys())}")

            # subtree=harvard already limits to Harvard's installation.
            # No DOI-prefix filter needed — would drop valid sub-dataverse results.
            results.extend(items)

            start += self.PER_PAGE
            if start >= total or not items or len(results) >= self.MAX_RESULTS_PER_QUERY:
                break

        return results[:self.MAX_RESULTS_PER_QUERY]

    def search_files(self, query: str) -> list[str]:
        """
        Search type=file for a QDA extension keyword.
        Returns a deduplicated list of parent dataset DOIs (doi:10.7910/...).
        The Dataverse file search result includes "dataset_persistent_id"
        pointing to the parent dataset — we fetch that dataset normally.
        """
        parent_dois = []
        start = 0
        while True:
            data = get_json(f"{HARVARD_BASE}/api/search", params={
                "q":        query,
                "type":     "file",
               # "subtree":  "harvard",
                "per_page": self.PER_PAGE,
                "start":    start,
            })
            if not data or data.get("status") != "OK":
                break
            inner = data.get("data", {})
            items = inner.get("items", [])
            total = inner.get("total_count", 0)
            for item in items:
                parent = item.get("dataset_persistent_id", "")
                if parent and parent not in parent_dois:
                    parent_dois.append(parent)
            start += self.PER_PAGE
            if start >= total or not items:
                break
        return parent_dois

    def get_dataset_meta(self, doi: str) -> dict | None:
        """Fetch full dataset metadata record."""
        data = get_json(
            f"{HARVARD_BASE}/api/datasets/:persistentId/",
            params={"persistentId": doi},
        )
        if data and data.get("status") == "OK":
            return data["data"]
        return None

    def _fields(self, ds_meta: dict) -> dict:
        """Extract citation metadata fields into a flat dict."""
        fields = (
            ds_meta.get("latestVersion", {})
            .get("metadataBlocks", {})
            .get("citation", {})
            .get("fields", [])
        )
        return {f.get("typeName", ""): f.get("value") for f in fields}

    def get_file_list(self, doi: str) -> list[dict]:
        data = get_json(
            f"{HARVARD_BASE}/api/datasets/:persistentId/versions/:latest/files",
            params={"persistentId": doi},
        )
        if data and data.get("status") == "OK":
            return data.get("data", [])
        return []

    # ── helpers (extract from search result item directly) ────────────────────────
    # The search API returns enough metadata in the item itself.
    # We do NOT call /api/datasets/:persistentId/ per dataset — that was causing
    # 401 errors on federated results and doubled the request count.

    def _extract_license_from_item(self, item: dict, base_url: str) -> str:
        """
        Pull licence from a Harvard Dataverse search result item.

        Attempt order:
          1. item["license"]["name"]  — present in Dataverse 5.10+ search results
          2. GET /api/datasets/:persistentId/versions/:latest  — version-level license
          3. GET /api/datasets/:persistentId/  — full dataset record as last resort
          4. If all fail: return "CC0" for publicly-listed items (Harvard policy
             default) so they are not silently dropped; logged for manual review.
        """
        doi = item.get("global_id", "")

        # 1. Direct field in search item
        lic = item.get("license", {})
        if isinstance(lic, dict):
            val = lic.get("name", "") or lic.get("uri", "")
            if val:
                return val
        if isinstance(lic, str) and lic:
            return lic

        # 2. Version endpoint — license lives at data["data"]["license"]["name"]
        if doi:
            ver = get_json(
                f"{base_url}/api/datasets/:persistentId/versions/:latest",
                params={"persistentId": doi, "excludeFiles": "true"},
            )
            if ver:
                if ver.get("status") == "OK":
                    ld = ver["data"].get("license", {})
                    if isinstance(ld, dict):
                        val = ld.get("name", "") or ld.get("uri", "")
                        if val:
                            return val
                    if isinstance(ld, str) and ld:
                        return ld
                    terms = ver["data"].get("termsOfUse", "")
                    if terms:
                        return terms
                else:
                    log.debug(f"  version endpoint status={ver.get('status')} for {doi}")
            else:
                log.debug(f"  version endpoint returned None for {doi}")

        # 3. Full dataset endpoint
        if doi:
            ds = get_json(
                f"{base_url}/api/datasets/:persistentId/",
                params={"persistentId": doi},
            )
            if ds and ds.get("status") == "OK":
                latest = ds["data"].get("latestVersion", {})
                ld = latest.get("license", {})
                if isinstance(ld, dict):
                    val = ld.get("name", "") or ld.get("uri", "")
                    if val:
                        return val
                if isinstance(ld, str) and ld:
                    return ld
                terms = latest.get("termsOfUse", "")
                if terms:
                    return terms

        # 4. License could not be determined from any endpoint.
        #    Project rule: "No license = do not use" — skip this dataset.
        log.debug(f"  license undetermined for {doi} — skipping")
        return ""

    def _extract_authors_from_item(self, item: dict) -> list[dict]:
        persons = []
        for author in item.get("authors", []):
            name = author if isinstance(author, str) else author.get("name", "")
            if name:
                persons.append({"name": name, "role": "AUTHOR"})
        return persons

    def _extract_keywords_from_item(self, item: dict) -> list[str]:
        kws = []
        for kw in item.get("keywords", []):
            if isinstance(kw, str) and kw:
                kws.append(kw)
        for subj in item.get("subjects", []):
            if isinstance(subj, str) and subj:
                kws.append(subj)
        return kws

    def _extract_doi_url(self, doi_str: str) -> str:
        if doi_str.startswith("doi:"):
            return "https://doi.org/" + doi_str[4:]
        if doi_str.startswith("http"):
            return doi_str
        return "https://doi.org/" + doi_str

    # ── main run ──────────────────────────────────────────────────────────────────

    def run(self, conn: sqlite3.Connection, dry_run: bool = False, test_mode: bool = False):
        log.info("=" * 60)
        log.info("STARTING Harvard Dataverse acquisition")
        log.info("=" * 60)

        seen_dois: set[str] = set()
        skipped_license = 0
        processed = 0

        queries = HARVARD_QUERIES[:1] if test_mode else HARVARD_QUERIES

        for query in queries:
            log.info(f"[Harvard] Query: '{query}'")
            items = self.search(query)
            if test_mode:
                items = items[:3]
            log.info(f"  → {len(items)} results (capped at {self.MAX_RESULTS_PER_QUERY})")

            for item in tqdm(items, desc=f"Harvard '{query}'", leave=False):
                doi = item.get("global_id", "")
                if not doi or doi in seen_dois:
                    continue
                seen_dois.add(doi)

                # Skip federated datasets from other institutions (DANS, QDR, etc.)
                # They appear in Harvard search results but always return 401.
                # Harvard's own datasets use the doi:10.7910/ prefix.
               # if not doi.startswith("doi:10.7910/"):
                  #  log.debug(f"  Skipping federated dataset: {doi}")
                  #  skipped_license += 1
                   #continue
#
                target_base = resolve_dataverse_base(doi)
                raw_license = self._extract_license_from_item(item, target_base)

                if not is_open_license(raw_license):
                    # Log first 5 skipped licenses at INFO so we can see what's returned
                    if skipped_license < 5:
                        log.info(f"  [license-skip] '{raw_license}' → {doi}")
                    skipped_license += 1
                    continue

                title        = item.get("name", doi)[:500]
                description  = item.get("description", "")[:4000]
                upload_date  = item.get("published_at", "")
                doi_url      = self._extract_doi_url(doi)
                doi_slug     = safe_folder_name(doi.replace("doi:", ""))
                project_url  = doi_url

                if project_exists(conn, HARVARD_REPOID, project_url):
                    log.debug(f"  Already in DB: {doi}")
                    continue

                proj_data = dict(
                    query_string               = query,
                    repository_id              = HARVARD_REPOID,
                    repository_url             = HARVARD_REPOURL,
                    project_url                = project_url,
                    version                    = None,
                    title                      = title,
                    description                = description,
                    language                   = "",
                    doi                        = doi_url,
                    upload_date                = str(upload_date) if upload_date else None,
                    download_date              = now_ts(),
                    download_repository_folder = HARVARD_FOLDER,
                    download_project_folder    = doi_slug,
                    download_version_folder    = None,
                    download_method            = "API-CALL",
                )

                proj_id = insert_project(conn, proj_data)
                log.info(f"  [DB id={proj_id}] {title[:60]}")

                insert_keywords(conn, proj_id, self._extract_keywords_from_item(item))
                insert_persons(conn, proj_id, self._extract_authors_from_item(item))
                insert_license(conn, proj_id, raw_license)

                if dry_run:
                    processed += 1
                    continue

                # Download files (only API call remaining per dataset)
                """file_list = self.get_file_list(doi)
                dest_base = FILES_DIR / HARVARD_FOLDER / doi_slug

                for fmeta in file_list:
                    df       = fmeta.get("dataFile", {})
                    file_id  = df.get("id")
                    filename = df.get("filename", f"file_{file_id}")
                    if not file_id:
                        continue
                    dl_url = f"{HARVARD_BASE}/api/access/datafile/{file_id}?format=original"
                    dest   = dest_base / filename
                    ok     = download_file(dl_url, dest)
                    if ok:
                        insert_file(conn, proj_id, filename)
                        log.debug(f"    ✓ {filename}")
                    else:
                        log.warning(f"    ✗ Failed: {filename}")"""
                # Download targeted QDA files only
                file_list = self.get_file_list(doi)
                dest_base = FILES_DIR / HARVARD_FOLDER / doi_slug
                
                for fmeta in file_list:
                    df       = fmeta.get("dataFile", {})
                    file_id  = df.get("id")
                    filename = df.get("filename", f"file_{file_id}")
                    if not file_id:
                        continue
                    
                    # --- NEW STRICT FILTER ---
                    ext = Path(filename).suffix.lower()
                    if ext not in QDA_EXTENSIONS:
                        log.debug(f"    - Skipping non-QDA file: {filename}")
                        continue
                    # -------------------------

                    dl_url = f"{HARVARD_BASE}/api/access/datafile/{file_id}?format=original"
                    dest = dest_base / filename
                    ok   = download_file(dl_url, dest)
                    
                    if ok:
                        insert_file(conn, proj_id, filename)
                        log.info(f"    ★ Downloaded: {filename}")
                    else:
                        log.warning(f"    ✗ Failed: {filename}")

                processed += 1

        # ── QDA file-level search pass ──────────────────────────────────────────
        # Search directly for files with QDA extensions to find datasets that
        # contain them — these are the most valuable files for QDArchive.
        log.info("[Harvard] Starting QDA file-extension search pass...")
        qda_queries = HARVARD_QDA_FILE_QUERIES[:1] if test_mode else HARVARD_QDA_FILE_QUERIES


        log.info(
            f"[Harvard] Done. processed={processed}, "
            f"skipped={skipped_license} (non-open-license or federated)"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 2. COLUMBIA ORAL HISTORY DLC  (repository_id = 2, download_method = SCRAPING)
# ══════════════════════════════════════════════════════════════════════════════

COLUMBIA_BASE   = "https://dlc.library.columbia.edu"
COLUMBIA_REPOID = 2
COLUMBIA_REPOURL = REPO_REGISTRY[2]["url"]
COLUMBIA_FOLDER  = REPO_REGISTRY[2]["folder"]

COLUMBIA_SEARCH_URL = f"{COLUMBIA_BASE}/catalog.json"   # .json suffix = JSON API endpoint

# Known publicly accessible sub-collections from the guide page
# Each entry carries its search query (acts as query_string in the DB)
COLUMBIA_PROJECTS = [
    "Carnegie Corporation oral history",
    "Rule of Law oral history Columbia",
    "Notable New Yorkers oral history",
    "IRWGS feminist oral history Columbia",
    "Obama presidency oral history INCITE",
    "Lehman oral history Columbia",
    "Rare Book Manuscript oral history Columbia",
]


class ColumbiaOralHistoryPipeline:
    """
    ⚠  LICENSE WARNING:
    Columbia DLC does NOT stamp per-item CC licences by default.
    Items are stored with whatever licence text appears in the metadata.
    If no licence is found, the field is left blank.
    Do NOT distribute without verifying with Prof. Riehle.

    ⚠  ACCESS WARNING:
    Most items require a Columbia UNI login.
    Only items whose catalogue record is publicly visible are recorded.
    Files that return a 403/redirect are skipped.
    """

    def _discover_repo_filter_value(self) -> str:
        """
        Query the DLC catalog with no filter and inspect the facet values
        to find the exact string used for the Oral History repository.
        Logs all available lib_repo_short_ssim values.
        """
        data = get_json(COLUMBIA_SEARCH_URL, params=[
            ("q",        "oral history"),
            ("per_page", 1),
            ("page",     1),
        ])
        if not data:
            return ""
        # Facets live under data["included"] in JSONAPI format
        # or under data["facets"] in older Blacklight
        for key in ("included", "facets", "search_facets"):
            facets = data.get(key, [])
            if facets:
                log.info(f"[Columbia] Facet container key='{key}' type={type(facets)}")
                log.info(f"[Columbia] Facets preview: {str(facets)[:400]}")
                break
        # Also log top-level keys for reference
        log.info(f"[Columbia] Top-level response keys: {list(data.keys())}")
        docs = data.get("data", [])
        log.info(f"[Columbia] Docs returned (no filter): {len(docs)}")
        if docs:
            log.info(f"[Columbia] First doc keys: {list(docs[0].keys())}")
        return ""

    def _search_page(self, query: str, page: int) -> dict | None:
        # Pass params as a list of tuples so requests does NOT percent-encode
        # the square brackets — Blacklight/Rails requires literal [ ] in the URL.
        # The filter value for the Oral History repository is discovered at runtime.
        params = [
            ("q",                            query),
            ("search_field",                 "all_text_teim"),
            ("f[lib_repo_short_ssim][]",     self._repo_filter),
            ("per_page",                     100),
            ("page",                         page),
        ]
        return get_json(COLUMBIA_SEARCH_URL, params=params)

    def _search_page_no_filter(self, query: str, page: int) -> dict | None:
        """Fallback: search without any repository filter."""
        params = [
            ("q",            query),
            ("search_field", "all_text_teim"),
            ("per_page",     20),
            ("page",         page),
        ]
        return get_json(COLUMBIA_SEARCH_URL, params=params)

    def _get_iiif_manifest(self, item_id: str) -> dict | None:
        return get_json(f"{COLUMBIA_BASE}/iiif/2/{item_id}/manifest")

    def _transcript_urls(self, manifest: dict) -> list[str]:
        """Pull downloadable file URLs from IIIF manifest rendering hints."""
        if not manifest:
            return []
        urls = []
        for r in manifest.get("rendering", []):
            if isinstance(r, dict) and r.get("@id"):
                urls.append(r["@id"])
        for seq in manifest.get("sequences", []):
            for canvas in seq.get("canvases", []):
                for r in canvas.get("rendering", []):
                    if isinstance(r, dict) and r.get("@id"):
                        urls.append(r["@id"])
        return list(dict.fromkeys(urls))  # deduplicate, preserve order

    def _attr_val(self, attrs: dict, key: str) -> str:
        v = attrs.get(key, {})
        if isinstance(v, dict):
            return str(v.get("attributes", {}).get("value", ""))
        if isinstance(v, list):
            return "; ".join(str(x) for x in v if x)
        return str(v) if v else ""

    def run(self, conn: sqlite3.Connection, dry_run: bool = False, test_mode: bool = False):
        log.info("=" * 60)
        log.info("STARTING Columbia Oral History DLC acquisition")
        log.info("=" * 60)
        log.warning(
            "[Columbia] Most materials require a Columbia UNI login. "
            "Only publicly visible items are recorded. "
            "Verify licences before distributing."
        )

        seen_ids: set[str] = set()
        processed = 0

        # Discover the correct lib_repo_short_ssim value for Oral History.
        # Known candidates from Columbia's DLC facets:
        #   "Avery"  "Barnard"  "Burke"  "RBML"  "Oral History"  "ldpd:..."
        # We try them in order; whichever returns docs > 0 is used.
        CANDIDATE_FILTERS = [
            "University Archives",                 # confirmed working from auto-detection
            "Oral History Archives at Columbia",
            "Oral History",
            "OHAC",
            "ldpd_4079329",
            "",                                    # no filter (last resort)
        ]
        self._repo_filter = "Oral History"   # default; overridden below

        log.info("[Columbia] Auto-detecting correct repository filter value...")
        self._discover_repo_filter_value()   # logs facet info for reference

        # Try each candidate to find one that returns results
        for candidate in CANDIDATE_FILTERS:
            self._repo_filter = candidate
            probe = self._search_page("oral history", 1) if candidate else self._search_page_no_filter("oral history", 1)
            probe_count = probe.get("meta", {}).get("pages", {}).get("total_count", 0) if probe else 0
            log.info(f"[Columbia] filter='{candidate}' → {probe_count} results")
            if probe_count > 0:
                log.info(f"[Columbia] Using filter value: '{candidate}'")
                break
        else:
            log.warning("[Columbia] No filter returned results — items may require login")

        queries = COLUMBIA_PROJECTS[:1] if test_mode else COLUMBIA_PROJECTS

        for query in queries:
            log.info(f"[Columbia] Query: '{query}'")
            page = 1

            while True:
                data = (self._search_page(query, page) if self._repo_filter
                        else self._search_page_no_filter(query, page))
                if not data:
                    break

                docs        = data.get("data", [])
                pages_meta  = data.get("meta", {}).get("pages", {})
                total_pages = pages_meta.get("total_pages", 1)
                total_count = pages_meta.get("total_count", 0)
                log.info(f"  Page {page}/{total_pages}, total={total_count}, docs={len(docs)}")

                if not docs:
                    break

                if test_mode:
                    docs = docs[:3]

                for doc in docs:
                    item_id = doc.get("id", "")
                    if not item_id or item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    attrs = doc.get("attributes", {})
                    av = lambda k: self._attr_val(attrs, k)

                    title       = av("title_ssi") or av("title_tesim") or item_id
                    description = av("abstract_tesim") or av("description_tesim")
                    language    = av("language_language_term_text_ssim")
                    pub_date    = av("origin_info_date_created_tesim") or av("pub_date_isi")
                    raw_license = av("rights_tesim") or av("use_and_reproduction_tesim") or ""

                    # Authors
                    author_raw = av("lib_author_ssim") or av("creator_tesim") or ""
                    persons = []
                    for name in re.split(r"[;|]", author_raw):
                        name = name.strip()
                        if name:
                            persons.append({"name": name, "role": "AUTHOR"})

                    # Keywords from subject fields
                    subject_raw = av("subject_topic_ssim") or av("subject_tesim") or ""
                    keywords = [k.strip() for k in re.split(r"[;|]", subject_raw) if k.strip()]

                    project_url = f"{COLUMBIA_BASE}/catalog/{item_id}"

                    if project_exists(conn, COLUMBIA_REPOID, project_url):
                        log.debug(f"  Already in DB: {item_id}")
                        continue

                    # Folder: download_project_folder = item_id
                    doi_url = av("identifier_doi_ssim") or ""

                    proj_data = dict(
                        query_string                = query,
                        repository_id               = COLUMBIA_REPOID,
                        repository_url              = COLUMBIA_REPOURL,
                        project_url                 = project_url,
                        version                     = None,
                        title                       = str(title)[:500],
                        description                 = str(description)[:4000],
                        language                    = language,
                        doi                         = doi_url if doi_url.startswith("http") else None,
                        upload_date                 = str(pub_date) if pub_date else None,
                        download_date               = now_ts(),
                        download_repository_folder  = COLUMBIA_FOLDER,
                        download_project_folder     = safe_folder_name(item_id),
                        download_version_folder     = None,
                        download_method             = "SCRAPING",
                    )

                    proj_id = insert_project(conn, proj_data)
                    log.info(f"  [DB id={proj_id}] {str(title)[:60]}")

                    insert_keywords(conn, proj_id, keywords)
                    insert_persons(conn, proj_id, persons)
                    insert_license(conn, proj_id, raw_license)

                    processed += 1   # count every metadata insert

                    if dry_run:
                        continue

                    # Attempt IIIF manifest to find downloadable files.
                    # Most Columbia items require a UNI login — IIIF will return
                    # no renderings. Files are skipped but metadata is kept.
                    try:
                        manifest = self._get_iiif_manifest(item_id)
                        urls     = self._transcript_urls(manifest)
                    except Exception as e:
                        log.debug(f"    IIIF error for {item_id}: {e}")
                        urls = []

                    dest_base = FILES_DIR / COLUMBIA_FOLDER / safe_folder_name(item_id)
                    files_saved = 0
                    for url in urls:
                        fn = Path(url.split("?")[0]).name or f"{item_id}_file"
                        if not Path(fn).suffix:
                            fn += ".pdf"
                        dest = dest_base / fn
                        ok   = download_file(url, dest)
                        if ok:
                            insert_file(conn, proj_id, fn)
                            files_saved += 1
                            log.debug(f"    ✓ {fn}")
                        else:
                            log.debug(f"    ✗ (login-restricted): {fn}")
                    if files_saved:
                        log.info(f"    → {files_saved} file(s) saved for {item_id}")

                if page >= total_pages:
                    break
                page += 1

        log.info(f"[Columbia] Done. metadata_stored={processed} "
                 f"(files: most Columbia items require UNI login — "
                 f"check qdarchive_data/files/columbia/ for any that downloaded)")


# ══════════════════════════════════════════════════════════════════════════════
# CSV export
# ══════════════════════════════════════════════════════════════════════════════

def export_csv(conn: sqlite3.Connection):
    """Export the projects table (all required fields) as a CSV."""
    rows = conn.execute("SELECT * FROM projects ORDER BY repository_id, id").fetchall()
    if not rows:
        log.info("No projects to export.")
        return
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
    log.info(f"CSV exported → {CSV_PATH}  ({len(rows)} rows)")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="QDArchive Part-1 Acquisition Pipeline (v2 – schema-compliant)"
    )
    parser.add_argument(
        "--source", choices=["all", "harvard", "columbia"], default="all",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Search and record metadata only; skip file downloads",
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Quick smoke test: 1 query, max 3 datasets per source, no downloads",
    )
    args = parser.parse_args()

    # test mode always implies dry-run
    if args.test:
        args.dry_run = True
        log.info("TEST MODE: 1 query, 3 results per source, no downloads")

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)

    conn = open_db()
    fix_existing_licenses(conn)   # normalize any ugly strings from previous runs

    if args.source in ("all", "harvard"):
        HarvardDataversePipeline().run(conn, dry_run=args.dry_run, test_mode=args.test)

    if args.source in ("all", "columbia"):
        ColumbiaOralHistoryPipeline().run(conn, dry_run=args.dry_run, test_mode=args.test)

    export_csv(conn)
    conn.close()
    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()