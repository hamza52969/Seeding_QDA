"""
QDArchive Seeding – Part 1: Data Acquisition Pipeline  (v2 – fixed)
=====================================================================
Repositories:
  1. Harvard Dataverse  (repository_id=1)  https://dataverse.harvard.edu
  2. Columbia Oral History DLC (repository_id=2) https://dlc.library.columbia.edu
  3. Zenodo             (repository_id=3)  https://zenodo.org
  4. OSF (Open Science Framework) (repository_id=4) https://osf.io

DB schema follows SQLite_Meta_Data_Database_Schema.xlsx exactly.
QDA extensions follow QDA_File_Extensions_Formats.xlsx exactly.

Usage:
  python pipeline2.py --source all
  python pipeline2.py --source harvard
  python pipeline2.py --source columbia
  python pipeline2.py --source zenodo
  python pipeline2.py --source osf
  python pipeline2.py --source all --dry-run          # metadata only, no downloads
  python pipeline2.py --source all --dry-run --test   # 1 query, 3 results – fast smoke test

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

# ── Harvard base URL (MUST be defined before resolve_dataverse_base) ─────────────
# BUG FIX: Was previously defined ~250 lines later, causing NameError at runtime.

HARVARD_BASE   = "https://dataverse.harvard.edu"
HARVARD_REPOID = 1

# ── Dataverse DOI routing (resolves federated Dataverse instances) ─────────────

DATAVERSE_ROUTER = {
    "10.7910":  "https://dataverse.harvard.edu",  # Harvard
    "10.5683":  "https://borealisdata.ca",         # Borealis (Canada)
    "10.17026": "https://dataverse.nl",            # DANS (Netherlands)
    "10.18710": "https://dataverse.no",            # DataverseNO (Norway)
    "10.34894": "https://dataverse.nl",            # DataverseNL
}


def resolve_dataverse_base(doi: str) -> str:
    """Route a DOI to the Dataverse instance that actually hosts it."""
    if not doi:
        return HARVARD_BASE
    for prefix, base in DATAVERSE_ROUTER.items():
        if doi.startswith(prefix):
            log.info(f"   Routing {doi} → {base}")
            return base
    return HARVARD_BASE


# ── QDA file extensions (all from QDA_File_Extensions_Formats.xlsx) ────────────

QDA_EXTENSIONS = {
    # REFI-QDA standard (also used by QDAcity)
    ".qdpx", ".qdc",
    # MAXQDA – all version variants
    #".mqda", ".mqbac", ".mqtc", ".mqex", ".mqmtr",
    #".mx24", ".mx24bac", ".mc24", ".mex24",
    #".mx22", ".mex22",
    # ".mx20", ".mx18", ".mx12", ".mx11",
    #".mx5", ".mx4", ".mx3", ".mx2", ".m2k",
    #".loa", ".sea", ".mtr", ".mod",
    # NVivo
    #".nvp", ".nvpx",
    # ATLAS.ti
   # ".atlasproj", ".hpr7",
    # QDA Miner
   # ".ppj", ".pprj", ".qlt",
    # f4analyse
  #  ".f4p",
    # Quirkos
    ".qpd",
}

# ── License normalisation ───────────────────────────────────────────────────────

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
]

PERSON_ROLES = {"UPLOADER", "AUTHOR", "OWNER", "OTHER", "UNKNOWN"}

# ── Harvard search queries ──────────────────────────────────────────────────────

HARVARD_QUERIES = [
    #"qualitative interview",
    #"interview transcript",
   # "oral history transcript",
   # "focus group qualitative",
    "qdpx",
    #"ATLAS.ti",
   # "NVivo",
  #  "MAXQDA",
   # "grounded theory",
   # "thematic analysis qualitative",
 #   "ethnographic fieldnotes",
  #  "qualitative data analysis",
]

# QDA file extension search terms (type=file to find parent datasets)
HARVARD_QDA_FILE_QUERIES = [
    # REFI-QDA standard
    "qdpx", "qdc",
    # MAXQDA
   # "mqda", "mx24", "mx22", "mx20", "mx18", "mx12", "mex24", "mex22",
    # NVivo
  #  "nvp", "nvpx",
    # ATLAS.ti
#    "atlasproj", "hpr7",
    # QDA Miner
   # "ppj", "pprj",
    # f4analyse
  #  "f4p",
    # Quirkos
    "qpd",
]

OPEN_LICENSE_KEYWORDS = [
    "cc0", "cc by", "cc-by", "creative commons", "public domain",
    "open data commons", "odc-by", "pddl", "odbl",
]

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

HARVARD_REPOURL = REPO_REGISTRY[1]["url"]
HARVARD_FOLDER  = REPO_REGISTRY[1]["folder"]


class HarvardDataversePipeline:
    PER_PAGE = 100
    MAX_RESULTS_PER_QUERY = 50

    def search(self, query: str) -> list[dict]:
        """
        Return public dataset search results.
        subtree=harvard restricts to Harvard's own dataverse only.
        """
        results = []
        start = 0
        while True:
            data = get_json(f"{HARVARD_BASE}/api/search", params={
                "q":        query,
                "type":     "dataset",
                "per_page": self.PER_PAGE,
                "start":    start,
                "sort":     "date",
                "order":    "desc",
            })
            if not data:
                log.warning(f"  [Harvard] No response for query='{query}' start={start}")
                break
            if data.get("status") != "OK":
                log.warning(f"  [Harvard] Bad status='{data.get('status')}' query='{query}'")
                break
            inner = data.get("data", {})
            items = inner.get("items", [])
            total = inner.get("total_count", 0)

            results.extend(items)
            start += self.PER_PAGE
            if start >= total or not items or len(results) >= self.MAX_RESULTS_PER_QUERY:
                break

        return results[:self.MAX_RESULTS_PER_QUERY]

    def search_files(self, query: str) -> list[str]:
        """
        Search type=file for a QDA extension keyword.
        Returns deduplicated parent dataset DOIs.
        """
        parent_dois = []
        start = 0
        while True:
            data = get_json(f"{HARVARD_BASE}/api/search", params={
                "q":        query,
                "type":     "file",
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

    def get_dataset_meta(self, doi: str, base_url: str = None) -> dict | None:
        """Fetch full dataset metadata from the correct host."""
        host = base_url or HARVARD_BASE
        data = get_json(
            f"{host}/api/datasets/:persistentId/",
            params={"persistentId": doi},
        )
        if data and data.get("status") == "OK":
            return data["data"]
        return None

    def _fields(self, ds_meta: dict) -> dict:
        fields = (
            ds_meta.get("latestVersion", {})
            .get("metadataBlocks", {})
            .get("citation", {})
            .get("fields", [])
        )
        return {f.get("typeName", ""): f.get("value") for f in fields}

    def get_file_list(self, doi: str, base_url: str = None) -> list[dict]:
        host = base_url or HARVARD_BASE
        data = get_json(
            f"{host}/api/datasets/:persistentId/versions/:latest/files",
            params={"persistentId": doi},
        )
        if data and data.get("status") == "OK":
            return data.get("data", [])
        return []

    def _extract_license_from_item(self, item: dict) -> str:
        """
        Pull licence from a Harvard Dataverse search result item.
        Attempt order:
          1. item["license"]["name"]
          2. GET /api/datasets/:persistentId/versions/:latest
          3. GET /api/datasets/:persistentId/
          4. Empty string → caller should skip this dataset.
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

        # 2. Version endpoint
        if doi:
            ver = get_json(
                f"{HARVARD_BASE}/api/datasets/:persistentId/versions/:latest",
                params={"persistentId": doi, "excludeFiles": "true"},
            )
            if ver and ver.get("status") == "OK":
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

        # 3. Full dataset endpoint
        if doi:
            ds = get_json(
                f"{HARVARD_BASE}/api/datasets/:persistentId/",
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

    def _process_dataset(
        self,
        conn: sqlite3.Connection,
        doi: str,
        query: str,
        item: dict,
        dry_run: bool,
    ) -> bool:
        """
        Insert a single dataset into the DB and optionally download its QDA files.
        Returns True if the project was newly inserted, False otherwise.

        BUG FIX: This helper enforces the correct order:
          1. License check  (skip non-open datasets — do NOT insert first)
          2. Deduplication  (skip already-stored projects)
          3. DB insert
          4. File download
        """
        # 1. License check — BEFORE any DB write
        raw_license = self._extract_license_from_item(item)
        if not is_open_license(raw_license):
            log.debug(f"  [license-skip] '{raw_license}' for {doi}")
            return False

        # 2. Build project URL and check for duplicates
        doi_url     = self._extract_doi_url(doi)
        project_url = doi_url
        if project_exists(conn, HARVARD_REPOID, project_url):
            log.debug(f"  Already in DB: {doi}")
            return False

        # 3. Extract metadata and insert
        title       = item.get("name", doi)[:500]
        description = item.get("description", "")[:4000]
        upload_date = item.get("published_at", "")
        doi_slug    = safe_folder_name(doi.replace("doi:", ""))

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
            return True

        # 4. Download QDA files only
        target_base = resolve_dataverse_base(doi)
        file_list   = self.get_file_list(doi, base_url=target_base)
        dest_base   = FILES_DIR / HARVARD_FOLDER / doi_slug

        for fmeta in file_list:
            df       = fmeta.get("dataFile", {})
            file_id  = df.get("id")
            filename = df.get("filename", f"file_{file_id}")
            if not file_id:
                continue
            ext = Path(filename).suffix.lower()
            if ext not in QDA_EXTENSIONS:
                log.debug(f"    - Skipping non-QDA file: {filename}")
                continue
            dl_url = f"{target_base}/api/access/datafile/{file_id}?format=original"
            if download_file(dl_url, dest_base / filename):
                insert_file(conn, proj_id, filename)
                log.info(f"    ★ Downloaded: {filename}")
            else:
                log.warning(f"    ✗ Failed: {filename}")

        return True

    def run(self, conn: sqlite3.Connection, dry_run: bool = False, test_mode: bool = False):
        log.info("=" * 60)
        log.info("STARTING Harvard Dataverse acquisition")
        log.info("=" * 60)

        seen_dois:       set[str] = set()
        skipped_license: int      = 0
        processed:       int      = 0

        # ── Pass 1: dataset-level keyword search ─────────────────────────────
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

                ok = self._process_dataset(conn, doi, query, item, dry_run)
                if ok:
                    processed += 1
                else:
                    skipped_license += 1

        # ── Pass 2: QDA file-extension search ────────────────────────────────
        log.info("[Harvard] Starting QDA file-extension search pass...")
        qda_queries = HARVARD_QDA_FILE_QUERIES[:1] if test_mode else HARVARD_QDA_FILE_QUERIES

        for ext_query in qda_queries:
            log.info(f"[Harvard] QDA file search: '{ext_query}'")
            parent_dois = self.search_files(ext_query)
            log.info(f"  → {len(parent_dois)} parent datasets")

            for doi in parent_dois:
                if doi in seen_dois:
                    continue
                seen_dois.add(doi)

                # Fetch metadata from the actual hosting repository
                target_base = resolve_dataverse_base(doi)
                ds_meta = self.get_dataset_meta(doi, base_url=target_base)
                if not ds_meta:
                    log.warning(f"  Failed to fetch metadata for {doi} from {target_base}")
                    continue

                # Build a synthetic item dict compatible with _process_dataset
                fields    = self._fields(ds_meta)
                latest    = ds_meta.get("latestVersion", {})
                title_v   = fields.get("title", "")
                title_str = (title_v if isinstance(title_v, str) else "") or doi

                desc_list = fields.get("dsDescription", []) or []
                desc = ""
                if isinstance(desc_list, list) and desc_list:
                    first = desc_list[0]
                    if isinstance(first, dict):
                        dv = first.get("dsDescriptionValue", {})
                        desc = dv.get("value", "") if isinstance(dv, dict) else str(dv)

                lang_list = fields.get("language", []) or []
                language  = lang_list[0] if isinstance(lang_list, list) and lang_list else ""

                # Build item dict reusing _process_dataset logic
                synth_item = {
                    "global_id":   doi,
                    "name":        title_str,
                    "description": desc,
                    "published_at": fields.get("productionDate", ""),
                    "license":     latest.get("license", {}),
                    "authors":     [],
                    "keywords":    [],
                    "subjects":    fields.get("subject", []),
                }

                ok = self._process_dataset(
                    conn, doi, f"file:{ext_query}", synth_item, dry_run
                )
                if ok:
                    processed += 1
                else:
                    skipped_license += 1

        log.info(
            f"[Harvard] Done. processed={processed}, "
            f"skipped_or_no_license={skipped_license}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 2. COLUMBIA ORAL HISTORY DLC  (repository_id = 2, download_method = SCRAPING)
# ══════════════════════════════════════════════════════════════════════════════

COLUMBIA_BASE    = "https://dlc.library.columbia.edu"
COLUMBIA_REPOID  = 2
COLUMBIA_REPOURL = REPO_REGISTRY[2]["url"]
COLUMBIA_FOLDER  = REPO_REGISTRY[2]["folder"]
COLUMBIA_SEARCH_URL = f"{COLUMBIA_BASE}/catalog.json"

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
    ⚠  LICENSE WARNING: Columbia DLC does NOT stamp per-item CC licences by default.
    ⚠  ACCESS WARNING: Most items require a Columbia UNI login.
    """

    def _discover_repo_filter_value(self) -> str:
        data = get_json(COLUMBIA_SEARCH_URL, params=[
            ("q",        "oral history"),
            ("per_page", 1),
            ("page",     1),
        ])
        if not data:
            return ""
        for key in ("included", "facets", "search_facets"):
            facets = data.get(key, [])
            if facets:
                log.info(f"[Columbia] Facet key='{key}': {str(facets)[:300]}")
                break
        log.info(f"[Columbia] Top-level keys: {list(data.keys())}")
        return ""

    def _search_page(self, query: str, page: int) -> dict | None:
        params = [
            ("q",                            query),
            ("search_field",                 "all_text_teim"),
            ("f[lib_repo_short_ssim][]",     self._repo_filter),
            ("per_page",                     100),
            ("page",                         page),
        ]
        return get_json(COLUMBIA_SEARCH_URL, params=params)

    def _search_page_no_filter(self, query: str, page: int) -> dict | None:
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
        return list(dict.fromkeys(urls))

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

        seen_ids:  set[str] = set()
        processed: int      = 0

        CANDIDATE_FILTERS = [
            "University Archives",
            "Oral History Archives at Columbia",
            "Oral History",
            "OHAC",
            "ldpd_4079329",
            "",
        ]
        self._repo_filter = "Oral History"

        log.info("[Columbia] Auto-detecting correct repository filter value...")
        self._discover_repo_filter_value()

        for candidate in CANDIDATE_FILTERS:
            self._repo_filter = candidate
            probe = (self._search_page("oral history", 1) if candidate
                     else self._search_page_no_filter("oral history", 1))
            probe_count = (probe.get("meta", {}).get("pages", {}).get("total_count", 0)
                           if probe else 0)
            log.info(f"[Columbia] filter='{candidate}' → {probe_count} results")
            if probe_count > 0:
                log.info(f"[Columbia] Using filter: '{candidate}'")
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

                    author_raw = av("lib_author_ssim") or av("creator_tesim") or ""
                    persons = [
                        {"name": n.strip(), "role": "AUTHOR"}
                        for n in re.split(r"[;|]", author_raw) if n.strip()
                    ]

                    subject_raw = av("subject_topic_ssim") or av("subject_tesim") or ""
                    keywords = [k.strip() for k in re.split(r"[;|]", subject_raw) if k.strip()]

                    project_url = f"{COLUMBIA_BASE}/catalog/{item_id}"

                    if project_exists(conn, COLUMBIA_REPOID, project_url):
                        log.debug(f"  Already in DB: {item_id}")
                        continue

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
                    processed += 1

                    if dry_run:
                        continue

                    try:
                        manifest = self._get_iiif_manifest(item_id)
                        urls     = self._transcript_urls(manifest)
                    except Exception as e:
                        log.debug(f"    IIIF error for {item_id}: {e}")
                        urls = []

                    dest_base  = FILES_DIR / COLUMBIA_FOLDER / safe_folder_name(item_id)
                    files_saved = 0
                    for url in urls:
                        fn = Path(url.split("?")[0]).name or f"{item_id}_file"
                        if not Path(fn).suffix:
                            fn += ".pdf"
                        dest = dest_base / fn
                        if download_file(url, dest):
                            insert_file(conn, proj_id, fn)
                            files_saved += 1
                    if files_saved:
                        log.info(f"    → {files_saved} file(s) saved for {item_id}")

                if page >= total_pages:
                    break
                page += 1

        log.info(f"[Columbia] Done. metadata_stored={processed}")




# ══════════════════════════════════════════════════════════════════════════════
# CSV export
# ══════════════════════════════════════════════════════════════════════════════

def export_csv(conn: sqlite3.Connection):
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
        "--source",
        choices=["all", "harvard", "columbia", "zenodo", "osf"],
        default="all",
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

    if args.test:
        args.dry_run = True
        log.info("TEST MODE: 1 query, 3 results per source, no downloads")

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)

    conn = open_db()
    fix_existing_licenses(conn)

    if args.source in ("all", "harvard"):
        HarvardDataversePipeline().run(conn, dry_run=args.dry_run, test_mode=args.test)

    if args.source in ("all", "columbia"):
        ColumbiaOralHistoryPipeline().run(conn, dry_run=args.dry_run, test_mode=args.test)

    if args.source in ("all", "zenodo"):
        ZenodoPipeline().run(conn, dry_run=args.dry_run, test_mode=args.test)

    if args.source in ("all", "osf"):
        OSFPipeline().run(conn, dry_run=args.dry_run, test_mode=args.test)

    export_csv(conn)
    conn.close()
    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()