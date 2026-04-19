import requests
import sqlite3
import os
import csv
import re
import time
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
STUDENT_ID  = "233181324"
DB_NAME     = f"{STUDENT_ID}-seeding.db"
REPO_NAME   = "datafirst"
API_BASE    = "https://www.datafirst.uct.ac.za/dataportal/index.php/api"
PORTAL_BASE = "https://www.datafirst.uct.ac.za/dataportal/index.php"

SAVE_DIR = "./qdarchive_storage_haseeb"
CSV_NAME = "metadata_export_HASEEB.csv"

SEARCH_TERMS = [
    "NVivo", "MAXQDA", "REFI-QDA", "qualitative interview", "interview transcript",
    "oral history transcript", "focus group qualitative", "grounded theory",
    "thematic analysis qualitative", "ethnographic fieldnotes", "qualitative data analysis",
    "qdpx", "qdc", "mqda", "mqbac", "mqtc", "mqex", "mqmtr",
    "mx24", "mx24bac", "mc24", "mex24", "mx22", "mex22",
    "mx20", "mx18", "mx12", "mx11", "mx5", "mx4", "mx3", "mx2", "m2k",
    "loa", "sea", "mtr", "mod", "nvp", "nvpx", "atlasproj", "hpr7",
    "ppj", "pprj", "qlt", "f4p", "f4analyse", "Quirkos", "qpd",
]

QDA_EXTENSIONS = [
    ".qdpx", ".qdc", ".mqda", ".mqbac", ".mqtc", ".mqex", ".mqmtr",
    ".mx24", ".mx24bac", ".mc24", ".mex24", ".mx22", ".mex22",
    ".mx20", ".mx18", ".mx12", ".mx11", ".mx5", ".mx4", ".mx3", ".mx2", ".m2k",
    ".loa", ".sea", ".mtr", ".mod", ".nvp", ".nvpx", ".atlasproj", ".hpr7",
    ".ppj", ".pprj", ".qlt", ".f4p", ".qpd",
]

# Pre-compiled lowercase set for fast local matching
TERMS_LOWER = {t.lower() for t in SEARCH_TERMS}

# ── SESSION ───────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Accept":     "application/json, */*",
})

# ── HELPERS ───────────────────────────────────────────────────────────────────
def clean_license(lic_str):
    if not lic_str:
        return "UNKNOWN"
    u = lic_str.upper()
    if "CC0"          in u: return "CC0"
    if "CC BY 4.0"    in u or "ATTRIBUTION 4.0" in u: return "CC BY 4.0"
    if "CC BY-NC 4.0" in u: return "CC BY-NC 4.0"
    if "CC BY-SA 4.0" in u: return "CC BY-SA 4.0"
    clean = re.sub(r"<[^>]+>", "", lic_str).strip()
    clean = re.sub(r"\s+", " ", clean)
    return clean[:100] if clean else "UNKNOWN"

def matches_qda(row_dict):
    """
    Returns (True, matched_term) if any QDA search term appears in
    the combined text fields of a catalog row. Case-insensitive.
    """
    # keywords may be list or None
    kw_str = " ".join(row_dict.get("keywords", []) or [])
    blob = " ".join([
        row_dict.get("title",    "") or "",
        row_dict.get("subtitle", "") or "",
        kw_str,
        row_dict.get("abstract",    "") or "",
        row_dict.get("description", "") or "",
    ]).lower()

    for term in TERMS_LOWER:
        if term in blob:
            return True, term
    return False, None

# ── DB SETUP ──────────────────────────────────────────────────────────────────
def init_env():
    os.makedirs(SAVE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_NAME)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS PROJECTS (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_string TEXT, repository_id INTEGER, repository_url TEXT,
            project_url TEXT, version TEXT, title TEXT, description TEXT,
            language TEXT, doi TEXT, upload_date TEXT, download_date TEXT,
            download_repository_folder TEXT, download_project_folder TEXT,
            download_version_folder TEXT, download_method TEXT
        );
        CREATE TABLE IF NOT EXISTS FILES (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, file_name TEXT, file_type TEXT, status TEXT,
            FOREIGN KEY(project_id) REFERENCES PROJECTS(id)
        );
        CREATE TABLE IF NOT EXISTS KEYWORDS (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, keyword TEXT,
            FOREIGN KEY(project_id) REFERENCES PROJECTS(id)
        );
        CREATE TABLE IF NOT EXISTS LICENSES (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, license TEXT,
            FOREIGN KEY(project_id) REFERENCES PROJECTS(id)
        );
        CREATE TABLE IF NOT EXISTS PERSON_ROLE (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, name TEXT, role TEXT,
            FOREIGN KEY(project_id) REFERENCES PROJECTS(id)
        );
    """)
    conn.commit()
    return conn

# ── STEP 1: Fetch entire catalog (paginated) ──────────────────────────────────
def fetch_all_catalog_rows(page_size=100):
    """
    KEY FINDING from diagnostic:
      - 'q'  param ignores the value → always returns all 596 rows (useless for search)
      - 'sk' param filters only study_keywords field → returns 0 for most QDA terms
    SOLUTION: fetch ALL rows in pages, filter locally.
    Confirmed response key: data["result"]["rows"]  (not "results")
    """
    print("📥 Fetching full catalog (paginated)...")
    all_rows = []
    offset   = 0

    while True:
        r = SESSION.get(
            f"{API_BASE}/catalog",
            params={"ps": page_size, "from": offset},
            timeout=20,
        )
        r.raise_for_status()
        data   = r.json()
        result = data.get("result", {})          # confirmed key: "result"
        rows   = result.get("rows",  [])
        total  = result.get("found", 0)

        if not rows:
            break

        all_rows.extend(rows)
        offset += len(rows)
        print(f"  Fetched {offset}/{total}...", end="\r")

        if offset >= total:
            break

        time.sleep(0.4)

    print(f"\n  ✅ Catalog fetched: {len(all_rows)} datasets total.")
    return all_rows

# ── STEP 2: Full study metadata ───────────────────────────────────────────────
def fetch_study_metadata(idno):
    try:
        r = SESSION.get(f"{API_BASE}/catalog/{idno}", timeout=20)
        if r.ok and r.content:
            return r.json().get("dataset", {})
    except Exception as e:
        print(f"    ⚠ Metadata error ({idno}): {e}")
    return {}

# ── STEP 3: Resource list ─────────────────────────────────────────────────────
def fetch_resources(idno):
    try:
        r = SESSION.get(f"{API_BASE}/catalog/{idno}/resources", timeout=20)
        if r.ok and r.content:
            return r.json().get("resources", [])
    except Exception as e:
        print(f"    ⚠ Resource error ({idno}): {e}")
    return []

# ── STEP 4: Download + persist ────────────────────────────────────────────────
def acquire_project(idno, matched_term, metadata, conn):
    cursor = conn.cursor()

    title       = metadata.get("title",    "Unknown Title")
    repo_id_int = metadata.get("id",       0)
    created     = str(metadata.get("created", ""))[:10]
    doi_val     = metadata.get("doi", "") or ""
    project_url = (
        f"https://doi.org/{doi_val}"
        if doi_val and doi_val.startswith("10.")
        else f"{PORTAL_BASE}/catalog/{idno}"
    )

    # Description is nested under metadata.study_desc.study_info.abstract
    study_info  = (
        metadata.get("metadata", {})
                .get("study_desc", {})
                .get("study_info", {})
    )
    description = (
        study_info.get("abstract", "")
        or metadata.get("subtitle", "")
        or "No description available"
    )

    # Authors
    raw_entities = metadata.get("authoring_entity") or []
    if isinstance(raw_entities, str):
        raw_entities = [raw_entities] # Wrap in list if it's a single string

    authors = []
    for a in raw_entities:
        if isinstance(a, dict) and a.get("name"):
            authors.append(a.get("name"))
        elif isinstance(a, str) and a.strip():
            authors.append(a.strip())

    # Keywords
    topics   = study_info.get("subject", []) or []
    keywords = [
        t.get("topic", "") or t.get("keyword", "")
        for t in topics
        if t.get("topic") or t.get("keyword")
    ]

    # License
    access_cond = (
        metadata.get("metadata", {})
                .get("study_desc", {})
                .get("data_access", {})
                .get("use_statement", {})
                .get("cond_of_access", "")
        or metadata.get("access_type", "")
    )
    clean_lic = clean_license(access_cond)

    # Folder structure
    version_string = "v1.0"
    project_folder = re.sub(r"[/:\\* ]", "_", idno)
    local_path     = os.path.join(SAVE_DIR, REPO_NAME, project_folder, version_string)
    os.makedirs(local_path, exist_ok=True)

    download_date = datetime.now().isoformat()

    cursor.execute("""
        INSERT INTO PROJECTS (
            query_string, repository_id, repository_url, project_url,
            version, title, description, language, doi, upload_date,
            download_date, download_repository_folder, download_project_folder,
            download_version_folder, download_method
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        matched_term, repo_id_int, API_BASE, project_url,
        version_string, title, description, "en",
        doi_val or project_url, created, download_date,
        REPO_NAME, project_folder, version_string, "API-CALL",
    ))
    project_id = cursor.lastrowid

    # Download files
    resources = fetch_resources(idno)
    if not resources:
        print(f"    ⚠ No resources listed.")
        cursor.execute(
            "INSERT INTO FILES (project_id, file_name, file_type, status) VALUES (?,?,?,?)",
            (project_id, "N/A", "N/A", "NO_RESOURCES"),
        )
    else:
        for res in resources:
            rid      = res.get("id", "")
            filename = res.get("filename") or res.get("title", f"resource_{rid}")
            filetype = os.path.splitext(filename)[-1].lower() or "unknown"
            dl_url   = f"{PORTAL_BASE}/catalog/{idno}/download-resource/{rid}"

            file_r = SESSION.get(dl_url, stream=True, timeout=30)

            if file_r.status_code == 200:
                with open(os.path.join(local_path, filename), "wb") as f:
                    for chunk in file_r.iter_content(8192):
                        f.write(chunk)
                print(f"    ✅ Downloaded: {filename}")
                status = "SUCCESS"
            elif file_r.status_code == 403:
                print(f"    🔒 Restricted: {filename}")
                status = "RESTRICTED"
            else:
                print(f"    ❌ Failed [{file_r.status_code}]: {filename}")
                status = "FAILED"

            cursor.execute(
                "INSERT INTO FILES (project_id, file_name, file_type, status) VALUES (?,?,?,?)",
                (project_id, filename, filetype, status),
            )

    cursor.execute(
        "INSERT INTO LICENSES (project_id, license) VALUES (?,?)",
        (project_id, clean_lic),
    )
    for a in authors:
        cursor.execute(
            "INSERT INTO PERSON_ROLE (project_id, name, role) VALUES (?,?,?)",
            (project_id, a, "AUTHOR"),
        )
    for kw in keywords:
        cursor.execute(
            "INSERT INTO KEYWORDS (project_id, keyword) VALUES (?,?)",
            (project_id, kw),
        )
    conn.commit()
    print(f"    💾 Saved: {title[:70]}")

# ── CSV EXPORT ────────────────────────────────────────────────────────────────
def export_to_csv(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM PROJECTS")
    with open(CSV_NAME, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in cursor.description])
        writer.writerows(cursor.fetchall())
    print(f"✅ CSV exported: {CSV_NAME}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        print(f"🗑️  Removed old database '{DB_NAME}'.")

    conn = init_env()

    # 1. Fetch all 596 datasets once
    all_rows = fetch_all_catalog_rows(page_size=100)

    # 2. Filter locally — the only reliable approach given API limitations
    print("\n🔍 Filtering locally for QDA-related datasets...")
    matches = []
    for row in all_rows:
        hit, term = matches_qda(row)
        if hit:
            matches.append((row["idno"], term))

    print(f"  Matched {len(matches)} dataset(s) out of {len(all_rows)} total.")

    if not matches:
        print("\n⚠️  No QDA matches found.")
        print("   DataFirst hosts quantitative survey/census microdata —")
        print("   QDA project files (NVivo, MAXQDA etc.) are not its focus.")
    else:
        # 3. Full metadata + download for each match
        for i, (idno, term) in enumerate(matches, 1):
            print(f"\n[{i}/{len(matches)}] {idno}  (matched: '{term}')")
            metadata = fetch_study_metadata(idno)
            if metadata:
                acquire_project(idno, term, metadata, conn)
            else:
                print("    ⚠ No metadata returned.")
            time.sleep(0.5)

    export_to_csv(conn)
    conn.close()
    print(f"✅ Done. Database: {DB_NAME}")