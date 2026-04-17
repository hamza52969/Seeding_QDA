import requests
import sqlite3
import os
import csv

# --- SUBMISSION CONFIGURATION ---
STUDENT_ID = "23082320" 
DB_NAME = f"{STUDENT_ID}-seeding2.db" 

# Repository APIs
REPOSITORIES = {
    
    "Harvard": "https://dataverse.harvard.edu/api"
}

SAVE_DIR = "./qdarchive_storage2"
CSV_NAME = "metadata_export2.csv"
#QDA_EXTENSIONS = ['.qdpx', '.qdpz', '.qdx', '.mx24']
QDA_EXTENSIONS = [
    # REFI-QDA standard (also used by QDAcity)
    ".qdpx", ".qdc",
    # MAXQDA – all version variants
    ".mqda", ".mqbac", ".mqtc", ".mqex", ".mqmtr",
    ".mx24", ".mx24bac", ".mc24", ".mex24",
    ".mx22", ".mex22",
    ".mx20", ".mx18", ".mx12", ".mx11",
    ".mx5", ".mx4", ".mx3", ".mx2", ".m2k",
    ".loa", ".sea", ".mtr", ".mod",
    # NVivo
    ".nvp", ".nvpx",
    # ATLAS.ti
    ".atlasproj", ".hpr7",
    # QDA Miner
    ".ppj", ".pprj", ".qlt",
    # f4analyse
    ".f4p",
    # Quirkos
    ".qpd",
]

def init_env():
    """Initializes the required database file in the root folder."""
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Schema designed to capture all metadata required for Step 2
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repository TEXT,
            source_link TEXT,
            license_info TEXT,
            project_scope TEXT,
            local_path TEXT,
            qda_status TEXT
        )
    ''')
    conn.commit()
    return conn

def search_repo(base_url, repo_name, keyword):
    """Searches a specific repository for datasets."""
    print(f"🔎 Searching {repo_name} for: '{keyword}'...")
    url = f"{base_url}/search?q={keyword}&type=dataset"
    try:
        res = requests.get(url, timeout=10).json()
        return [item.get('global_id') for item in res.get('data', {}).get('items', [])]
    except Exception as e:
        print(f"Search failed for {repo_name}: {e}")
        return []

def acquire_project(base_url, repo_name, doi):
    """Downloads the project bundle and logs metadata to the student DB."""
    conn = init_env()
    cursor = conn.cursor()
    print(f"\nProcessing {repo_name} Project: {doi}")
    
    try:
        # 1. Fetch Metadata
        res = requests.get(f"{base_url}/datasets/:persistentId/?persistentId={doi}").json()
        if 'data' not in res: return
        version = res['data']['latestVersion']
        
        # 2. Scanner: Check for Analysis Data (QDA files)
        qda_status = "Not Found"
        for f_entry in version.get('files', []):
            fname = f_entry.get('dataFile', {}).get('filename', '').lower()
            is_restricted = f_entry.get('restricted', False)
            
            if any(fname.endswith(ext) for ext in QDA_EXTENSIONS):
                qda_status = f"Restricted ({fname})" if is_restricted else f"Public ({fname})"
                if is_restricted:
                    print(f"Note: {fname} is restricted. Downloading other files...")
                else:
                    print(f"Found Public QDA: {fname}")
                break

        # 3. License Check (Mandatory Rule)
        license_info = version.get('license', 'No License Found')
        if isinstance(license_info, dict): license_info = license_info.get('name', 'No License Found')
        if license_info == 'No License Found': license_info = version.get('termsOfUse', 'No License Found')

        if license_info == 'No License Found' or "restricted" in license_info.lower():
            print(f"Skipping {doi}: Proprietary or missing license.")
            return

        # 4. Download Folder Heuristic
        folder_name = doi.replace("/", "_").replace(":", "_")
        local_path = os.path.join(SAVE_DIR, folder_name)
        os.makedirs(local_path, exist_ok=True)

        dl_url = f"{base_url}/access/dataset/:persistentId/?persistentId={doi}"
        file_res = requests.get(dl_url, stream=True)
        
        content_type = file_res.headers.get('Content-Type', '')
        if file_res.status_code == 200 and 'zip' in content_type:
            zip_path = os.path.join(local_path, "project_bundle.zip")
            with open(zip_path, 'wb') as f:
                for chunk in file_res.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded successfully to {local_path}")

            # 5. Metadata Extraction for Part 1 Step 2
            source_link = f"https://doi.org/{doi.replace('doi:', '')}"
            project_scope = ""
            for field in version['metadataBlocks']['citation']['fields']:
                if field['typeName'] == 'dsDescription':
                    project_scope = field['value'][0]['dsDescriptionValue']['value']

            cursor.execute('''
                INSERT INTO projects (repository, source_link, license_info, project_scope, local_path, qda_status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (repo_name, source_link, license_info, project_scope, local_path, qda_status))
            conn.commit()
        else:
            print(f"❌ Zip bundle download failed. (Status: {file_res.status_code})")

    except Exception as e:
        print(f"⚠️ Technical challenge: {e}")
    finally:
        conn.close()

def export_to_csv():
    """Generates the metadata export for the project delivery."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects")
    with open(CSV_NAME, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in cursor.description])
        writer.writerows(cursor.fetchall())
    conn.close()

if __name__ == "__main__":
    # Target search terms for qualitative analysis
    search_terms = ["qdpx", "NVivo", "MAXQDA", "REFI-QDA"]
    
    for name, api_url in REPOSITORIES.items():
        for term in search_terms:
            found_dois = search_repo(api_url, name, term)
            for doi in found_dois:
                acquire_project(api_url, name, doi)
    
    export_to_csv()
    print(f"\n Database created: {DB_NAME}")
    print(f" CSV created: {CSV_NAME}")