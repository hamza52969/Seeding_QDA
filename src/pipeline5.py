import requests
import sqlite3
import os

# --- SUBMISSION CONFIGURATION ---
STUDENT_ID = "23082320" 
DB_NAME = f"{STUDENT_ID}-seeding.db" 
BASE_URL = "https://dataverse.harvard.edu/api"
SAVE_DIR = "./qdarchive_storage"

def init_env():
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repository TEXT, source_link TEXT, license_info TEXT,
            project_scope TEXT, local_path TEXT, qda_status TEXT
        )
    ''')
    conn.commit()
    return conn

def hunt_all_potential_qda():
    """Broad search for any dataset mentioning QDA formats."""
    print(f"🔎 Searching Harvard for any QDA-related datasets...")
    # Searching for multiple extensions at once
    search_url = f"{BASE_URL}/search?q=qdpx+OR+qdpz+OR+qdx+OR+MAXQDA+OR+NVivo&type=datasetOR"
    try:
        res = requests.get(search_url, timeout=15).json()
        items = res.get('data', {}).get('items', [])
        return [item.get('global_id') for item in items if 'global_id' in item]
    except Exception as e:
        print(f"⚠️ Search failed: {e}")
        return []

def acquire_if_possible(doi):
    conn = init_env()
    cursor = conn.cursor()
    
    try:
        # 1. Fetch Metadata
        res = requests.get(f"{BASE_URL}/datasets/:persistentId/?persistentId={doi}").json()
        if 'data' not in res or 'latestVersion' not in res['data']:
            return
            
        version = res['data']['latestVersion']
        files_list = version.get('files', [])
        
        # 2. Skip if Harvard doesn't actually have the file list (Harvested)
        if not files_list:
            return

        # 3. Setup Folder
        folder_name = doi.replace("/", "_").replace(":", "_")
        local_path = os.path.join(SAVE_DIR, folder_name)
        
        print(f"🚀 Inspecting: {doi}")
        downloaded_files = []

        # 4. Try to download each file
        for f_entry in files_list:
            f_id = f_entry['dataFile']['id']
            f_name = f_entry['dataFile']['filename']
            
            # Skip if restricted
            if f_entry.get('restricted', False):
                continue

            f_url = f"{BASE_URL}/access/datafile/{f_id}"
            try:
                # We check the response before creating the folder
                r = requests.get(f_url, stream=True, timeout=20)
                if r.status_code == 200:
                    if not os.path.exists(local_path):
                        os.makedirs(local_path)
                    
                    with open(os.path.join(local_path, f_name), 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                    downloaded_files.append(f_name)
            except:
                continue

        # 5. Log to DB only if we actually got data
        if downloaded_files:
            print(f"✅ Success! Saved {len(downloaded_files)} files to {local_path}")
            license_info = version.get('license', 'Open Access')
            if isinstance(license_info, dict): license_info = license_info.get('name', 'Open Access')

            cursor.execute('''
                INSERT INTO projects (repository, source_link, license_info, project_scope, local_path, qda_status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ("Harvard Dataverse", f"https://doi.org/{doi}", str(license_info), "Qualitative Archive Seed", local_path, "Files Downloaded"))
            conn.commit()
        else:
            print(f"⏭️ Skipped {doi}: No public files available for download.")

    except Exception as e:
        pass # Silently handle metadata errors to keep the search moving
    finally:
        conn.close()

if __name__ == "__main__":
    dois = hunt_all_potential_qda()
    print(f"📊 Found {len(dois)} potential projects. Starting acquisition...")
    
    for doi in dois:
        acquire_if_possible(doi)
    
    print(f"\n📁 Seeding complete: {DB_NAME}")