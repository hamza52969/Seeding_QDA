# QDArchive Part 1 – Data Acquisition Pipeline

**Project:** Seeding QDArchive (SQ26)  
**Repos:** Harvard Dataverse · Columbia University Oral History DLC  
**Due:** March 15

---

## Setup

```bash
# 1. Clone / copy this folder to your machine
cd qdarchive_acquisition

# 2. Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Running the Pipeline

```bash
# Full run – search + download from both repos
python pipeline.py --source all

# Dry run – search & write metadata to DB, NO file downloads (fast, good for testing)
python pipeline.py --source all --dry-run

# Single source
python pipeline.py --source harvard
python pipeline.py --source columbia
```

---

## Output Structure

```
qdarchive_data/
├── metadata.db          ← SQLite database (canonical record)
├── metadata.csv         ← CSV export (submit this in the form)
├── pipeline.log         ← Full execution log
└── files/
    ├── Harvard/
    │   └── <doi-slug>/
    │       ├── transcript_interview.pdf     (primary)
    │       ├── coding.qdpx                  (qda)
    │       └── README.txt                   (additional)
    └── Columbia/
        └── <item-id>/
            └── transcript.pdf               (primary)
```

---

## Database Schema

### `projects` table (one row per research project / dataset)

| Column | Description |
|---|---|
| `source` | "Harvard Dataverse" or "Columbia Oral History DLC" |
| `source_url` | Direct link to the dataset/project page |
| `persistent_id` | DOI (Harvard) or DLC item ID (Columbia) |
| `title` | Dataset title |
| `description` | Abstract / description |
| `authors` | JSON array of author names |
| `subjects` | JSON array of subject categories |
| `keywords` | JSON array of keywords |
| `language` | Language of the data |
| `license` | Licence name / statement |
| `license_url` | URL to licence text |
| `published_date` | Date of publication |
| `has_qda_file` | 1 if a `.qdpx` / ATLAS.ti / NVivo / MAXQDA file is present |
| `qda_file_types` | Comma-separated extensions found (e.g. `.qdpx,.nvp`) |
| `file_count` | Number of files downloaded |
| `local_folder` | Path under `qdarchive_data/files/` |
| `notes` | Free-text notes / warnings |

### `files` table (one row per downloaded file)

| Column | Description |
|---|---|
| `project_id` | FK → `projects.id` |
| `filename` | Original filename |
| `content_type` | MIME type |
| `file_size_bytes` | File size |
| `file_role` | "qda" · "primary" · "additional" |
| `local_path` | Relative path under `qdarchive_data/` |
| `source_url` | Download URL |

---

## Inspecting Results

```bash
# Summary statistics
python inspect_db.py --stats

# List duplicates
python inspect_db.py --duplicates

# List projects that contain QDA files
python inspect_db.py --list-qda

# Re-export CSV
python inspect_db.py --export-csv
```

---

## ⚠ Columbia Access Warning

Most Columbia oral history materials require a **Columbia UNI login** to access.  
Only items publicly visible in the DLC without authentication are downloaded.  
Items are stored with `license = "Columbia fair-use only (verify before distributing)"`.  

**Before including Columbia data in the final QDArchive seed:**  
Check with Prof. Riehle whether "fair use for research" meets the project's open-licence requirement.  
Items with a confirmed Creative Commons licence will have that noted in the `license` column.

---

## Harvard Dataverse Tips

- The pipeline searches ~12 qualitative-focused queries and deduplicates by DOI.
- Only datasets with an **open licence** (CC0, CC-BY, etc.) are included.
- Files are downloaded individually to preserve original formats (not the zip bundle).
- To add more search terms, edit `HARVARD_QUERIES` at the top of `pipeline.py`.

---

## Submitting (Part 1 Checklist)

- [ ] Run `python pipeline.py --source all`
- [ ] Verify `qdarchive_data/metadata.db` and `metadata.csv` are present
- [ ] Upload `qdarchive_data/files/` to a shared drive / cloud storage
- [ ] Fill in the course submission form with:
  - Link to metadata.csv / database download
  - Link to the files folder
- [ ] `git tag part-1-release`
