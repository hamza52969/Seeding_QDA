"""
inspect_db.py — Database inspection for the 5-table schema
===========================================================
Usage:
  python inspect_db.py --stats
  python inspect_db.py --duplicates
  python inspect_db.py --list-qda
  python inspect_db.py --export-csv
"""
import argparse
import csv
import sqlite3
from pathlib import Path

DB_PATH  = Path("src") / "metadata.db"
CSV_PATH = Path("src") / "metadata.csv"

QDA_EXTENSIONS = {
    "qdpx","qdc",
    "mqda","mqbac","mqtc","mqex","mqmtr",
    "mx24","mx24bac","mc24","mex24",
    "mx22","mex22","mx20","mx18","mx12","mx11",
    "mx5","mx4","mx3","mx2","m2k",
    "loa","sea","mtr","mod",
    "nvp","nvpx",
    "atlasproj","hpr7",
    "ppj","pprj","qlt",
    "f4p","qpd",
}

def open_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def print_stats(conn):
    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    print(f"\n=== QDArchive DB Stats ===")
    print(f"Total projects: {total}")
    for row in conn.execute("SELECT repository_id, repository_url, COUNT(*) n FROM projects GROUP BY repository_id").fetchall():
        print(f"  repo_id={row['repository_id']}  {row['repository_url']:45s} → {row['n']:>5} projects")

    method_rows = conn.execute("SELECT download_method, COUNT(*) n FROM projects GROUP BY download_method").fetchall()
    print("\nBy download method:")
    for r in method_rows:
        print(f"  {r['download_method']:12s} {r['n']:>5}")

    total_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    qda_count   = conn.execute(
        f"SELECT COUNT(*) FROM files WHERE file_type IN ({','.join('?'*len(QDA_EXTENSIONS))})",
        list(QDA_EXTENSIONS)
    ).fetchone()[0]
    print(f"\nTotal files    : {total_files}")
    print(f"QDA files      : {qda_count}")

    total_kw = conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
    total_pr = conn.execute("SELECT COUNT(*) FROM person_role").fetchone()[0]
    total_lic = conn.execute("SELECT COUNT(*) FROM licenses").fetchone()[0]
    print(f"Keywords       : {total_kw}")
    print(f"Person records : {total_pr}")
    print(f"License records: {total_lic}")

    print("\nLicense distribution:")
    for r in conn.execute("SELECT license, COUNT(*) n FROM licenses GROUP BY license ORDER BY n DESC").fetchall():
        print(f"  {r['license']:30s} {r['n']:>5}")


def find_duplicates(conn):
    rows = conn.execute("""
        SELECT title, COUNT(*) n, GROUP_CONCAT(repository_id) repos
        FROM projects
        GROUP BY LOWER(TRIM(title))
        HAVING n > 1
        ORDER BY n DESC
    """).fetchall()
    print(f"\n=== Suspected Duplicates ({len(rows)}) ===")
    if not rows:
        print("  None found.")
    for r in rows:
        print(f"  [{r['n']}x] repos={r['repos']} | {r['title'][:70]}")


def list_qda(conn):
    placeholders = ",".join("?" * len(QDA_EXTENSIONS))
    rows = conn.execute(f"""
        SELECT DISTINCT p.id, p.repository_id, p.title, p.project_url, f.file_type
        FROM projects p
        JOIN files f ON f.project_id = p.id
        WHERE f.file_type IN ({placeholders})
        ORDER BY p.repository_id, p.title
    """, list(QDA_EXTENSIONS)).fetchall()
    print(f"\n=== Projects with QDA files ({len(rows)}) ===")
    for r in rows:
        print(f"  [id={r['id']} repo={r['repository_id']}] .{r['file_type']:12s} | {r['title'][:55]:55s}")
        print(f"    {r['project_url']}")


def export_csv(conn):
    rows = conn.execute("SELECT * FROM projects ORDER BY repository_id, id").fetchall()
    if not rows:
        print("No data.")
        return
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=rows[0].keys())
        w.writeheader()
        for r in rows:
            w.writerow(dict(r))
    print(f"CSV → {CSV_PATH}  ({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stats",      action="store_true")
    parser.add_argument("--duplicates", action="store_true")
    parser.add_argument("--list-qda",   action="store_true")
    parser.add_argument("--export-csv", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"DB not found at {DB_PATH}. Run pipeline.py first.")
        return

    conn = open_db()
    if args.stats:      print_stats(conn)
    if args.duplicates: find_duplicates(conn)
    if args.list_qda:   list_qda(conn)
    if args.export_csv: export_csv(conn)
    if not any(vars(args).values()):
        print_stats(conn)
        find_duplicates(conn)
    conn.close()

if __name__ == "__main__":
    main()