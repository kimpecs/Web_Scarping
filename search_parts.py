import sqlite3
from pathlib import Path

DB_PATH = Path("etl_output/parts_staging.sqlite")

def search(term, limit=10):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Search in candidate parts
    cur.execute("""
        SELECT part_number, description, page, s.filename
        FROM parts_candidates p
        JOIN source_documents s ON p.doc_id = s.doc_id
        WHERE part_number LIKE ? OR description LIKE ?
        LIMIT ?
    """, (f"%{term}%", f"%{term}%", limit))
    results = cur.fetchall()

    # FTS fallback: raw lines search
    cur.execute("""
        SELECT text, page, s.filename
        FROM raw_lines_fts f
        JOIN source_documents s ON f.doc_id = s.doc_id
        WHERE f.text MATCH ?
        LIMIT ?
    """, (term, limit))
    fts_results = cur.fetchall()

    conn.close()
    return results, fts_results


if __name__ == "__main__":
    q = input("Enter part search term: ").strip()
    results, fts_results = search(q)

    print("\n--- Candidate Parts ---")
    for r in results:
        print(r)

    print("\n--- Raw Line Matches (FTS) ---")
    for r in fts_results:
        print(r)
