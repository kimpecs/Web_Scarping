import os
import re
import argparse
import logging
import sqlite3
import requests
import pdfplumber
import pandas as pd
from pathlib import Path
from datetime import datetime

# -----------------------------
# CONFIGURATION
# -----------------------------
PDF_URLS = {
    "Dayton_Hydraulic_Brakes": "https://lascotruckparts.com/wp-content/uploads/2023/09/Dayton-Parts-Frenos-Hidraulicos-Parte-1-ENG-Dayton-Parts-Hydraulic-Brakes-Part-1.pdf",
    "Dana_Spicer": "https://www.canadawideparts.com/downloads/catalogs/dana_spicer_tandemAxles_461-462-463-521-581_AXIP-0085A.pdf",
    "PAI_Drivetrain": "https://barringtondieselclub.co.za/mack/general/mack/pai-mack-volvo-parts.pdf",
    "FP_Cummins": "https://www.drivparts.com/content/dam/marketing/North-America/catalogs/fp-diesel/pdf/fp-diesel-cummins-engines.pdf",
    "FP_Caterpillar": "https://www.drivparts.com/content/dam/marketing/North-America/catalogs/fp-diesel/pdf/fp-diesel-caterpillar-engines.pdf",
    "FP_Detroit": "https://www.dieselduck.info/historical/01%20diesel%20engine/detroit%20diesel/_docs/Detroit%20Diesel%20%28all%29%20FP%20Parts%20manual.pdf",
    "FP_International": "https://www.drivparts.com/content/dam/marketing/emea/fmmp/brands/catalogues/fp-diesel-international-navistar-engines.pdf",
    "Nelson_Exhaust": "https://nelsonexhaust.com.au/files/File/Nelson%20old%20catalogue.pdf",
    "FortPro_HeavyDuty": "https://www.fortpro.com/images/uploaded/HEAVY_DUTY_PARTS.pdf",
    "FortPro_Lighting": "https://www.fortpro.com/images/uploaded/LIGHTING_2020.pdf",
    "Velvac": "https://www.velvac.com/sites/default/files/velvac_catalog_2016.pdf",
    "Leaf_Springs": "https://springer-parts.com/OMK_Springs_catalog-2024_EN.pdf",
    "Stemco_Gaff": "https://www.stemco.com/wp-content/uploads/2020/07/STEMCO_GAFF-Catalog.pdf"
}

SAVE_DIR = Path("pdf_catalogs")
SAVE_DIR.mkdir(exist_ok=True)

DB_PATH = Path("etl_output/parts_staging.sqlite")
DB_PATH.parent.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------------
# DATABASE SETUP
# -----------------------------
def init_db(db_path=DB_PATH):
    if Path(db_path).exists():
        Path(db_path).unlink()

    conn = sqlite3.connect(db_path)

    conn.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA foreign_keys=ON;

    CREATE TABLE IF NOT EXISTS source_documents (
        doc_id        INTEGER PRIMARY KEY,
        source_key    TEXT NOT NULL UNIQUE,
        url           TEXT,
        filename      TEXT NOT NULL,
        brand         TEXT,
        category_hint TEXT,
        downloaded_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS raw_lines (
        line_id   INTEGER PRIMARY KEY,
        doc_id    INTEGER NOT NULL,
        page      INTEGER NOT NULL,
        text      TEXT NOT NULL,
        FOREIGN KEY (doc_id) REFERENCES source_documents(doc_id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_raw_lines_doc ON raw_lines(doc_id);
    CREATE INDEX IF NOT EXISTS idx_raw_lines_page ON raw_lines(page);

    CREATE VIRTUAL TABLE IF NOT EXISTS raw_lines_fts USING fts5(
        text,
        doc_id UNINDEXED,
        page UNINDEXED,
        content='raw_lines',
        content_rowid='line_id',
        tokenize='porter'
    );

    CREATE TABLE IF NOT EXISTS parts_candidates (
        part_id     INTEGER PRIMARY KEY,
        doc_id      INTEGER NOT NULL,
        page        INTEGER,
        part_number TEXT NOT NULL,
        description TEXT,
        category    TEXT,
        confidence  REAL,
        raw_text    TEXT,
        UNIQUE(part_number, description) ON CONFLICT IGNORE,
        FOREIGN KEY (doc_id) REFERENCES source_documents(doc_id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_parts_pn ON parts_candidates(part_number);
    CREATE INDEX IF NOT EXISTS idx_parts_cat ON parts_candidates(category);
    """)

    return conn

# -----------------------------
# PDF DOWNLOAD
# -----------------------------
def download_pdfs():
    for name, url in PDF_URLS.items():
        file_path = SAVE_DIR / f"{name}.pdf"
        if file_path.exists():
            logging.info(f"[SKIP] Already downloaded: {file_path.name}")
            continue
        try:
            logging.info(f"[DOWNLOAD] {name} from {url}")
            r = requests.get(url, headers=HEADERS, stream=True, timeout=30)
            r.raise_for_status()

            if "application/pdf" not in r.headers.get("Content-Type", ""):
                logging.warning(f"[SKIP] {name} is not a PDF")
                continue

            with open(file_path, "wb") as f:
                f.write(r.content)
        except Exception as e:
            logging.error(f"[ERROR] Could not download {name}: {e}")

# -----------------------------
# DATABASE HELPERS
# -----------------------------
def upsert_source(conn, source_key, url, filename, brand=None, category_hint=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO source_documents (source_key, url, filename, brand, category_hint)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            url=excluded.url,
            filename=excluded.filename,
            brand=excluded.brand,
            category_hint=excluded.category_hint
    """, (source_key, url, filename, brand, category_hint))
    conn.commit()
    return cur.lastrowid or cur.execute("SELECT doc_id FROM source_documents WHERE source_key=?", (source_key,)).fetchone()[0]

def insert_raw_lines(conn, doc_id, page, lines):
    cur = conn.cursor()
    cur.executemany("INSERT INTO raw_lines (doc_id, page, text) VALUES (?, ?, ?)",
                    [(doc_id, page, line) for line in lines])
    conn.commit()

def insert_candidates(conn, candidates):
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO parts_candidates (doc_id, page, part_number, description, category, confidence, raw_text)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, candidates)
    conn.commit()

# -----------------------------
# PDF TEXT EXTRACTION
# -----------------------------
def extract_pdf_text(pdf_path):
    extracted = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text()
                if not text:
                    continue
                lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
                extracted[page_num] = lines
    except Exception as e:
        logging.error(f"[ERROR] Failed to read {pdf_path.name}: {e}")
    return extracted

# -----------------------------
# PART NUMBER PARSING
# -----------------------------
PART_PATTERN = re.compile(r"\b([A-Z0-9-]{4,})\b")

def detect_part_candidates(doc_id, page, lines):
    candidates = []
    for line in lines:
        matches = PART_PATTERN.findall(line)
        for match in matches:
            candidates.append((doc_id, page, match, line, None, 0.8, line))
    return candidates

# -----------------------------
# PIPELINE
# -----------------------------
def run_pipeline(limit=None):
    conn = init_db(DB_PATH)

    # Step 1: Download PDFs
    download_pdfs()

    # Step 2: Process PDFs
    for idx, (source_key, url) in enumerate(PDF_URLS.items(), start=1):
        if limit and idx > limit:
            break
        pdf_path = SAVE_DIR / f"{source_key}.pdf"
        if not pdf_path.exists():
            logging.warning(f"[MISSING] Skipping {source_key}, not downloaded")
            continue

        logging.info(f"[EXTRACT] {pdf_path.name}")
        doc_id = upsert_source(conn, source_key, url, pdf_path.name)

        pages = extract_pdf_text(pdf_path)
        for page_num, lines in pages.items():
            insert_raw_lines(conn, doc_id, page_num, lines)
            candidates = detect_part_candidates(doc_id, page_num, lines)
            if candidates:
                insert_candidates(conn, candidates)

    conn.close()
    logging.info(f"[SUCCESS] Pipeline finished. Database at {DB_PATH}")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Limit number of PDFs to process")
    args = parser.parse_args()

    run_pipeline(limit=args.limit)
