import os
import requests
import pdfplumber
import pandas as pd
from pathlib import Path
import logging

# Suppress noisy PDFMiner warnings
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# -----------------------------
# 1. CONFIGURATION
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
    "Automan_Suspension": "https://www.midwestwheel.com/w/automann-suspension-catalog", 
    "Stemco_Gaff": "https://www.stemco.com/wp-content/uploads/2020/07/STEMCO_GAFF-Catalog.pdf"

}

SAVE_DIR = Path("pdf_catalogs")
SAVE_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

# -----------------------------
# 2. DOWNLOAD PDFs
# -----------------------------
def download_pdfs():
    for name, url in PDF_URLS.items():
        file_path = SAVE_DIR / f"{name}.pdf"
        if file_path.exists():
            print(f"[SKIP] {name} already downloaded.")
            continue
        try:
            print(f"[DOWNLOAD] {name} from {url}")
            r = requests.get(url, headers=HEADERS, stream=True, timeout=30)
            r.raise_for_status()

            # Check file type
            content_type = r.headers.get("Content-Type", "")
            if "application/pdf" not in content_type:
                print(f"[SKIP] {name} is not a PDF (Content-Type={content_type})")
                continue

            with open(file_path, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"[ERROR] Could not download {name}: {e}")

# -----------------------------
# 3. EXTRACT DATA FROM PDF
# -----------------------------
def extract_pdf_text(pdf_path):
    extracted_data = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text()
                if text:
                    for line in text.split("\n"):
                        extracted_data.append({
                            "File": pdf_path.name,
                            "Page": page_num,
                            "Content": line.strip()
                        })
        if not extracted_data:
            print(f"[WARNING] {pdf_path.name} might be image-only (scanned PDF). No text extracted.")
    except Exception as e:
        print(f"[ERROR] Failed to read {pdf_path.name}: {e}")
    return extracted_data

# -----------------------------
# 4. PIPELINE: DOWNLOAD + EXTRACT + SAVE
# -----------------------------
def run_pipeline():
    # Step 1: Download PDFs
    download_pdfs()

    # Step 2: Extract all PDFs into a DataFrame
    all_data = []
    for pdf_file in SAVE_DIR.glob("*.pdf"):
        print(f"[EXTRACT] Reading {pdf_file.name}")
        all_data.extend(extract_pdf_text(pdf_file))

    # Step 3: Save as CSV
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv("extracted_catalog_data.csv", index=False, encoding="utf-8")
        print(f"[SUCCESS] Extracted data saved to extracted_catalog_data.csv")
    else:
        print("[WARNING] No data extracted.")

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    run_pipeline()
