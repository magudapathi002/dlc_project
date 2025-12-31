import pdfplumber
import glob
import os

files = glob.glob("d:\\dlc_project\\downloads\\SRLDC\\**\\*.pdf", recursive=True)
files.sort(key=os.path.getmtime, reverse=True)
pdf_path = files[0]
print(f"Checking: {pdf_path}")

with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        words = page.extract_words()
        heading_top = None
        for w in words:
             if "2(C)" in w["text"]: heading_top = w["top"]; break
        
        if heading_top:
            print(f"Found heading at {heading_top}")
            found_header = False
            for table in page.find_tables():
                if table.bbox[3] > heading_top:
                    print(f"Checking table with bbox {table.bbox}")
                    rows = table.extract()
                    for i, r in enumerate(rows):
                        r_str = " ".join([str(x).upper() for x in r if x])
                        # Look for 2C header signature
                        if ("MAXIMUM" in r_str and "DEMAND" in r_str) or ("DEMAND" in r_str and "MET" in r_str and "ACE" in r_str):
                             print(f"HEADER ROW {i}: {r}")
                             if i+1 < len(rows):
                                 print(f"DATA ROW {i+1}: {rows[i+1]}")
                             if i+2 < len(rows):
                                 print(f"DATA ROW {i+2}: {rows[i+2]}")
                             found_header = True
                    if found_header: break
