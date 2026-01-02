import pdfplumber
import pandas as pd
import re

def clean_cell(cell):
    if cell is None: return ""
    return str(cell).replace("\n", " ").strip()

def extract_table_3B_using_heading(pdf_path):
    tables_dict = {"central_sector": [], "joint_venture": []}
    
    current_section = None 
    start_collecting = False

    with pdfplumber.open(pdf_path) as pdf:
        # 1. FIND HEADING "3(B)"
        heading_page_idx = -1
        heading_top = -1
        
        for p_idx, page in enumerate(pdf.pages):
            text = page.extract_text()
            print(f"--- PAGE {p_idx+1} TEXT START ---")
            print(text[:500] if text else "NO TEXT")
            print("---------------------------------")
            for w in page.extract_words(use_text_flow=True):
                if "3(B)" in w.get("text", "").upper():
                    heading_page_idx = p_idx
                    heading_top = w["top"]
                    print(f"DEBUG: Found 3(B) match in word: {w}")
                    break
            if heading_page_idx != -1: break
            
        if heading_page_idx == -1:
            print("❌ Table 3(B) heading NOT FOUND.")
            return tables_dict

        print(f"📍 Table 3(B) heading FOUND on Page {heading_page_idx+1} at top={heading_top}")

        # 2. EXTRACT TABLES
        for p_idx in range(heading_page_idx, len(pdf.pages)):
            page = pdf.pages[p_idx]
            found_tables = page.find_tables()
            
            for t_obj in found_tables:
                if p_idx == heading_page_idx and t_obj.bbox[1] < heading_top:
                    continue # Skip tables above heading

                rows = t_obj.extract()
                if not rows: continue
                
                cleaned_rows = []
                for r in rows:
                    if r: cleaned_rows.append([clean_cell(c) for c in r])
                if not cleaned_rows: continue
                
                maxcols = max(len(r) for r in cleaned_rows)
                cleaned_rows = [r + [""] * (maxcols - len(r)) for r in cleaned_rows]

                for i, r in enumerate(cleaned_rows):
                    if i > 100: break # Safety limit
                    row_text = " ".join(r).upper()
                    row_text_clean = row_text.replace(" ", "")
                    first_col = r[0].upper().strip()
                    first_col_clean = first_col.replace(" ", "")
                    
                    try:
                        print(f"ROW {i}: {str(r)[:100]}... | Section: {current_section}")
                    except: pass

                    # 1. Detect start of ISGS section
                    if "STATION" in row_text and "CONSTITUENTS" in row_text:
                        print(" -> Skip Header")
                        continue 

                    if "ISGS" == first_col or ("ISGS" in row_text and "TOTAL" not in row_text):
                         current_section = "central_sector"
                         start_collecting = True
                         print(" -> Start ISGS")
                         continue

                    # Improved JV Detection
                    if ("JOINT" in row_text and "VENTURE" in row_text and "TOTAL" not in row_text) or "JOINTVENTURE" in row_text_clean:
                        current_section = "joint_venture"
                        start_collecting = True
                        print(" -> Start JV")
                        continue

                    # STOP CONDITIONS / SWITCH
                    if "TOTAL" in row_text and "ISGS" in row_text:
                        if "central_sector" in tables_dict: tables_dict["central_sector"].append((p_idx+1, 0, r))
                        current_section = "joint_venture" 
                        print(" -> Total ISGS (Switch to JV)")
                        continue 

                    if "TOTAL" in row_text and ("JOINT" in row_text and "VENTURE" in row_text):
                         if "joint_venture" in tables_dict: tables_dict["joint_venture"].append((p_idx+1, 0, r))
                         print(" -> Total JV (STOP)")
                         return tables_dict

                    # STRICT STOP for Renewable/State Sector
                    if (first_col_clean.startswith("4(") or 
                        ("STATE" in first_col and "SECTOR" in row_text) or 
                        "RENEWABLE" in row_text or
                        "SOLAR" in row_text or
                        "WIND" in row_text or
                        "NBUN" in row_text or 
                        "BUN" in row_text or
                        "IPP" in row_text or
                        "INTER-REGIONAL" in row_text or 
                        "VOLTAGEPROFILE" in row_text_clean):
                        print(f" -> STRICT STOP matched: {first_col}")
                        return tables_dict

                    # COLLECT DATA
                    if start_collecting and current_section:
                        if "INST." in row_text and "CAPACITY" in row_text: continue
                        if "MW" in row_text and "PEAK" in row_text: continue
                        
                        tables_dict[current_section].append((p_idx + 1, 0, r))
                        print(" -> Collect Row")
                        
                    # FALLBACK Central Sector
                    if not start_collecting:
                        if ("KUDGI" in first_col or "NEYVELI" in first_col or "NTPC" in first_col) and "SOLAR" not in row_text:
                            current_section = "central_sector"
                            start_collecting = True
                            tables_dict[current_section].append((p_idx + 1, 0, r))
                            print(" -> Fallback Start ISGS")

    return tables_dict


# Run on latest PDF
import glob
import os
files = glob.glob("d:\\dlc_project\\downloads\\SRLDC\\**\\*.pdf", recursive=True)
files.sort(key=os.path.getmtime, reverse=True)
if files:
    latest_pdf = files[0]
    print(f"Processing: {latest_pdf}")
    res = extract_table_3B_using_heading(latest_pdf)
    print(f"Result keys: {res.keys()}")
    print(f"Central Rows: {len(res['central_sector'])}")
    print(f"JV Rows: {len(res['joint_venture'])}")
else:
    print("No PDF found")
