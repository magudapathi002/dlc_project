import requests
import datetime
import os
from tabula.io import read_pdf
import pandas as pd
import json
import logging
from django.core.management.base import BaseCommand, CommandError
from ...models import Srldc2AData, Srldc2CData
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from urllib3.poolmanager import PoolManager


class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = create_urllib3_context()
        ctx.load_default_certs()
        # Enable "Legacy Server Connect" (0x4) to allow unsafe renegotiation
        ctx.options |= 0x4
        # (Optional) Lower security level to allow older ciphers often used by gov sites
        try:
            ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        except Exception:
            pass

        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx
        )

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Date for which to run the report, format: DD-MM-YYYY',
            required=False
        )

    def write(self, message, level='info'):
        self.stdout.write(message)
        if level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'srldc.log')
        self.logger = logging.getLogger('srldc_logger')
        self.logger.setLevel(logging.INFO)
        if not self.logger.hasHandlers():
            handler = logging.FileHandler(log_file, encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    help = 'Download today\'s SRLDC report and extract tables 2(A) and 2(C) to a single JSON file and save to DB'

    # List of expected state names for validation
    SOUTH_INDIAN_STATES = [
        "ANDHRA PRADESH", "KARNATAKA", "KERALA", "PONDICHERRY", "TAMILNADU", "TELANGANA", "REGION"
    ]
    # For Table 2C, states might be abbreviated or slightly different
    SOUTH_INDIAN_STATES_2C = [
        "AP", "KAR", "KER", "PONDY", "TN", "TG", "REGION"
    ]

    def extract_subtable_by_markers(self, df, start_marker, end_marker=None, header_row_count=0, debug_table_name="Unknown Table"):
        """
        Extracts a sub-table from a DataFrame based on start and optional end markers.
        Handles multi-level headers by explicitly taking a specified number of rows after the start marker
        as header rows and combining them intelligently.

        Args:
            df (pd.DataFrame): The DataFrame to search within.
            start_marker (str): The regex pattern to identify the start of the sub-table (usually the table title).
            end_marker (str, optional): The regex pattern to identify the end of the sub-table.
                                        If None, extracts from start_marker to the end of the DataFrame.
            header_row_count (int): The number of rows immediately following the start_marker (or actual data start)
                                    that constitute the header. These rows will be combined to form column names.
            debug_table_name (str): A name for the table being processed, used in debug prints.

        Returns:
            tuple: (pd.DataFrame or None, list or None): The extracted sub-table and its column names,
            or (None, None) if the start marker is not found.
        """
        start_idx = None
        end_idx = None
        new_columns = None

        for i, row in df.iterrows():
            row_str_series = row.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
            if row_str_series.str.contains(start_marker, regex=True, na=False, case=False).any():
                start_idx = i
                break

        if start_idx is None:
            self.write(self.style.WARNING(f"⚠️ Start marker '{start_marker}' not found for {debug_table_name}."), level='warning')
            return None, None

        if end_marker:
            for i in range(start_idx + 1, len(df)):
                row_str_series = df.iloc[i].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
                if row_str_series.str.contains(end_marker, regex=True, na=False, case=False).any():
                    end_idx = i
                    break

        if end_idx is not None:
            raw_sub_df = df.iloc[start_idx:end_idx].copy().reset_index(drop=True)
        else:
            raw_sub_df = df.iloc[start_idx:].copy().reset_index(drop=True)

        data_start_row_in_raw_sub_df = 1 + header_row_count

        if header_row_count > 0 and len(raw_sub_df) >= data_start_row_in_raw_sub_df:
            headers_df = raw_sub_df.iloc[1 : data_start_row_in_raw_sub_df]
            new_columns = []
            if debug_table_name == "Table 2(A)":
                new_columns = [
                    'STATE',
                    'THERMAL',
                    'HYDRO',
                    'GAS/DIESEL/NAPTHA',    
                    'WIND',
                    'SOLAR',
                    'OTHERS',
                    'Net SCH (Net Mu)',
                    'Drawal (Net Mu)',
                    'UI (Net Mu)',
                    'Availability (Net MU)',
                    'Demand Met (Net MU)',
                    'Shortage # (Net MU)'
                ]
            elif debug_table_name == "Table 2(C)":
                new_columns = [
                    'State',
                    'Maximum Demand Met of the day',
                    'Time',
                    'Shortage during maximum demand',
                    'Requirement at maximum demand',
                    'Maximum requirement of the day',
                    'Time.1',
                    'Shortage during maximum requirement',
                    'Demand Met at maximum Requirement',
                    'Min Demand Met',  # This will map to ace_min
                    'Time.2',          # This will map to time_ace_min
                    'ACE_MAX',
                    'Time.3',
                    # Removed 'ACE_MIN' and 'Time.4' as they don't exist in extracted DF
                ]
            else:
                self.write(self.style.WARNING(f"⚠️ Custom header combination logic not defined for {debug_table_name}. Falling back to generic combination."), level='warning')
                raw_top_header = headers_df.iloc[0].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna('')
                raw_bottom_header = headers_df.iloc[1].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna('')
                for idx in range(raw_top_header.shape[0]):
                    t_col = raw_top_header.iloc[idx].strip()
                    b_col = raw_bottom_header.iloc[idx].strip()
                    if not t_col and not b_col:
                        new_columns.append(f"Unnamed_{idx}")
                    elif not b_col:
                        new_columns.append(t_col)
                    elif not t_col:
                        new_columns.append(b_col)
                    elif not b_col.startswith(t_col):
                        new_columns.append(f"{t_col} {b_col}".strip())
                    else:
                        new_columns.append(b_col)

            if new_columns is not None:
                sub_df_data = raw_sub_df.iloc[data_start_row_in_raw_sub_df:].copy()
                sub_df_data = sub_df_data.reindex(columns=list(sub_df_data.columns) + [col for col in new_columns if col not in sub_df_data.columns])
                sub_df_data = sub_df_data.iloc[:, :len(new_columns)]
                sub_df_data.columns = new_columns
                sub_df_data = sub_df_data.loc[:, ~sub_df_data.columns.duplicated(keep='first')]
                sub_df_data.columns = sub_df_data.columns.astype(str).str.strip()
                sub_df_data.columns = sub_df_data.columns.str.replace(r'\s*\r\s*', ' ', regex=True).str.strip()
                sub_df_data = sub_df_data.dropna(axis=0, how='all')
                return sub_df_data.dropna(axis=1, how='all'), new_columns
            else:
                return raw_sub_df.iloc[1:].dropna(axis=1, how='all'), None
        else:
            return raw_sub_df.iloc[1:].dropna(axis=1, how='all'), None

    def _safe_float(self, value):
        if pd.isna(value) or value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if ':' in value:
                return None
            value = value.replace(',', '')
            if not value or value.lower() in ['n/a', '-', 'null', 'nan', 'na', '']:
                return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_string(self, value):
        if pd.isna(value) or value is None:
            return None
        s_val = str(value).strip()
        if s_val.lower() == 'nan' or s_val == '':
            return None
        s_val = s_val.replace('\r', ' ')
        return s_val

    def extract_tables_from_pdf(self, pdf_path, output_dir, report_date):
        self.logger.info("🔍 Extracting tables from PDF...")

        try:
            tables = read_pdf(
                pdf_path,
                pages='all',
                multiple_tables=True,
                pandas_options={'header': None},
                lattice=True
            )
        except Exception as e:
            raise CommandError(f"❌ Tabula extraction failed: {e}")

        if not tables:
            raise CommandError("❌ No tables found in the PDF.")

        self.write(self.style.SUCCESS(f"✅ Found {len(tables)} tables."))
        self.logger.info(f"✅ Found {len(tables)} tables.")

        all_content_df = pd.concat(tables, ignore_index=True)
        all_content_df_cleaned = all_content_df.dropna(axis=0, how='all')

        combined_json_data = {}

        # --- Extract Table 2(A) ---
        sub_2A, headers_2A = self.extract_subtable_by_markers(
            all_content_df_cleaned,
            start_marker=r".*2\s*\(A\)State['’]?s\s*Load\s*Deails\s*\(At\s*State\s*Periphery\)\s*in\s*MUs.*",
            end_marker=r".*2\s*\(B\)\s*State['’]?s\s*Demand\s*Met\s*in\s*MWs\s*and\s*day\s*energy\s*forecast\s*and\s*deviation\s*particulars.*",
            header_row_count=2,
            debug_table_name="Table 2(A)"
        )

        if sub_2A is not None:
            column_mapping_2A = {
                'STATE': 'state',
                'THERMAL': 'thermal',
                'HYDRO': 'hydro',
                'GAS/DIESEL/NAPTHA': 'gas_naptha_diesel',
                'SOLAR': 'solar',
                'WIND': 'wind',
                'OTHERS': 'others',
                'Net SCH (Net Mu)': 'net_sch',
                'Drawal (Net Mu)': 'drawal',
                'UI (Net Mu)': 'ui',
                'Availability (Net MU)': 'availability',
                'Demand Met (Net MU)': 'demand_met',
                'Shortage # (Net MU)': 'shortage',
            }
            sub_2A_renamed = sub_2A.rename(columns={k: v for k, v in column_mapping_2A.items() if k in sub_2A.columns})
            self.write(f"Renamed columns: {sub_2A_renamed.columns.tolist()}")
            self.logger.info(f"Renamed columns: {sub_2A_renamed.columns.tolist()}")
            self.write(f"Shape after rename: {sub_2A_renamed.shape}")
            self.logger.info(f"Shape after rename: {sub_2A_renamed.shape}")
            if 'state' in sub_2A_renamed.columns:
                normalized_states = [s.strip().upper() for s in self.SOUTH_INDIAN_STATES]
                sub_2A_filtered = sub_2A_renamed[
                    sub_2A_renamed['state'].astype(str)
                                         .str.strip()
                                         .str.upper()
                                         .str.replace(r'\s+', ' ', regex=True)
                                         .str.replace('–', '-', regex=False)
                                         .isin(normalized_states)
                ].copy()

                if sub_2A_filtered.empty and not sub_2A_renamed.empty:
                    self.write(self.style.WARNING("⚠️ Exact state name matching failed for Table 2A. Attempting a more lenient match."), level='warning')
                    sub_2A_filtered = sub_2A_renamed[
                        sub_2A_renamed['state'].astype(str)
                                             .str.strip()
                                             .str.upper()
                                             .str.contains('ANDHRA PRADESH|KARNATAKA|KERALA|PONDICHERRY|TAMILNADU|TELANGANA|REGION', case=False, na=False)
                    ].copy()
                self.write(f"States found for Table 2A after filtering: {sub_2A_filtered['state'].tolist()}")
                self.logger.info(f"States found for Table 2A after filtering: {sub_2A_filtered['state'].tolist()}")
            else:
                self.write(self.style.WARNING("⚠️ 'state' column not found in Table 2(A) after rename. Skipping row filtering."), level='warning')
                sub_2A_filtered = sub_2A_renamed.copy()

            model_fields_2A = list(column_mapping_2A.values())
            sub_2A_final = sub_2A_filtered[[col for col in model_fields_2A if col in sub_2A_filtered.columns]]
            sub_2A_final = sub_2A_final.dropna(subset=['state']).copy()
            combined_json_data['srldc_table_2A'] = sub_2A_final.to_dict(orient='records')
            self.write(self.style.SUCCESS(f"✅ Table 2(A) extracted for combined JSON."))

            for index, row_data in sub_2A_final.iterrows():
                state_name = self._safe_string(row_data.get('state'))
                if state_name:
                    try:
                        obj, created = Srldc2AData.objects.update_or_create(
                            report_date=report_date,
                            state=state_name,
                            defaults={
                                'thermal': self._safe_float(row_data.get('thermal')),
                                'hydro': self._safe_float(row_data.get('hydro')),
                                'gas_naptha_diesel': self._safe_float(row_data.get('gas_naptha_diesel')),
                                'solar': self._safe_float(row_data.get('solar')),
                                'wind': self._safe_float(row_data.get('wind')),
                                'others': self._safe_float(row_data.get('others')),
                                # 'total': self._safe_float(row_data.get('total')),
                                'net_sch': self._safe_float(row_data.get('net_sch')),
                                'drawal': self._safe_float(row_data.get('drawal')),
                                'ui': self._safe_float(row_data.get('ui')),
                                'availability': self._safe_float(row_data.get('availability')),
                                'demand_met': self._safe_float(row_data.get('demand_met')),
                                'shortage': self._safe_float(row_data.get('shortage')),
                            }
                        )
                        if created:
                            self.write(self.style.SUCCESS(f"➕ Created Table 2A entry for {report_date} - {state_name}"))
                            self.logger.info(f"➕ Created Table 2A entry for {report_date} - {state_name}")
                        else:
                            self.write(self.style.SUCCESS(f"🔄 Updated Table 2A entry for {report_date} - {state_name}"))
                            self.logger.info(f"🔄 Updated Table 2A entry for {report_date} - {state_name}")
                    except Exception as e:
                        self.write(self.style.ERROR(f"❌ Error saving Table 2A row to DB (State: {state_name}): {e}"), level='error')
                        self.logger.error(f"❌ Error saving Table 2A row to DB (State: {state_name}): {e}")
            self.write(self.style.SUCCESS(f"✅ Table 2(A) data saved to database."))
            self.logger.info(f"✅ Table 2(A) data saved to database.")
        else:
            self.write(self.style.WARNING("⚠️ Table 2(A) not found or extraction failed."), level='warning')

        # --- Extract Table 2(C) ---
        sub_2C, headers_2C = self.extract_subtable_by_markers(
            all_content_df_cleaned,
            start_marker=r"2\s*\(C\)\s*State's\s*Demand\s*Met\s*in\s*MWs.*",
            end_marker=r"3\s*\(A\)\s*StateEntities\s*Generation:",
            header_row_count=2,
            debug_table_name="Table 2(C)"
        )
        if sub_2C is not None:
            self.write("--- RAW DataFrame for Table 2(C) before renaming ---")
            self.logger.info("--- RAW DataFrame for Table 2(C) before renaming ---")
            self.write(str(sub_2C))
            self.logger.info(str(sub_2C))
            self.write("-----------------------------------------------------")
            self.logger.info("-----------------------------------------------------")

            # Debug lines still here (optional)
            self.write("Raw columns in Table 2(C): " + str(sub_2C.columns.tolist()))
            self.logger.info("Raw columns in Table 2(C): " + str(sub_2C.columns.tolist()))
            if not sub_2C.empty:
                self.write("Sample first row data in Table 2(C):")
                self.logger.info("Sample first row data in Table 2(C):")
                sample_row = sub_2C.iloc[0].to_dict()
                for k, v in sample_row.items():
                    self.write(f"  Column: '{k}' => Value: '{v}'")
                    self.logger.info(f"  Column: '{k}' => Value: '{v}'")
            else:
                self.stdout.write("Table 2(C) extracted DataFrame is empty.")

            # UPDATED mapping here with corrected column mapping
            column_mapping_2C = {
                'State': 'state',
                'Maximum Demand Met of the day': 'max_demand',
                'Time': 'time',
                'Shortage during maximum demand': 'shortage_max_demand',
                'Requirement at maximum demand': 'req_max_demand',
                'Demand Met at maximum Requirement': 'demand_max_req',
                'Time.1': 'time_max_req',
                'Shortage during maximum requirement': 'shortage_max_req',
                'Maximum requirement of the day': 'max_req_day',
                'Min Demand Met': 'ace_min',      # Corrected mapping
                'Time.2': 'time_ace_min',         # Corrected mapping
                'ACE_MAX': 'ace_max',
                'Time.3': 'time_ace_max',
                # Removed ACE_MIN and Time.4 from mapping, do not exist in extracted data
            }
            sub_2C_renamed = sub_2C.rename(columns={k: v for k, v in column_mapping_2C.items() if k in sub_2C.columns})
            self.stdout.write(f"Columns present in Table 2C after renaming: {sub_2C_renamed.columns.tolist()}")
            self.logger.info(f"Columns present in Table 2C after renaming: {sub_2C_renamed.columns.tolist()}")
            if 'state' in sub_2C_renamed.columns:
                normalized_states_2C = [s.strip().upper() for s in self.SOUTH_INDIAN_STATES_2C]
                sub_2C_filtered = sub_2C_renamed[
                    sub_2C_renamed['state'].astype(str)
                                         .str.strip()
                                         .str.upper()
                                         .str.replace(r'\s+', ' ', regex=True)
                                         .str.replace('–', '-', regex=False)
                                         .isin(normalized_states_2C)
                ].copy()

                if sub_2C_filtered.empty and not sub_2C_renamed.empty:
                    self.stdout.write(self.style.WARNING("⚠️ Exact state name matching failed for Table 2C. Attempting a more lenient match."))
                    sub_2C_filtered = sub_2C_renamed[
                        sub_2C_renamed['state'].astype(str)
                                             .str.strip()
                                             .str.upper()
                                             .str.contains('AP|KAR|KER|PONDY|TN|TG|REGION', case=False, na=False)
                    ].copy()
                self.stdout.write(f"States found for Table 2C after filtering: {sub_2C_filtered['state'].tolist()}")
                self.logger.info(f"States found for Table 2C after filtering: {sub_2C_filtered['state'].tolist()}")
            else:
                self.stdout.write(self.style.WARNING("⚠️ 'state' column not found in Table 2(C) after rename. Skipping row filtering."))
                sub_2C_filtered = sub_2C_renamed.copy()

            model_fields_2C = list(column_mapping_2C.values())
            sub_2C_final = sub_2C_filtered[[col for col in model_fields_2C if col in sub_2C_filtered.columns]]
            sub_2C_final = sub_2C_final.dropna(subset=['state']).copy()

            combined_json_data['srldc_table_2C'] = sub_2C_final.to_dict(orient='records')
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(C) extracted for combined JSON."))

            for index, row_data in sub_2C_final.iterrows():
                state_name = self._safe_string(row_data.get('state'))
                if state_name:
                    try:
                        ace_min_val = None
                        time_ace_min_val = None

                        if 'ace_min' in sub_2C_final.columns:
                            ace_min_val = self._safe_float(row_data.get('ace_min'))

                        if 'time_ace_min' in sub_2C_final.columns:
                            time_ace_min_val = self._safe_string(row_data.get('time_ace_min'))

                        obj, created = Srldc2CData.objects.update_or_create(
                            report_date=report_date,
                            state=state_name,
                            defaults={
                                'max_demand': self._safe_float(row_data.get('max_demand')),
                                'time': self._safe_string(row_data.get('time')),
                                'shortage_max_demand': self._safe_float(row_data.get('shortage_max_demand')),
                                'req_max_demand': self._safe_float(row_data.get('req_max_demand')),
                                'demand_max_req': self._safe_float(row_data.get('demand_max_req')),
                                'max_req_day': self._safe_float(row_data.get('max_req_day')),
                                'time_max_req': self._safe_string(row_data.get('time_max_requirement')),
                                'shortage_max_req': self._safe_float(row_data.get('shortage_max_req')),
                                'ace_max': self._safe_float(row_data.get('ace_max')),
                                'time_ace_max': self._safe_string(row_data.get('time_ace_max')),
                                'ace_min': ace_min_val,
                                'time_ace_min': time_ace_min_val,
                            }
                        )
                        if created:
                            self.stdout.write(self.style.SUCCESS(f"➕ Created Table 2C entry for {report_date} - {state_name}"))
                            self.logger.info(f"➕ Created Table 2C entry for {report_date} - {state_name}")
                        else:
                            self.stdout.write(self.style.SUCCESS(f"🔄 Updated Table 2C entry for {report_date} - {state_name}"))
                            self.logger.info(f"🔄 Updated Table 2C entry for {report_date} - {state_name}")
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"❌ Error saving Table 2C row to DB (State: {state_name}): {e}"))
                        self.logger.error(f"❌ Error saving Table 2C row to DB (State: {state_name}): {e}")
            
            # ------- Print ace_min and time_ace_min for all states as aligned table -------
            self.stdout.write(self.style.HTTP_INFO("\n--- ACE MIN and Time for Each State (Table 2C) ---"))
            self.stdout.write(f"{'STATE':<12} | {'ACE MIN':>10} | {'TIME':>8}")
            self.stdout.write("-" * 36)
            for index, row_data in sub_2C_final.iterrows():
                state_name = self._safe_string(row_data.get('state')) or '-'
                ace_min_val = self._safe_float(row_data.get('ace_min')) if 'ace_min' in sub_2C_final.columns else None
                time_ace_min_val = self._safe_string(row_data.get('time_ace_min')) if 'time_ace_min' in sub_2C_final.columns else None
                ace_min_str = f"{ace_min_val:.2f}" if ace_min_val is not None else "-"
                time_str = time_ace_min_val or "-"
                self.stdout.write(f"{state_name:<12} | {ace_min_str:>10} | {time_str:>8}")
            # -------------------------------------------------------------------

            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(C) data saved to database."))
            self.logger.info(f"✅ Table 2(C) data saved to database.")
        else:
            self.stdout.write(self.style.WARNING("⚠️ Table 2(C) not found or extraction failed."))

        if combined_json_data:
            # Try to derive JSON filename from folder name (to match PDF naming derived from folder)
            folder_basename = os.path.basename(output_dir).replace('report_', '')  # e.g., '2025-11-11_10-07-00'
            folder_date_part = folder_basename.split('_')[0] if folder_basename else None    # '2025-11-11'
            try:
                if folder_date_part:
                    json_date_str = datetime.datetime.strptime(folder_date_part, '%Y-%m-%d').strftime('%d%m%Y')
                else:
                    json_date_str = report_date.strftime('%d%m%Y')
            except Exception:
                # fallback: if report_date isn't a date object convert to string and try to parse
                try:
                    json_date_str = datetime.datetime.fromisoformat(str(report_date)).strftime('%d%m%Y')
                except Exception:
                    json_date_str = datetime.datetime.now().strftime('%d%m%Y')

            combined_json_path = os.path.join(output_dir, f'srldc_{json_date_str}.json')
            with open(combined_json_path, 'w', encoding='utf-8') as f:
                json.dump(combined_json_data, f, indent=4, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f"✅ Combined tables saved to: {combined_json_path}"))
            self.logger.info(f"✅ Combined tables saved to: {combined_json_path}")
        else:
            self.stdout.write(self.style.WARNING("⚠️ No tables were successfully extracted to create a combined JSON file."))
            self.logger.warning("⚠️ No tables were successfully extracted to create a combined JSON file.")

    def download_latest_srldc_pdf(self, base_url="https://www.srldc.in/var/ftp/reports/psp/", base_download_dir="downloads", given_date=None):
        project_name = "SRLDC"  # Add project name here

        base_download_dir = os.path.join(base_download_dir, project_name)
        os.makedirs(base_download_dir, exist_ok=True)

        pdf_path = None
        report_date = None
        report_dir = None

        IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

        # Convert given_date if provided, else use current IST datetime
        if given_date:
            today = datetime.datetime.strptime(given_date, "%Y-%m-%d").replace(tzinfo=IST) - datetime.timedelta(days=1)
        else:
            today = datetime.datetime.now(datetime.timezone.utc).astimezone(IST) - datetime.timedelta(days=1)

        print("qwerty", today)
        dates_to_try = [today, today - datetime.timedelta(days=1)]
        print("Dates to try for downloading report:", dates_to_try)

        current_date = today
        # for current_date in dates_to_try:
        year = current_date.year
        month_abbr = current_date.strftime('%b').capitalize()
        day = current_date.day

        directory_path_on_server = f"{year}/{month_abbr}{str(year)[-2:]}/"
        file_name_on_server = f"{day:02d}-{current_date.month:02d}-{year}-psp.pdf"
        print(f"Trying to download report for date: {file_name_on_server}")

        full_url = f"{base_url}{directory_path_on_server}{file_name_on_server}"

        # Always include time for folder naming, even if date is provided
        if given_date:
            now_str = f"{given_date}_{datetime.datetime.now().strftime('%H-%M-%S')}"
        else:
            now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Folder name uses now_str (timestamped)
        report_dir = os.path.join(base_download_dir, f"report_{now_str}")
        os.makedirs(report_dir, exist_ok=True)
        self.stdout.write(f"📁 Checking/Created report directory: {report_dir}")

        # --- Rename behavior based on folder timestamp while keeping original logic ---
        # Use the folder date (YYYY-MM-DD from now_str) to create file date in DDMMYYYY format
        folder_date_part = now_str.split('_')[0] if now_str else None  # '2025-11-11'
        try:
            if folder_date_part:
                file_date_str = datetime.datetime.strptime(folder_date_part, '%Y-%m-%d').strftime('%d%m%Y')
            else:
                # fallback to current_date (original logic)
                file_date_str = current_date.strftime('%d%m%Y')
        except Exception:
            file_date_str = current_date.strftime('%d%m%Y')

        local_pdf_filename = f"srldc_{file_date_str}.pdf"
        local_file_path = os.path.join(report_dir, local_pdf_filename)

        if os.path.exists(local_file_path):
            self.stdout.write(self.style.NOTICE(f"📄 PDF already exists locally for {current_date.strftime('%d-%m-%Y')} at {local_file_path}. Skipping download."))
            self.logger.info(f"📄 PDF already exists locally for {current_date.strftime('%d-%m-%Y')} at {local_file_path}. Skipping download.")
            pdf_path = local_file_path
            report_date = current_date.date()
            return pdf_path, report_date, report_dir

        self.stdout.write(f"🌐 Attempting to download from: {full_url}")
        self.logger.info(f"🌐 Attempting to download from: {full_url}")

        try:
            session = requests.Session()
            session.mount('https://', LegacySSLAdapter())
            response = session.get(full_url, stream=True)
            response.raise_for_status()
            with open(local_file_path, 'wb') as pdf_file:
                for chunk in response.iter_content(chunk_size=8192):
                    pdf_file.write(chunk)
            self.stdout.write(self.style.SUCCESS(f"✅ Successfully downloaded: {local_pdf_filename} to {report_dir}"))
            self.logger.info(f"✅ Successfully downloaded: {local_pdf_filename} to {report_dir}")
            pdf_path = local_file_path
            report_date = current_date.date()
            return pdf_path, report_date, report_dir

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.stdout.write(self.style.WARNING(f"⚠️ File not found for {current_date.strftime('%d-%m-%Y')} at {full_url}. Trying next date if available."))
                self.logger.warning(f"⚠️ File not found for {current_date.strftime('%d-%m-%Y')} at {full_url}. Trying next date if available.")
                if os.path.exists(report_dir) and not os.listdir(report_dir):
                    os.rmdir(report_dir)
            else:
                self.stdout.write(self.style.ERROR(f"❌ HTTP Error {e.response.status_code} while downloading {file_name_on_server}: {e}"))
                self.logger.error(f"❌ HTTP Error {e.response.status_code} while downloading {file_name_on_server}: {e}")
        except requests.exceptions.RequestException as e:
            self.stdout.write(self.style.ERROR(f"❌ An unexpected error occurred during download: {e}"))
            self.logger.error(f"❌ An unexpected error occurred during download: {e}")

        self.stdout.write(self.style.ERROR("❌ Failed to download the latest PSP report after trying all attempts."))
        self.logger.error("❌ Failed to download the latest PSP report after trying all attempts.")
        return None, None, None

    def handle(self, *args, **options):
        if "JAVA_HOME" not in os.environ:
            self.stdout.write(self.style.WARNING("JAVA_HOME environment variable not set. tabula-py may fail."))
            self.logger.warning("JAVA_HOME environment variable not set. tabula-py may fail.")

        # Capture the report_date returned by download_latest_srldc_pdf so JSON & DB use the same date
        pdf_path, report_date, report_output_dir = self.download_latest_srldc_pdf(given_date=options.get('date'))

        # If download returned None or failed, warn and exit
        if pdf_path is None:
            self.stdout.write(self.style.WARNING("No PDF report was successfully downloaded or found locally. Exiting."))
            self.logger.warning("No PDF report was successfully downloaded or found locally. Exiting.")
            return

        # Pass the report_date returned by the downloader into extract_tables_from_pdf
        self.extract_tables_from_pdf(pdf_path, report_output_dir, report_date)
        self.stdout.write(self.style.SUCCESS(f"Finished processing. Files saved in: {report_output_dir}"))
        self.logger.info(f"Finished processing. Files saved in: {report_output_dir}")
