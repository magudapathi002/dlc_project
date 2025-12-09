import requests
import datetime
import os
import pandas as pd
import json
import logging
from django.core.management.base import BaseCommand, CommandError
from ...models import Nrldc2AData, Nrldc2CData
from tabula.io import read_pdf
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

class Command(BaseCommand):
    help = 'Download NRLDC report for a specific date (or today if not provided), extract tables 2(A) and 2(C) to a single JSON file and save to DB'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'nrldc.log')

        self.logger = logging.getLogger('nrldc_logger')
        self.logger.setLevel(logging.INFO)

        if not self.logger.hasHandlers():
            fh = logging.FileHandler(log_file, encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    def write(self, message, level='info'):
        self.stdout.write(message)
        if level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)

    # Accept a --date argument from the dashboard / CLI
    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            dest='date',
            help='Target report date to download in YYYY-MM-DD or DD-MM-YYYY format. If omitted, today is used.',
            required=False
        )

    def parse_date_string(self, date_str):
        """Parse incoming date string in common formats to a datetime.date."""
        if not date_str:
            return None
        for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y'):
            try:
                return datetime.datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Unsupported date format: {date_str}. Use YYYY-MM-DD or DD-MM-YYYY.")

    def extract_subtable_by_markers(self, df, start_marker, end_marker=None, header_row_count=0, debug_table_name="Unknown Table"):
        start_idx = None
        end_idx = None

        for i, row in df.iterrows():
            if row.astype(str).str.strip().str.contains(start_marker, regex=True, na=False, case=False).any():
                start_idx = i
                break

        if start_idx is None:
            self.write(self.style.WARNING(f"⚠️ Start marker '{start_marker}' not found for {debug_table_name}."), level='warning')
            return None

        if end_marker:
            for i in range(start_idx + 1, len(df)):
                if df.iloc[i].astype(str).str.strip().str.contains(end_marker, regex=True, na=False, case=False).any():
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
            if header_row_count == 1:
                new_columns = headers_df.iloc[0].astype(str).str.strip().tolist()
            elif header_row_count == 2:
                raw_top_header = headers_df.iloc[0].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna('')
                raw_bottom_header = headers_df.iloc[1].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna('')

                if debug_table_name == "Table 2(A)":
                    new_columns = [
                        'State',
                        'Thermal',
                        'Hydro',
                        'Gas/Naptha/Diesel',
                        'Solar',
                        'Wind',
                        'Others(Biomass/Co-gen etc.)',
                        'Total',
                        'Drawal Sch (Net MU)',
                        'Act Drawal (Net MU)',
                        'UI (Net MU)',
                        'Requirement (Net MU)',
                        'Shortage (Net MU)',
                        'Consumption (Net MU)'
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
                        'Min Demand Met',
                        'Time.2',
                        'ACE_MAX',
                        'ACE_MIN',
                        'Time.3',
                        'Time.4'
                    ]
                else:
                    self.write(self.style.WARNING(f"⚠️ Custom header combination logic not defined for {debug_table_name}. Falling back to generic combination."), level='warning')
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
            else:
                self.write(self.style.WARNING(f"⚠️ Unsupported header_row_count: {header_row_count} for {debug_table_name}. Header processing skipped."), level='warning')
                new_columns = None

            if new_columns is not None:
                expected_data_cols = raw_sub_df.shape[1]
                if len(new_columns) < expected_data_cols:
                    new_columns.extend([f"Unnamed_Col_{i}" for i in range(len(new_columns), expected_data_cols)])
                elif len(new_columns) > expected_data_cols:
                    new_columns = new_columns[:expected_data_cols]

                sub_df_data = raw_sub_df.iloc[data_start_row_in_raw_sub_df:].copy()
                sub_df_data.columns = new_columns
                sub_df_data = sub_df_data.loc[:, ~sub_df_data.columns.duplicated()]
                sub_df_data.columns = sub_df_data.columns.astype(str).str.strip()
                sub_df_data.columns = sub_df_data.columns.str.replace(r'\s*\r\s*', ' ', regex=True).str.strip()

                sub_df_data = sub_df_data.dropna(axis=0, how='all')
                return sub_df_data.dropna(axis=1, how='all')
            else:
                return raw_sub_df.iloc[data_start_row_in_raw_sub_df:].dropna(axis=1, how='all')
        else:
            return raw_sub_df.iloc[1:].dropna(axis=1, how='all')

    def _safe_float(self, value):
        if isinstance(value, str):
            value = value.strip()
            if ':' in value:
                return None
            value = value.replace(',', '')
            if not value or value.lower() in ['n/a', '-', 'null', 'nan']:
                return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_string(self, value):
        if pd.isna(value) or value is None:
            return None
        return str(value).strip() if value is not None else None

    def extract_tables_from_pdf(self, pdf_path, output_dir, report_date):
        self.write("🔍 Extracting tables from PDF...")

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

        all_content_df = pd.DataFrame()
        for df in tables:
            all_content_df = pd.concat([all_content_df, df], ignore_index=True)

        all_content_df_cleaned = all_content_df.dropna(axis=0, how='all')

        combined_json_data = {}

        # Extract Table 2(A)
        sub_2A = self.extract_subtable_by_markers(
            all_content_df_cleaned,
            start_marker=r".*2\s*\(A\)\s*State's\s*Load\s*Deails.*",
            end_marker=r"2\s*\(B\)\s*State\s*Demand\s*Met\s*\(Peak\s*and\s*off-Peak\s*Hrs\)",
            header_row_count=2,
            debug_table_name="Table 2(A)"
        )
        if sub_2A is not None:
            column_mapping_2A = {
                'State': 'state',
                'Thermal': 'thermal',
                'Hydro': 'hydro',
                'Gas/Naptha/Diesel': 'gas_naptha_diesel',
                'Solar': 'solar',
                'Wind': 'wind',
                'Others(Biomass/Co-gen etc.)': 'other_biomass',
                'Total': 'total',
                'Drawal Sch (Net MU)': 'drawal_sch',
                'Act Drawal (Net MU)': 'act_drawal',
                'UI (Net MU)': 'ui',
                'Requirement (Net MU)': 'requirement',
                'Shortage (Net MU)': 'shortage',
                'Consumption (Net MU)': 'consumption',
            }
            sub_2A_renamed = sub_2A.rename(columns=column_mapping_2A)
            sub_2A_filtered = sub_2A_renamed[[col for col in column_mapping_2A.values() if col in sub_2A_renamed.columns]]

            combined_json_data['nrldc_table_2A'] = sub_2A_filtered.to_dict(orient='records')
            self.write(self.style.SUCCESS(f"✅ Table 2(A) extracted for combined JSON."))

            for index, row_data in sub_2A_filtered.iterrows():
                try:
                    obj, created = Nrldc2AData.objects.update_or_create(
                        report_date=report_date,
                        state=self._safe_string(row_data.get('state')),
                        defaults={
                            'thermal': self._safe_float(row_data.get('thermal')),
                            'hydro': self._safe_float(row_data.get('hydro')),
                            'gas_naptha_diesel': self._safe_float(row_data.get('gas_naptha_diesel')),
                            'solar': self._safe_float(row_data.get('solar')),
                            'wind': self._safe_float(row_data.get('wind')),
                            'other_biomass': self._safe_float(row_data.get('other_biomass')),
                            'total': self._safe_float(row_data.get('total')),
                            'drawal_sch': self._safe_float(row_data.get('drawal_sch')),
                            'act_drawal': self._safe_float(row_data.get('act_drawal')),
                            'ui': self._safe_float(row_data.get('ui')),
                            'requirement': self._safe_float(row_data.get('requirement')),
                            'shortage': self._safe_float(row_data.get('shortage')),
                            'consumption': self._safe_float(row_data.get('consumption')),
                        }
                    )
                    if created:
                        self.write(self.style.SUCCESS(f"➕ Created Table 2A entry for {report_date} - {row_data.get('state')}"))
                    else:
                        self.write(self.style.SUCCESS(f"🔄 Updated Table 2A entry for {report_date} - {row_data.get('state')}"))
                except Exception as e:
                    self.write(self.style.ERROR(f"❌ Error saving Table 2A row to DB (State: {row_data.get('state')}): {e}"), level='error')
            self.write(self.style.SUCCESS(f"✅ Table 2(A) data saved to database."))
        else:
            self.write(self.style.WARNING("⚠️ Table 2(A) not found or extraction failed."), level='warning')

        # Extract Table 2(C)
        sub_2C = self.extract_subtable_by_markers(
            all_content_df_cleaned,
            start_marker=r"2\s*\(C\)\s*State's\s*Demand\s*Met\s*in\s*MWs.*",
            end_marker=r"3\s*\(A\)\s*StateEntities\s*Generation:",
            header_row_count=2,
            debug_table_name="Table 2(C)"
        )
        if sub_2C is not None:
            column_mapping_2C = {
                'State': 'state',
                'Maximum Demand Met of the day': 'max_demand',
                'Time': 'time_max',
                'Shortage during maximum demand': 'shortage_during',
                'Requirement at maximum demand': 'req_max_demand',
                'Maximum requirement of the day': 'max_req_day',
                'Time.1': 'time_max_req',
                'Shortage during maximum requirement': 'shortage_max_req',
                'Demand Met at maximum Requirement': 'demand_met_max_req',
                'Min Demand Met': 'min_demand_met',
                'Time.2': 'time_min_demand',
                'ACE_MAX': 'ace_max',
                'ACE_MIN': 'ace_min',
                'Time.3': 'time_ace_max',
                'Time.4': 'time_ace_min'
            }

            sub_2C_renamed = sub_2C.rename(columns=column_mapping_2C)
            sub_2C_filtered = sub_2C_renamed[[col for col in column_mapping_2C.values() if col in sub_2C_renamed.columns]]

            combined_json_data['nrldc_table_2C'] = sub_2C_filtered.to_dict(orient='records')
            self.write(self.style.SUCCESS(f"✅ Table 2(C) extracted for combined JSON."))

            for index, row_data in sub_2C_filtered.iterrows():
                try:
                    obj, created = Nrldc2CData.objects.update_or_create(
                        report_date=report_date,
                        state=self._safe_string(row_data.get('state')),
                        defaults={
                            'max_demand': self._safe_float(row_data.get('max_demand')),
                            'time_max': self._safe_string(row_data.get('time_max')),
                            'shortage_during': self._safe_float(row_data.get('shortage_during')),
                            'req_max_demand': self._safe_float(row_data.get('req_max_demand')),
                            'max_req_day': self._safe_float(row_data.get('max_req_day')),
                            'time_max_req': self._safe_string(row_data.get('time_max_req')),
                            'shortage_max_req': self._safe_float(row_data.get('shortage_max_req')),
                            'demand_met_max_req': self._safe_float(row_data.get('demand_met_max_req')),
                            'min_demand_met': self._safe_float(row_data.get('min_demand_met')),
                            'time_min_demand': self._safe_string(row_data.get('time_min_demand')),
                            'ace_max': self._safe_float(row_data.get('ace_max')),
                            'ace_min': self._safe_float(row_data.get('ace_min')),
                            'time_ace_max': self._safe_string(row_data.get('time_ace_max')),
                            'time_ace_min': self._safe_string(row_data.get('time_ace_min')),
                        }
                    )
                    if created:
                        self.write(self.style.SUCCESS(f"➕ Created Table 2C entry for {report_date} - {row_data.get('state')}"))
                    else:
                        self.write(self.style.SUCCESS(f"🔄 Updated Table 2C entry for {report_date} - {row_data.get('state')}"))
                except Exception as e:
                    self.write(self.style.ERROR(f"❌ Error saving Table 2C row to DB (State: {self._safe_string(row_data.get('state'))}): {e}"), level='error')
            self.write(self.style.SUCCESS(f"✅ Table 2(C) data saved to database."))
        else:
            self.write(self.style.WARNING("⚠️ Table 2(C) not found or extraction failed."), level='warning')

        if combined_json_data:
            # Save JSON file using the passed report_date so it matches the PDF naming
            try:
                json_name = f"nrldc_{report_date.strftime('%d%m%Y')}.json"
            except Exception:
                # Fallback if report_date isn't a date-like object
                json_name = f"nrldc_{datetime.datetime.now().strftime('%d%m%Y')}.json"

            json_path = os.path.join(output_dir, json_name)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(combined_json_data, f, indent=4, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f"✅ Combined tables saved to: {json_path}"))
        else:
            self.write(self.style.WARNING("⚠️ No tables were successfully extracted to create a combined JSON file."), level='warning')

    def handle(self, *args, **options):
        # If dashboard passes --date, parse and use it. Otherwise, use today.
        raw_date = options.get('date')
        try:
            target_date = self.parse_date_string(raw_date) if raw_date else datetime.date.today()-datetime.timedelta(days=1)
        except ValueError as e:
            raise CommandError(str(e))

        # Logging what date we will process
        self.write(self.style.SUCCESS(f"🔔 Requested report date: {target_date}"), level='info')

        project_name = "NRLDC"
        today_str_for_query = target_date.strftime("%Y-%m-%d")

        # If data already exists for requested date, skip.
        if Nrldc2AData.objects.filter(report_date=target_date).exists() or \
           Nrldc2CData.objects.filter(report_date=target_date).exists():
            self.write(self.style.SUCCESS(f"✅ Pass: Report data for {today_str_for_query} already exists in the database. Skipping download and extraction."))
            return

        # Build the metadata URL using the target date
        url = f"https://nrldc.in/get-documents-list/111?start_date={today_str_for_query}&end_date={today_str_for_query}"
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        session = requests.Session()
        session.mount('https://', LegacySSLAdapter())


        # self.write(f"🌐 Fetching NRDC report metadata for {today_str_for_query}...")
        try:
            response = session.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise CommandError(f"❌ Error fetching NRDC metadata: {e}")

        try:
            data = response
        except Exception as e:
            raise CommandError(f"❌ Failed to parse JSON response: {e}")

        if data.get("recordsFiltered", 0) == 0:
            self.write(self.style.WARNING(f"⚠️ No report available for {today_str_for_query}. This might be due to weekends, holidays, or late publishing."), level='warning')
            return

        file_info = data["data"][0]
        file_name = file_info["file_name"]
        title = file_info.get("title", file_name)

        # Compose download_url
        download_url = f"https://nrldc.in/download-file?any=Reports%2FDaily%2FDaily%20PSP%20Report%2F{file_name}"

        # ------------------- CHANGE: Build output_dir using target_date -------------------
        # Use the requested report date in the folder name, and keep a time suffix to avoid collisions.
        date_part = target_date.strftime('%Y-%m-%d')
        time_part = datetime.datetime.now().strftime('%H-%M-%S')
        output_dir = os.path.join("downloads", project_name, f"report_{date_part}_{time_part}")
        os.makedirs(output_dir, exist_ok=True)
        self.write(f"📁 Created output directory: {output_dir}")
        # -------------------------------------------------------------------------------

        # Save to a temporary name, then rename to match requested target_date
        pdf_path = os.path.join(output_dir, f"{title}.pdf")
        self.write(f"⬇️ Attempting to download PDF to: {pdf_path}")

        try:
            pdf_response = session.get(download_url, headers=headers, timeout=60)
            pdf_response.raise_for_status()
            with open(pdf_path, "wb") as f:
                f.write(pdf_response.content)

            # Rename the downloaded PDF to match the target_date
            try:
                new_pdf_name = f"nrldc_{target_date.strftime('%d%m%Y')}.pdf"
                new_pdf_path = os.path.join(output_dir, new_pdf_name)
                os.rename(pdf_path, new_pdf_path)
                pdf_path = new_pdf_path  # keep using same variable name afterwards
            except Exception as e:
                # If rename fails, log a warning but continue (pdf_path remains original)
                self.write(self.style.WARNING(f"⚠️ Failed to rename PDF file: {e}"), level='warning')

            self.write(self.style.SUCCESS(f"✅ Downloaded report to: {pdf_path}"))
        except Exception as e:
            raise CommandError(f"❌ Failed to download PDF: {e}")

        # Pass target_date (a datetime.date) to extract_tables_from_pdf so JSON/DB use same date
        self.extract_tables_from_pdf(pdf_path, output_dir, target_date)
