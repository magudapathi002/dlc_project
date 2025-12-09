import os
import json
import re
import requests
from glob import glob
from datetime import datetime, timedelta

from django.core.management.base import BaseCommand, CommandError

def extract_date_from_filename(filename):
    patterns = [
        (r'(\d{4})-(\d{2})-(\d{2})', (1, 2, 3)),
        (r'(\d{2})-(\d{2})-(\d{4})', (3, 2, 1)),
        (r'daily(\d{2})(\d{2})(\d{2})', (3, 2, 1)),
        (r'(\d{2})(\d{2})(\d{4})', (3, 2, 1)),
    ]
    for pattern, (y_idx, m_idx, d_idx) in patterns:
        match = re.search(pattern, filename)
        if match:
            try:
                day = match.group(d_idx)
                month = match.group(m_idx)
                year = match.group(y_idx)
                if len(year) == 2:
                    year = f'20{year}'
                datetime(int(year), int(month), int(day))
                return f'{year}-{month}-{day}'
            except (ValueError, IndexError):
                continue
    return None

report_dirs = {
    'NRLDC': 'downloads/NRLDC',
    'SRLDC': 'downloads/SRLDC',
    'WRLDC': 'downloads/WRLDC',
    'POSOCO': 'downloads/POSOCO'
}

empty_templates = {
    # ... (this dictionary is unchanged) ...
    'NRLDC': {
        "date": None,
        "nrldc_table_2A": [
            {"state": s["state"], "thermal": None, "hydro": None, "gas_naptha_diesel": None, "solar": None, "wind": None, "other_biomass": None, "total": None, "drawal_sch": None, "act_drawal": None, "ui": None, "requirement": None, "shortage": None, "consumption": None}
            for s in [
                {"state": "PUNJAB"}, {"state": "HARYANA"}, {"state": "RAJASTHAN"}, {"state": "DELHI"}, {"state": "UTTAR PRADESH"}, {"state": "UTTARAKHAND"}, {"state": "HIMACHAL\rPRADESH"}, {"state": "J&K(UT) &\rLadakh(UT)"}, {"state": "CHANDIGARH"}, {"state": "RAILWAYS_NR ISTS"}, {"state": "Region"}
            ]
        ],
        "nrldc_table_2C": [
            {"state": s["state"], "max_demand": None, "time_max": None, "shortage_during": None, "req_max_demand": None, "max_req_day": None, "time_max_req": None, "shortage_max_req": None, "demand_met_max_req": None, "min_demand_met": None, "time_min_demand": None, "ace_max": None, "time_ace_max": None, "ace_min": None, "time_ace_min": None}
            for s in [
                {"state": "PUNJAB"}, {"state": "HARYANA"}, {"state": "RAJASTHAN"}, {"state": "DELHI"}, {"state": "UP"}, {"state": "UTTARAKHA.."}, {"state": "HP"}, {"state": "J&K(UT)&Lad.."}, {"state": "CHANDIGARH"}, {"state": "RAILWAYS_NR\rISTS"}, {"state": "NR"}
            ]
        ]
    },
    'SRLDC': {
        "date": None,
        "srldc_table_2A": [
            {"state": s["state"], "thermal": None, "hydro": None, "gas_naptha_diesel": None, "solar": None, "wind": None, "others": None, "net_sch": None, "drawal": None, "ui": None, "availability": None, "demand_met": None, "shortage": None}
            for s in [
                {"state": "ANDHRA\rPRADESH"}, {"state": "KARNATAKA"}, {"state": "KERALA"}, {"state": "PONDICHERRY"}, {"state": "TAMILNADU"}, {"state": "TELANGANA"}, {"state": "Region"}
            ]
        ],
        "srldc_table_2C": [
            {"state": s["state"], "max_demand": None, "time": None, "shortage_max_demand": None, "req_max_demand": None, "demand_max_req": None, "time_max_req": None, "shortage_max_req": None, "max_req_day": None, "ace_min": None, "time_ace_min": None, "ace_max": None, "time_ace_max": None}
            for s in [
                {"state": "AP"}, {"state": "KAR"}, {"state": "KER"}, {"state": "PONDY"}, {"state": "TN"}, {"state": "TG"}, {"state": "Region"}
            ]
        ]
    },
    'WRLDC': {
        "date": None,
        "wrldc_table_2A": [
            {"state": s["state"], "thermal": None, "hydro": None, "gas": None, "wind": None, "solar": None, "others": None, "total": None, "net_sch": None, "drawal": None, "ui": None, "availability": None, "requirement": None, "shortage": None, "consumption": None}
            for s in [
                {"state": "BALCO"}, {"state": "CHHATTISGARH"}, {"state": "DNHDDPDCL"}, {"state": "AMNSIL"}, {"state": "GOA"}, {"state": "GUJARAT"}, {"state": "MAHARASHTRA"}, {"state": "RIL JAMNAGAR"}, {"state": "Region"}
            ]
        ],
        "wrldc_table_2C": [
            {"state": s["state"], "max_demand_day": None, "time": None, "shortage_max_demand": None, "req_max_demand": None, "ace_max": None, "time_ace_max": None, "ace_min": None, "time_ace_min": None}
            for s in [
                {"state": "AMNSIL"}, {"state": "BALCO"}, {"state": "CHHATTISGARH"}, {"state": "DNHDDPDCL"}, {"state": "GOA"}, {"state": "GUJARAT"}, {"state": "MAHARASHTRA"}, {"state": "RIL JAMNAGAR"}, {"state": "WR"}
            ]
        ]
    },
    "POSOCO": {
        "date": None,
        "posoco_table_a": [{
            "demand_evening_peak": None, "peak_shortage": None, "energy": None, "hydro": None,
            "wind": None, "solar": None, "energy_shortage": None, "max_demand_day": None,
            "time_of_max_demand": None
        }],
        "posoco_table_g": [{
            "coal": None, "lignite": None, "hydro": None, "nuclear": None,
            "gas_naptha_diesel": None, "res_total": None, "total": None
        }]
    }
}


class Command(BaseCommand):
    help = 'Merges the latest JSON reports from all sources and pushes to an API.'

    # NEW: Add the --date argument
    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Merge reports for a specific date in YYYY-MM-DD format. Defaults to today.'
        )

    def handle(self, *args, **options):
        # NEW: Determine the target date dynamically
        date_str_option = options.get('date')
        if date_str_option:
            try:
                # Use the date provided by the user
                target_datetime = datetime.strptime(date_str_option, '%Y-%m-%d')
            except ValueError:
                raise CommandError("Date format is incorrect. Please use YYYY-MM-DD.")
        else:
            # Default to today if no date is provided
            target_datetime = datetime.now()

        # The date for finding report files
        target_date_str = target_datetime.strftime('%Y-%m-%d')
        # The date for the API payload and final report (always the day before the target)
        report_date = (target_datetime - timedelta(days=1)).strftime('%Y-%m-%d')

        BASE_API_URL = "http://172.16.7.118:8003/api/tamilnadu/wind/api.grid.php"
        api_url_with_date = f"{BASE_API_URL}?date={report_date}"
        merged_data = {}

        for region, report_dir in report_dirs.items():
            region_data = None
            try:
                # UPDATED LOGIC: Find the latest subdirectory for the target date
                subdirs_for_date = [
                    d for d in os.listdir(report_dir)
                    if os.path.isdir(os.path.join(report_dir, d)) and d.startswith(f'report_{target_date_str}')
                ]
                
                if subdirs_for_date:
                    # Sort to get the latest directory (e.g., ..._09-31-20 is newer than ..._09-30-26)
                    latest_subdir_name = sorted(subdirs_for_date)[-1]
                    full_subdir = os.path.join(report_dir, latest_subdir_name)
                    
                    # Find the JSON file inside the latest subdirectory
                    json_files = glob(os.path.join(full_subdir, '*.json'))
                    
                    if json_files:
                        json_file_name = json_files[0] # Assume only one JSON per folder
                        try:
                            with open(json_file_name, 'r', encoding='utf-8') as f:
                                data = json
                            
                            inner_data = data.get(region, data)
                            restructured_data = {'date': report_date, **inner_data} # Use report_date
                            
                            # Validate against template
                            template = empty_templates.get(region, {})
                            for table_key, template_value in template.items():
                                if table_key != 'date' and (not restructured_data.get(table_key) or not any(restructured_data.get(table_key))):
                                    self.stdout.write(self.style.WARNING(f"⚠️ Missing or empty table '{table_key}' for {region}, applying empty template."))
                                    restructured_data[table_key] = template_value
                            
                            region_data = restructured_data
                            self.stdout.write(self.style.SUCCESS(f"✅ Merged data for {region} from {json_file_name}"))

                        except Exception as e:
                            self.stdout.write(self.style.ERROR(f"Error reading {json_file_name} for {region}: {e}, using empty template."))
                            region_data = empty_templates.get(region, {})
                    else:
                        self.stdout.write(self.style.WARNING(f"No JSON files found in {full_subdir} for {region}, using empty template."))
                        region_data = empty_templates.get(region, {})
                else:
                    self.stdout.write(self.style.WARNING(f"No subdir for '{target_date_str}' in {report_dir} for {region}, using empty template."))
                    region_data = empty_templates.get(region, {})

            except (FileNotFoundError, NotADirectoryError):
                self.stdout.write(self.style.ERROR(f"Directory not found: {report_dir}, using empty template."))
                region_data = empty_templates.get(region, {})

            # Set the final date for the payload
            if region_data:
                region_data['date'] = report_date
            merged_data[region] = region_data

        # --- API PUSH and SAVE LOCALLY (Unchanged) ---
        headers = {"Content-Type": "application/json"}
        print(merged_data,"asdfgh")
        try:
            print("done")
            pass
            self.stdout.write(f"\nAttempting to push data to: {api_url_with_date}...")
            response = requests.post(api_url_with_date, headers=headers, json=merged_data, timeout=30)
            if response.status_code in [200, 201]:
                self.stdout.write(self.style.SUCCESS(f"✅ Successfully pushed data to API. Status Code: {response.status_code}"))
                self.stdout.write(f"Response text: {response.text}")
            else:
                self.stdout.write(self.style.ERROR(f"Failed to push data. Status Code: {response.status_code}"))
                self.stdout.write(f"Error Response: {response.text}")
        except requests.exceptions.RequestException as e:
            raise CommandError(f"🚨 An error occurred while trying to connect to the API: {e}")

        # -----------------------
        # Save merged JSON locally (+1 day logic for saving)
        # -----------------------
        output_dir = os.path.join('downloads', 'overall_json')
        os.makedirs(output_dir, exist_ok=True)

        # Add +1 day for file naming purpose only
        save_date = (datetime.strptime(report_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        # === CHANGE: filename logic (use +1 day for saving) ===
        if date_str_option:
            filename = f'merged_reports_{save_date}.json'
        else:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            filename = f'merged_reports_{timestamp}.json'

        output_path = os.path.join(output_dir, filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, indent=2, ensure_ascii=False)
        self.stdout.write(self.style.SUCCESS(f"\nMerged latest reports for {report_date} saved to {output_path}"))
