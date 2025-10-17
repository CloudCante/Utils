import requests
import csv
import json
from datetime import datetime
from typing import List, Dict, Optional

# API Configuration
API_BASE_URL = "http://10.23.8.215:5000/api/v1/sql-portal"

class WebRawTimestampsExporter:
    def __init__(self, api_base_url: str = API_BASE_URL):
        self.api_base_url = api_base_url
        self.session = requests.Session()
    
    def get_serial_history(self, serial_numbers: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """
        Get production history for serial numbers from the API
        """
        url = f"{self.api_base_url}/serial-history"
        
        payload = {
            "serialNumbers": serial_numbers
        }
        
        if start_date and end_date:
            payload["startDate"] = start_date
            payload["endDate"] = end_date
        
        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return {"success": False, "error": str(e)}
    
    def process_raw_timestamps(self, serial_number: str, history_data: List[Dict]) -> Dict:
        """
        Process the raw timestamps for a serial number (same logic as original script)
        """
        if not history_data:
            return {
                'serial_number': serial_number,
                'vi1_end': None,
                'vi1_next_station': None,
                'vi1_next_start': None,
                'upgrade_end': None,
                'bbd_assy_station': None,
                'bbd_assy_start': None,
                'bbd_assy_end': None,
                'fla_chiflash_station': None,
                'fla_chiflash_start': None,
                'packing_end': None,
                'shipping_start': None
            }
        
        # Filter only workstation data (we need workstation_name, history_station_start_time, history_station_end_time)
        workstation_data = [
            record for record in history_data 
            if record.get('source') == 'workstation' and record.get('workstation_name')
        ]
        
        if not workstation_data:
            return {
                'serial_number': serial_number,
                'vi1_end': None,
                'vi1_next_station': None,
                'vi1_next_start': None,
                'upgrade_end': None,
                'bbd_assy_station': None,
                'bbd_assy_start': None,
                'bbd_assy_end': None,
                'fla_chiflash_station': None,
                'fla_chiflash_start': None,
                'packing_end': None,
                'shipping_start': None
            }
        
        # Build a dictionary of station times (with lists for multiple occurrences)
        stations = {}
        for record in workstation_data:
            station = record['workstation_name']
            start_time = record.get('history_station_start_time')
            end_time = record.get('history_station_end_time')
            
            if station not in stations:
                stations[station] = []
            stations[station].append({
                'start': start_time,
                'end': end_time
            })
        
        result = {
            'serial_number': serial_number,
            'vi1_end': None,
            'vi1_next_station': None,
            'vi1_next_start': None,
            'upgrade_end': None,
            'bbd_assy_station': None,
            'bbd_assy_start': None,
            'bbd_assy_end': None,
            'fla_chiflash_station': None,
            'fla_chiflash_start': None,
            'packing_end': None,
            'shipping_start': None
        }
        
        # 1. VI1 end time and next station start time
        if 'VI1' in stations:
            vi1_end = stations['VI1'][-1]['end']  # MOST RECENT occurrence
            result['vi1_end'] = vi1_end
            
            # Find the next station after VI1's MOST RECENT occurrence
            next_station_name = None
            next_station_start = None
            
            # Look for Disassembly or UPGRADE only
            candidates = []
            
            if 'Disassembly' in stations:
                for disassembly_time in stations['Disassembly']:
                    if disassembly_time['start'] and vi1_end and disassembly_time['start'] > vi1_end:
                        candidates.append(('Disassembly', disassembly_time['start']))
                        break
            
            if 'UPGRADE' in stations:
                for upgrade_time in stations['UPGRADE']:
                    if upgrade_time['start'] and vi1_end and upgrade_time['start'] > vi1_end:
                        candidates.append(('UPGRADE', upgrade_time['start']))
                        break
            
            # Pick whichever comes first
            if candidates:
                candidates.sort(key=lambda x: x[1])
                next_station_name = candidates[0][0]
                next_station_start = candidates[0][1]
            
            result['vi1_next_station'] = next_station_name
            result['vi1_next_start'] = next_station_start
        
        # 2. Upgrade end time and BBD/ASSY1 start time
        if 'UPGRADE' in stations:
            upgrade_end = stations['UPGRADE'][-1]['end']  # MOST RECENT occurrence
            result['upgrade_end'] = upgrade_end
            
            # Look for BBD OR ASSY1 after UPGRADE, whichever comes first
            candidates = []
            
            if 'BBD' in stations:
                for bbd_time in stations['BBD']:
                    if bbd_time['start'] and upgrade_end and bbd_time['start'] > upgrade_end:
                        candidates.append(('BBD', bbd_time['start'], bbd_time['end']))
                        break
            
            if 'ASSY1' in stations:
                for assy1_time in stations['ASSY1']:
                    if assy1_time['start'] and upgrade_end and assy1_time['start'] > upgrade_end:
                        candidates.append(('ASSY1', assy1_time['start'], assy1_time['end']))
                        break
            
            if 'Assembley' in stations:
                for assembley_time in stations['Assembley']:
                    if assembley_time['start'] and upgrade_end and assembley_time['start'] > upgrade_end:
                        candidates.append(('Assembley', assembley_time['start'], assembley_time['end']))
                        break
            
            # Pick whichever comes first chronologically
            if candidates:
                candidates.sort(key=lambda x: x[1])  # Sort by start time
                result['bbd_assy_station'] = candidates[0][0]
                result['bbd_assy_start'] = candidates[0][1]
                result['bbd_assy_end'] = candidates[0][2]
        
        # 3. BBD/ASSY1 end time and FLA/CHIFLASH start time
        if result['bbd_assy_station'] and result['bbd_assy_end']:
            prev_end = result['bbd_assy_end']
            
            # Find the earliest FLA or CHIFLASH that comes AFTER the last BBD/ASSY1
            candidates = []
            
            if 'FLA' in stations:
                for fla_time in stations['FLA']:
                    if fla_time['start'] and prev_end and fla_time['start'] > prev_end:
                        candidates.append(('FLA', fla_time['start']))
                        break
            
            if 'CHIFLASH' in stations:
                for chiflash_time in stations['CHIFLASH']:
                    if chiflash_time['start'] and prev_end and chiflash_time['start'] > prev_end:
                        candidates.append(('CHIFLASH', chiflash_time['start']))
                        break
            
            if candidates:
                candidates.sort(key=lambda x: x[1])
                result['fla_chiflash_station'] = candidates[0][0]
                result['fla_chiflash_start'] = candidates[0][1]
        
        # 4. Packing end time and Shipping start time
        if 'PACKING' in stations:
            packing_end = stations['PACKING'][-1]['end']  # MOST RECENT occurrence
            result['packing_end'] = packing_end
            
            if 'SHIPPING' in stations:
                # Find first SHIPPING after the most recent PACKING
                for shipping_time in stations['SHIPPING']:
                    if shipping_time['start'] and packing_end and shipping_time['start'] > packing_end:
                        result['shipping_start'] = shipping_time['start']
                        break
        
        return result
    
    def export_raw_timestamps(self, serial_numbers: List[str], output_file: str = "raw_timestamps.csv", start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Export raw timestamps for multiple serial numbers
        """
        print(f"Processing {len(serial_numbers)} serial numbers...")
        
        results = []
        
        # Process serial numbers in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(0, len(serial_numbers), batch_size):
            batch = serial_numbers[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(serial_numbers) + batch_size - 1)//batch_size}...")
            
            # Get history data for this batch
            api_response = self.get_serial_history(batch, start_date, end_date)
            
            if not api_response.get('success'):
                print(f"API error for batch: {api_response.get('error', 'Unknown error')}")
                continue
            
            # Group history data by serial number
            history_by_sn = {}
            for record in api_response.get('history', []):
                sn = record['sn']
                if sn not in history_by_sn:
                    history_by_sn[sn] = []
                history_by_sn[sn].append(record)
            
            # Process each serial number in the batch
            for sn in batch:
                if i % 100 == 0 and i > 0:
                    print(f"Processed {i}/{len(serial_numbers)}...")
                
                history_data = history_by_sn.get(sn, [])
                result = self.process_raw_timestamps(sn, history_data)
                results.append(result)
        
        # Create CSV with raw timestamps
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Serial Number',
                'VI1 End Time',
                'Next Station After VI1',
                'Next Station Start Time',
                'Upgrade End Time',
                'BBD/ASSY1 Station',
                'BBD/ASSY1 Start Time',
                'BBD/ASSY1 End Time',
                'FLA/CHIFLASH Station',
                'FLA/CHIFLASH Start Time',
                'Packing End Time',
                'Shipping Start Time'
            ])
            
            for result in results:
                writer.writerow([
                    result['serial_number'],
                    result['vi1_end'] if result['vi1_end'] else '',
                    result['vi1_next_station'] if result['vi1_next_station'] else '',
                    result['vi1_next_start'] if result['vi1_next_start'] else '',
                    result['upgrade_end'] if result['upgrade_end'] else '',
                    result['bbd_assy_station'] if result['bbd_assy_station'] else '',
                    result['bbd_assy_start'] if result['bbd_assy_start'] else '',
                    result['bbd_assy_end'] if result['bbd_assy_end'] else '',
                    result['fla_chiflash_station'] if result['fla_chiflash_station'] else '',
                    result['fla_chiflash_start'] if result['fla_chiflash_start'] else '',
                    result['packing_end'] if result['packing_end'] else '',
                    result['shipping_start'] if result['shipping_start'] else ''
                ])
        
        print(f"\n✓ Raw timestamps exported to {output_file}")
        
        # Show a sample
        if results:
            print("\n" + "="*80)
            print("Sample data for first serial number:")
            print("="*80)
            sample = results[0]
            for key, value in sample.items():
                print(f"{key}: {value}")
        
        return results

def main():
    """
    Main function - can be used in different ways:
    1. Command line with serial numbers
    2. Read from CSV file
    3. Interactive input
    """
    import sys
    
    exporter = WebRawTimestampsExporter()
    
    if len(sys.argv) > 1:
        # Command line usage: python web_raw_timestamps.py SN123 SN456 SN789
        serial_numbers = sys.argv[1:]
        exporter.export_raw_timestamps(serial_numbers)
    else:
        # Interactive mode or CSV file
        print("Web-based Raw Timestamps Exporter")
        print("=" * 40)
        print("1. Enter serial numbers manually")
        print("2. Read from CSV file")
        print("3. Read from numbers.csv (default)")
        
        choice = input("\nChoose option (1/2/3): ").strip()
        
        if choice == "1":
            print("\nEnter serial numbers (one per line, empty line to finish):")
            serial_numbers = []
            while True:
                sn = input().strip()
                if not sn:
                    break
                serial_numbers.append(sn)
            
            if serial_numbers:
                exporter.export_raw_timestamps(serial_numbers)
            else:
                print("No serial numbers entered.")
        
        elif choice == "2":
            filename = input("Enter CSV filename: ").strip()
            try:
                serial_numbers = []
                with open(filename, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if row:
                            serial_numbers.append(row[0].strip())
                
                if serial_numbers:
                    exporter.export_raw_timestamps(serial_numbers)
                else:
                    print("No serial numbers found in file.")
            except FileNotFoundError:
                print(f"File {filename} not found.")
        
        else:  # Default to numbers.csv
            try:
                serial_numbers = []
                with open('numbers.csv', 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if row:
                            serial_numbers.append(row[0].strip())
                
                if serial_numbers:
                    exporter.export_raw_timestamps(serial_numbers)
                else:
                    print("No serial numbers found in numbers.csv.")
            except FileNotFoundError:
                print("numbers.csv file not found.")

if __name__ == "__main__":
    main()
