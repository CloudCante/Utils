import psycopg2
import csv
from datetime import datetime

# Database settings
DATABASE = {
    'host': 'localhost',
    'port': 5432,
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': ''
}

def get_raw_timestamps(serial_number):
    """
    Get the raw timestamps used in calculations for a serial number
    """
    try:
        conn = psycopg2.connect(**DATABASE)
        cur = conn.cursor()
        
        # Get all station times for this serial number
        cur.execute("""
            SELECT workstation_name, history_station_start_time, history_station_end_time
            FROM workstation_master_log 
            WHERE sn = %s
            ORDER BY history_station_start_time;
        """, (serial_number,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        if not rows:
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
        for station, start_time, end_time in rows:
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
            vi1_end = stations['VI1'][-1]['end']  # LAST occurrence
            result['vi1_end'] = vi1_end
            
            # Find the next station after VI1's LAST occurrence
            next_station_name = None
            next_station_start = None
            
            # First try Disassembly
            if 'Disassembly' in stations:
                for disassembly_time in stations['Disassembly']:
                    if disassembly_time['start'] and vi1_end and disassembly_time['start'] > vi1_end:
                        next_station_name = 'Disassembly'
                        next_station_start = disassembly_time['start']
                        break
            
            # If no Disassembly after VI1, find whatever comes next
            if not next_station_start:
                for station_name, times_list in stations.items():
                    if station_name != 'VI1':
                        for time_data in times_list:
                            if time_data['start'] and vi1_end and time_data['start'] > vi1_end:
                                if next_station_start is None or time_data['start'] < next_station_start:
                                    next_station_start = time_data['start']
                                    next_station_name = station_name
            
            result['vi1_next_station'] = next_station_name
            result['vi1_next_start'] = next_station_start
        
        # 2. Upgrade end time and BBD/ASSY1 start time
        bbd_or_assy_station = None
        if 'BBD' in stations:
            bbd_or_assy_station = 'BBD'
        elif 'ASSY1' in stations:
            bbd_or_assy_station = 'ASSY1'
        elif 'Assembley' in stations:
            bbd_or_assy_station = 'Assembley'
        
        if 'UPGRADE' in stations:
            upgrade_end = stations['UPGRADE'][-1]['end']  # LAST occurrence
            result['upgrade_end'] = upgrade_end
            
            if bbd_or_assy_station:
                result['bbd_assy_station'] = bbd_or_assy_station
                
                # Find the first BBD/ASSY1 that comes AFTER the last UPGRADE
                for time_data in stations[bbd_or_assy_station]:
                    if time_data['start'] and upgrade_end and time_data['start'] > upgrade_end:
                        result['bbd_assy_start'] = time_data['start']
                        result['bbd_assy_end'] = time_data['end']
                        break
        
        # 3. BBD/ASSY1 end time and FLA/CHIFLASH start time
        if bbd_or_assy_station and result['bbd_assy_end']:
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
            packing_end = stations['PACKING'][-1]['end']  # LAST occurrence
            result['packing_end'] = packing_end
            
            if 'SHIPPING' in stations:
                # Find first SHIPPING after the last PACKING
                for shipping_time in stations['SHIPPING']:
                    if shipping_time['start'] and packing_end and shipping_time['start'] > packing_end:
                        result['shipping_start'] = shipping_time['start']
                        break
        
        return result
        
    except Exception as e:
        print(f"Error processing {serial_number}: {e}")
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

def main():
    # Read serial numbers from CSV
    serial_numbers = []
    with open('numbers.csv', 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if row:
                serial_numbers.append(row[0].strip())
    
    print(f"Processing {len(serial_numbers)} serial numbers...")
    
    results = []
    
    for i, sn in enumerate(serial_numbers, 1):
        if i % 100 == 0:
            print(f"Processed {i}/{len(serial_numbers)}...")
        
        result = get_raw_timestamps(sn)
        results.append(result)
    
    # Create CSV with raw timestamps
    with open('raw_timestamps.csv', 'w', newline='') as f:
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
    
    print(f"\n✓ Raw timestamps exported to raw_timestamps.csv")
    
    # Show a sample
    if results:
        print("\n" + "="*80)
        print("Sample data for first serial number:")
        print("="*80)
        sample = results[0]
        for key, value in sample.items():
            print(f"{key}: {value}")

if __name__ == "__main__":
    main()

