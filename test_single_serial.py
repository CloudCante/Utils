#!/usr/bin/env python3
"""
Test script to check a single serial number with the export_raw_timestamps function
"""
import psycopg2
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
        
        print(f"Found {len(rows)} records for serial number {serial_number}")
        print("Raw data from database:")
        for row in rows:
            print(f"  {row[0]}: {row[1]} - {row[2]}")
        print()
        
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
        
        print("Stations dictionary:")
        for station, times_list in stations.items():
            print(f"  {station}: {len(times_list)} occurrence(s)")
            for i, time_data in enumerate(times_list):
                print(f"    [{i}] {time_data['start']} - {time_data['end']}")
        print()
        
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
            print(f"VI1 last occurrence ends at: {vi1_end}")
            
            # Find the next station after VI1's LAST occurrence
            next_station_name = None
            next_station_start = None
            
            # Look for Disassembly or UPGRADE only
            candidates = []
            
            if 'Disassembly' in stations:
                print("Checking Disassembly stations...")
                for disassembly_time in stations['Disassembly']:
                    print(f"  Disassembly: {disassembly_time['start']} (VI1 end: {vi1_end})")
                    if disassembly_time['start'] and vi1_end and disassembly_time['start'] > vi1_end:
                        candidates.append(('Disassembly', disassembly_time['start']))
                        print(f"    ✓ Added Disassembly candidate: {disassembly_time['start']}")
                        break
            
            if 'UPGRADE' in stations:
                print("Checking UPGRADE stations...")
                for upgrade_time in stations['UPGRADE']:
                    print(f"  UPGRADE: {upgrade_time['start']} (VI1 end: {vi1_end})")
                    if upgrade_time['start'] and vi1_end and upgrade_time['start'] > vi1_end:
                        candidates.append(('UPGRADE', upgrade_time['start']))
                        print(f"    ✓ Added UPGRADE candidate: {upgrade_time['start']}")
                        break
            
            print(f"Candidates found: {candidates}")
            
            # Pick whichever comes first
            if candidates:
                candidates.sort(key=lambda x: x[1])
                next_station_name = candidates[0][0]
                next_station_start = candidates[0][1]
                print(f"Selected next station: {next_station_name} at {next_station_start}")
            
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
            print(f"UPGRADE last occurrence ends at: {upgrade_end}")
            
            if bbd_or_assy_station:
                result['bbd_assy_station'] = bbd_or_assy_station
                print(f"Looking for {bbd_or_assy_station} after UPGRADE...")
                
                # Find the first BBD/ASSY1 that comes AFTER the last UPGRADE
                for time_data in stations[bbd_or_assy_station]:
                    print(f"  {bbd_or_assy_station}: {time_data['start']} (UPGRADE end: {upgrade_end})")
                    if time_data['start'] and upgrade_end and time_data['start'] > upgrade_end:
                        result['bbd_assy_start'] = time_data['start']
                        result['bbd_assy_end'] = time_data['end']
                        print(f"    ✓ Found {bbd_or_assy_station}: {time_data['start']} - {time_data['end']}")
                        break
        
        # 3. BBD/ASSY1 end time and FLA/CHIFLASH start time
        if bbd_or_assy_station and result['bbd_assy_end']:
            prev_end = result['bbd_assy_end']
            print(f"Looking for FLA/CHIFLASH after {bbd_or_assy_station} ends at: {prev_end}")
            
            # Find the earliest FLA or CHIFLASH that comes AFTER the last BBD/ASSY1
            candidates = []
            
            if 'FLA' in stations:
                for fla_time in stations['FLA']:
                    print(f"  FLA: {fla_time['start']} ({bbd_or_assy_station} end: {prev_end})")
                    if fla_time['start'] and prev_end and fla_time['start'] > prev_end:
                        candidates.append(('FLA', fla_time['start']))
                        print(f"    ✓ Added FLA candidate: {fla_time['start']}")
                        break
            
            if 'CHIFLASH' in stations:
                for chiflash_time in stations['CHIFLASH']:
                    print(f"  CHIFLASH: {chiflash_time['start']} ({bbd_or_assy_station} end: {prev_end})")
                    if chiflash_time['start'] and prev_end and chiflash_time['start'] > prev_end:
                        candidates.append(('CHIFLASH', chiflash_time['start']))
                        print(f"    ✓ Added CHIFLASH candidate: {chiflash_time['start']}")
                        break
            
            print(f"FLA/CHIFLASH candidates: {candidates}")
            
            if candidates:
                candidates.sort(key=lambda x: x[1])
                result['fla_chiflash_station'] = candidates[0][0]
                result['fla_chiflash_start'] = candidates[0][1]
                print(f"Selected FLA/CHIFLASH: {candidates[0][0]} at {candidates[0][1]}")
        
        # 4. Packing end time and Shipping start time
        if 'PACKING' in stations:
            packing_end = stations['PACKING'][-1]['end']  # LAST occurrence
            result['packing_end'] = packing_end
            print(f"PACKING last occurrence ends at: {packing_end}")
            
            if 'SHIPPING' in stations:
                # Find first SHIPPING after the last PACKING
                for shipping_time in stations['SHIPPING']:
                    print(f"  SHIPPING: {shipping_time['start']} (PACKING end: {packing_end})")
                    if shipping_time['start'] and packing_end and shipping_time['start'] > packing_end:
                        result['shipping_start'] = shipping_time['start']
                        print(f"    ✓ Found SHIPPING: {shipping_time['start']}")
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
    # Test with the serial number you provided
    serial_number = '1654223007122'
    
    print(f"Testing serial number: {serial_number}")
    print("=" * 80)
    
    result = get_raw_timestamps(serial_number)
    
    print("\n" + "=" * 80)
    print("FINAL RESULT:")
    print("=" * 80)
    for key, value in result.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    main()
