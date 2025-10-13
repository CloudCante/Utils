import psycopg2
import csv
from datetime import datetime, timedelta
import json

# Database settings
DATABASE = {
    'host': 'localhost',
    'port': 5432,
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': ''
}

def calculate_time_gaps(serial_number):
    """
    Calculate time gaps for a serial number:
    1. VI1 end time - Disassembly start time
    2. Upgrade end time - BBD/ASSY1 start time
    3. Assy1/BBD end time - FLA/CHIFLASH start time
    4. Packing end time - Shipping start time
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
                'error': 'No data found',
                'vi1_to_next': None,
                'upgrade_to_bbd_or_assy1': None,
                'bbd_or_assy1_to_fla_or_chiflash': None,
                'packing_to_shipping': None
            }
        
        # Build a dictionary of station times
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
            'vi1_to_next': None,
            'upgrade_to_bbd_or_assy1': None,
            'bbd_or_assy1_to_fla_or_chiflash': None,
            'packing_to_shipping': None,
            'missing_stations': []
        }
        
        # 1. VI1 end time → Disassembly start time (or next station if no Disassembly)
        # Use the LAST/MOST RECENT occurrence of VI1 (in case of rework/multiple passes)
        if 'VI1' in stations:
            vi1_end = stations['VI1'][-1]['end']  # LAST occurrence
            
            # Find the next station after VI1's LAST occurrence
            next_station_name = None
            next_station_start = None
            
            # First try Disassembly - use the first one that comes AFTER the last VI1
            if 'Disassembly' in stations:
                for disassembly_time in stations['Disassembly']:
                    if disassembly_time['start'] and vi1_end and disassembly_time['start'] > vi1_end:
                        next_station_name = 'Disassembly'
                        next_station_start = disassembly_time['start']
                        break
            
            # If no Disassembly after VI1, find whatever comes next chronologically
            if not next_station_start:
                for station_name, times_list in stations.items():
                    if station_name != 'VI1':
                        for time_data in times_list:
                            if time_data['start'] and vi1_end and time_data['start'] > vi1_end:
                                if next_station_start is None or time_data['start'] < next_station_start:
                                    next_station_start = time_data['start']
                                    next_station_name = station_name
            
            if vi1_end and next_station_start:
                gap = next_station_start - vi1_end
                result['vi1_to_next'] = {
                    'vi1_end': vi1_end.isoformat(),
                    'next_station': next_station_name,
                    'next_station_start': next_station_start.isoformat(),
                    'gap_seconds': gap.total_seconds(),
                    'gap_hours': round(gap.total_seconds() / 3600, 2),
                    'gap_formatted': str(gap)
                }
        else:
            result['missing_stations'].append('VI1')
        
        # 2. Upgrade end time → BBD/ASSY1 start time
        # Check for BBD, ASSY1, or Assembley (variations exist)
        # Use LAST occurrence of UPGRADE and find the next BBD/ASSY1 after it
        bbd_or_assy_station = None
        if 'BBD' in stations:
            bbd_or_assy_station = 'BBD'
        elif 'ASSY1' in stations:
            bbd_or_assy_station = 'ASSY1'
        elif 'Assembley' in stations:
            bbd_or_assy_station = 'Assembley'
        
        if 'UPGRADE' in stations and bbd_or_assy_station:
            upgrade_end = stations['UPGRADE'][-1]['end']  # LAST occurrence
            
            # Find the first BBD/ASSY1 that comes AFTER the last UPGRADE
            next_start = None
            for time_data in stations[bbd_or_assy_station]:
                if time_data['start'] and upgrade_end and time_data['start'] > upgrade_end:
                    next_start = time_data['start']
                    break
            
            if upgrade_end and next_start:
                gap = next_start - upgrade_end
                result['upgrade_to_bbd_or_assy1'] = {
                    'upgrade_end': upgrade_end.isoformat(),
                    'next_station': bbd_or_assy_station,
                    'next_station_start': next_start.isoformat(),
                    'gap_seconds': gap.total_seconds(),
                    'gap_hours': round(gap.total_seconds() / 3600, 2),
                    'gap_formatted': str(gap)
                }
        elif 'UPGRADE' not in stations:
            result['missing_stations'].append('UPGRADE')
        elif not bbd_or_assy_station:
            result['missing_stations'].append('BBD/ASSY1')
        
        # 3. BBD/ASSY1 end time → FLA/CHIFLASH start time
        # Find which comes first after the LAST BBD/ASSY1: FLA or CHIFLASH
        if bbd_or_assy_station:
            # Use the LAST occurrence of BBD/ASSY1
            prev_end = stations[bbd_or_assy_station][-1]['end']
            
            next_station = None
            next_start = None
            
            # Find the earliest FLA or CHIFLASH that comes AFTER the last BBD/ASSY1
            candidates = []
            
            if 'FLA' in stations:
                for fla_time in stations['FLA']:
                    if fla_time['start'] and prev_end and fla_time['start'] > prev_end:
                        candidates.append(('FLA', fla_time['start']))
                        break  # Take the first one after prev_end
            
            if 'CHIFLASH' in stations:
                for chiflash_time in stations['CHIFLASH']:
                    if chiflash_time['start'] and prev_end and chiflash_time['start'] > prev_end:
                        candidates.append(('CHIFLASH', chiflash_time['start']))
                        break  # Take the first one after prev_end
            
            if candidates:
                # Pick the earliest one
                candidates.sort(key=lambda x: x[1])
                next_station, next_start = candidates[0]
            
            if prev_end and next_start:
                gap = next_start - prev_end
                result['bbd_or_assy1_to_fla_or_chiflash'] = {
                    'prev_station': bbd_or_assy_station,
                    'prev_station_end': prev_end.isoformat(),
                    'next_station': next_station,
                    'next_station_start': next_start.isoformat(),
                    'gap_seconds': gap.total_seconds(),
                    'gap_hours': round(gap.total_seconds() / 3600, 2),
                    'gap_formatted': str(gap)
                }
            else:
                result['missing_stations'].append('FLA/CHIFLASH')
        
        # 4. Packing end time → Shipping start time
        # Use LAST occurrence of PACKING and find first SHIPPING after it
        if 'PACKING' in stations and 'SHIPPING' in stations:
            packing_end = stations['PACKING'][-1]['end']  # LAST occurrence
            
            # Find first SHIPPING after the last PACKING
            shipping_start = None
            for shipping_time in stations['SHIPPING']:
                if shipping_time['start'] and packing_end and shipping_time['start'] > packing_end:
                    shipping_start = shipping_time['start']
                    break
            
            if packing_end and shipping_start:
                gap = shipping_start - packing_end
                result['packing_to_shipping'] = {
                    'packing_end': packing_end.isoformat(),
                    'shipping_start': shipping_start.isoformat(),
                    'gap_seconds': gap.total_seconds(),
                    'gap_hours': round(gap.total_seconds() / 3600, 2),
                    'gap_formatted': str(gap)
                }
        else:
            if 'PACKING' not in stations:
                result['missing_stations'].append('PACKING')
            if 'SHIPPING' not in stations:
                result['missing_stations'].append('SHIPPING')
        
        return result
        
    except psycopg2.Error as e:
        return {
            'serial_number': serial_number,
            'error': f'Database error: {str(e)}',
            'vi1_to_next': None,
            'upgrade_to_bbd_or_assy1': None,
            'bbd_or_assy1_to_fla_or_chiflash': None,
            'packing_to_shipping': None
        }
    except Exception as e:
        return {
            'serial_number': serial_number,
            'error': f'Error: {str(e)}',
            'vi1_to_next': None,
            'upgrade_to_bbd_or_assy1': None,
            'bbd_or_assy1_to_fla_or_chiflash': None,
            'packing_to_shipping': None
        }

def main():
    # Read serial numbers from CSV (handle UTF-8 BOM)
    serial_numbers = []
    with open('numbers.csv', 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if row:
                serial_numbers.append(row[0].strip())
    
    print(f"Processing {len(serial_numbers)} serial numbers...")
    
    results = []
    errors = []
    
    for i, sn in enumerate(serial_numbers, 1):
        if i % 100 == 0:
            print(f"Processed {i}/{len(serial_numbers)}...")
        
        result = calculate_time_gaps(sn)
        results.append(result)
        
        if 'error' in result or result.get('missing_stations'):
            errors.append(result)
    
    # Save results to JSON
    with open('time_gaps_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✓ Results saved to time_gaps_results.json")
    
    # Save errors/missing data to separate file
    if errors:
        with open('time_gaps_errors.json', 'w') as f:
            json.dump(errors, f, indent=2)
        print(f"✓ Found {len(errors)} serial numbers with missing data - saved to time_gaps_errors.json")
    
    # Create summary CSV
    with open('time_gaps_summary.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Serial Number',
            'VI1→Next Station',
            'VI1→Next (hours)',
            'Upgrade→BBD/ASSY1 Station',
            'Upgrade→BBD/ASSY1 (hours)',
            'BBD/ASSY1→Next Station',
            'BBD/ASSY1→FLA/CHIFLASH (hours)',
            'Packing→Shipping (hours)',
            'Missing Stations'
        ])
        
        for result in results:
            vi1_next = result.get('vi1_to_next')
            upgrade_next = result.get('upgrade_to_bbd_or_assy1')
            bbd_next = result.get('bbd_or_assy1_to_fla_or_chiflash')
            packing = result.get('packing_to_shipping')
            
            writer.writerow([
                result['serial_number'],
                vi1_next['next_station'] if vi1_next else 'N/A',
                vi1_next['gap_hours'] if vi1_next else 'N/A',
                upgrade_next['next_station'] if upgrade_next else 'N/A',
                upgrade_next['gap_hours'] if upgrade_next else 'N/A',
                bbd_next['next_station'] if bbd_next else 'N/A',
                bbd_next['gap_hours'] if bbd_next else 'N/A',
                packing['gap_hours'] if packing else 'N/A',
                ', '.join(result.get('missing_stations', []))
            ])
    
    print(f"✓ Summary saved to time_gaps_summary.csv")
    
    # Print sample result
    if results:
        print("\n" + "="*80)
        print("Sample result for first serial number:")
        print(json.dumps(results[0], indent=2))

if __name__ == "__main__":
    main()

