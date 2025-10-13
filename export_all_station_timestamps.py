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

def get_all_station_timestamps(serial_number):
    """
    Get all station start and end times for a serial number
    Uses the MOST RECENT visit if a station appears multiple times
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
                'stations': {}
            }
        
        # Build dictionary of stations (with lists for multiple visits)
        stations = {}
        for station, start_time, end_time in rows:
            if station not in stations:
                stations[station] = []
            stations[station].append({
                'start': start_time,
                'end': end_time
            })
        
        # Use MOST RECENT visit for each station
        result = {
            'serial_number': serial_number,
            'stations': {}
        }
        
        for station_name, visits in stations.items():
            # Get the last (most recent) visit
            last_visit = visits[-1]
            result['stations'][station_name] = {
                'start': last_visit['start'],
                'end': last_visit['end']
            }
        
        return result
        
    except Exception as e:
        print(f"Error processing {serial_number}: {e}")
        return {
            'serial_number': serial_number,
            'stations': {}
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
        
        result = get_all_station_timestamps(sn)
        results.append(result)
    
    # Get ALL unique stations from all results
    all_stations = set()
    for result in results:
        all_stations.update(result['stations'].keys())
    
    # Sort them alphabetically for consistent ordering
    all_stations = sorted(all_stations)
    
    print(f"\n✓ Found {len(all_stations)} unique stations across all serial numbers:")
    for station in all_stations:
        print(f"  - {station}")
    
    # Create CSV with all station timestamps
    with open('all_station_timestamps.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Build header with start/end columns for each station
        header = ['Serial Number']
        for station in all_stations:
            header.append(f'{station} Start Time')
            header.append(f'{station} End Time')
        writer.writerow(header)
        
        # Write data rows
        for result in results:
            row = [result['serial_number']]
            stations = result['stations']
            
            for station in all_stations:
                if station in stations:
                    start = stations[station]['start']
                    end = stations[station]['end']
                    row.append(start if start else '')
                    row.append(end if end else '')
                else:
                    row.append('')  # No start time
                    row.append('')  # No end time
            
            writer.writerow(row)
    
    print(f"\n✓ All station timestamps exported to all_station_timestamps.csv")
    
    # Show a sample
    if results:
        print("\n" + "="*80)
        print("Sample data for first serial number:")
        print("="*80)
        sample = results[0]
        print(f"Serial: {sample['serial_number']}")
        for station, times in sorted(sample['stations'].items()):
            print(f"  {station:20} Start: {times['start']}  |  End: {times['end']}")

if __name__ == "__main__":
    main()

