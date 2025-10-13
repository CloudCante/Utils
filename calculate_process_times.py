import psycopg2
import csv
import json

# Database settings
DATABASE = {
    'host': 'localhost',
    'port': 5432,
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': ''
}

def calculate_process_times(serial_number):
    """
    Calculate how long each station took (process time)
    Uses the MOST RECENT visit for each station
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
                'process_times': {}
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
        
        # Calculate process time for each station (using MOST RECENT visit)
        process_times = {}
        
        for station_name, visits in stations.items():
            # Get MOST RECENT visit
            last_visit = visits[-1]
            
            if last_visit['start'] and last_visit['end']:
                # Calculate how long the work took
                process_time = last_visit['end'] - last_visit['start']
                process_times[station_name] = {
                    'start': last_visit['start'].isoformat(),
                    'end': last_visit['end'].isoformat(),
                    'duration_seconds': process_time.total_seconds(),
                    'duration_hours': round(process_time.total_seconds() / 3600, 2),
                    'duration_formatted': str(process_time)
                }
        
        return {
            'serial_number': serial_number,
            'process_times': process_times
        }
        
    except Exception as e:
        return {
            'serial_number': serial_number,
            'error': f'Error: {str(e)}',
            'process_times': {}
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
        
        result = calculate_process_times(sn)
        results.append(result)
    
    # Save detailed JSON
    with open('process_times_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✓ Results saved to process_times_results.json")
    
    # Get ALL unique stations from all results
    all_stations = set()
    for result in results:
        all_stations.update(result.get('process_times', {}).keys())
    
    # Sort them alphabetically for consistent ordering
    all_stations = sorted(all_stations)
    
    print(f"✓ Found {len(all_stations)} unique stations across all serial numbers")
    
    with open('process_times_summary.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        header = ['Serial Number']
        for station in all_stations:
            header.append(f'{station} (HH:MM:SS)')
        writer.writerow(header)
        
        # Data rows
        for result in results:
            row = [result['serial_number']]
            process_times = result.get('process_times', {})
            
            for station in all_stations:
                if station in process_times:
                    row.append(process_times[station]['duration_formatted'])
                else:
                    row.append('N/A')
            
            writer.writerow(row)
    
    print(f"✓ Summary saved to process_times_summary.csv")
    
    # Print sample result
    if results:
        print("\n" + "="*80)
        print("Sample result for first serial number:")
        print("="*80)
        sample = results[0]
        print(f"Serial: {sample['serial_number']}")
        if 'process_times' in sample:
            for station, data in sample['process_times'].items():
                print(f"  {station:15} : {data['duration_hours']} hours ({data['duration_formatted']})")

if __name__ == "__main__":
    main()

