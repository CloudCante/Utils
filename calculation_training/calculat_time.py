import psycopg2
import csv

DATABASE = {
    'host': 'localhost',
    'port': 5432,
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': ''
}

serial_numbers = []
with open('numbers.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    for row in reader:
        if row:
            serial_numbers.append(row[0].strip())

print(f"Found {len(serial_numbers)} serial numbers from CSV")


conn = psycopg2.connect(**DATABASE)
cur = conn.cursor()

for i, serial_number in enumerate(serial_numbers[:5]):
    print(f"\n--- Processing {i + 1}/5: {serial_number} ---")

    cur.execute("""
    SELECT workstation_name, history_station_start_time, history_station_end_time
    FROM workstation_master_log
    WHERE sn = %s
    ORDER BY history_station_start_time;
    """, (serial_number, ))

    rows = cur.fetchall()

    stations = {}

    for station_name, start_time, end_time in rows:
        if station_name not in stations:
            stations[station_name] = []

        stations[station_name].append({
            'start': start_time,
            'end': end_time
        })
    
    if 'VI1' in stations:
        vi1_end = stations['VI1'][-1]['end']

        next_station_name = None
        next_station_start = None

        if 'Disassembly' in stations:
            for visit in stations['Disassembly']:

                if visit['start'] > vi1_end:
                    next_station_name = 'Disassembly'
                    next_station_start = visit['start']
                    break
        
        if next_station_start:
            gap = next_station_start - vi1_end
            gap_hours = gap.total_seconds() / 3600

            print(f"\n VI1 {next_station_name}")
            print(f"VI1 ended: {vi1_end}")
            print(f"{next_station_name} started: {next_station_start}")
            print(f"Gap in hours: {gap_hours:.2f}")
        else:
            print("No Disassembly after VI1!")
    else:
        print("Missing VI1 data!")


cur.close()
conn.close()