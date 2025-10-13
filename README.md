# Time Gap Analysis Workflow

This workflow calculates time gaps between manufacturing stations for GPU units.

## 📋 What It Does

Calculates the time between these station transitions:
1. **VI1 end** → **Next station start** (usually Disassembly)
2. **Upgrade end** → **BBD/ASSY1 start**
3. **BBD/ASSY1 end** → **FLA/CHIFLASH start**
4. **Packing end** → **Shipping start**

## 🚀 Quick Start

### Prerequisites
- Python 3.x
- PostgreSQL database access
- `psycopg2` library: `pip install psycopg2-binary`

### Database Configuration
Edit the `DATABASE` dict in both `calculate_times.py` and `export_raw_timestamps.py`:
```python
DATABASE = {
    'host': 'localhost',
    'port': 5432,
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': ''  # Add password if needed
}
```

### Running the Analysis

1. **Prepare input file:**
   - Create/update `numbers.csv` with serial numbers (one per line)

2. **Run the master script:**
   ```bash
   python run_analysis.py
   ```

3. **Check output files** (see below)

## 📁 Files

### Input
- **`numbers.csv`** - Serial numbers to analyze (one per line)

### Core Scripts
- **`run_analysis.py`** - Master script (runs everything) ⭐
- **`calculate_times.py`** - Calculates time gaps
- **`export_raw_timestamps.py`** - Exports raw timestamps

### Output Files
- **`time_gaps_summary.csv`** - Time gaps in hours (easy to read)
- **`raw_timestamps.csv`** - Raw database timestamps used in calculations
- **`time_gaps_results.json`** - Detailed JSON with all metadata
- **`missing_data_serial_numbers.txt`** - Simple list of serials with missing data
- **`missing_data_breakdown.csv`** - Details on what data is missing

## 🔧 How It Works

### Handles Multiple Workflow Types
- Units can go through **different stations** based on their service flow
- The "/" in requirements means "OR" (e.g., BBD/ASSY1 means BBD OR ASSY1)
- Automatically finds the appropriate alternate station if the primary is missing

### Handles Rework/Multiple Passes
- If a unit fails testing and goes through stations multiple times, the script uses the **MOST RECENT** cycle
- Example: If a unit goes through VI1 twice, it uses the 2nd (most recent) VI1 end time

### Database Table
Uses `workstation_master_log` table:
- `sn` - Serial number
- `workstation_name` - Station name
- `history_station_start_time` - When unit arrived at station
- `history_station_end_time` - When unit left station

## 📊 Output Explanation

### time_gaps_summary.csv
Easy-to-read spreadsheet with:
- Serial number
- Station names used (shows which alternate path was taken)
- Time gaps in hours
- Missing stations (if any)

### raw_timestamps.csv
Shows the exact timestamps pulled from the database:
- VI1 end time
- Next station after VI1 and its start time
- Upgrade end time
- BBD/ASSY1 station name, start and end times
- FLA/CHIFLASH station name and start time
- Packing end time
- Shipping start time

### missing_data_serial_numbers.txt
Simple list of serial numbers that have incomplete data.
Use this when someone asks "which ones don't we have?"

### missing_data_breakdown.csv
Detailed view showing exactly what is missing for each serial number.

## 🔄 Running Multiple Times

To run the analysis again:
1. Update `numbers.csv` with new serial numbers
2. Run `python run_analysis.py`
3. Output files will be overwritten with new results

## ❓ Common Issues

**"No data found" for a serial number**
- Serial doesn't exist in the database
- Check spelling/formatting of serial number

**Missing stations for many units**
- Normal! Different service flows use different stations
- Units starting at VI2 won't have VI1 data
- Units that don't need upgrades won't have UPGRADE data

**Multiple passes through same station**
- Also normal! Units that fail testing get reworked
- Script automatically uses the most recent pass

## 📝 Notes

- Time gaps can be negative if data timestamps are incorrect in the database
- Very large time gaps (days/weeks) usually indicate the unit was waiting or on hold
- The script processes ~1600 units in about 20-30 seconds

## 🐛 Troubleshooting

**Database connection error:**
- Check database credentials in the scripts
- Verify database is running and accessible
- Test with: `psql -h localhost -U gpu_user -d fox_db`

**Import error for psycopg2:**
- Install: `pip install psycopg2-binary`

**CSV encoding issues:**
- The scripts handle UTF-8 BOM automatically
- If you see weird characters, save `numbers.csv` as UTF-8

