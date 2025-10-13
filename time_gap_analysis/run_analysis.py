#!/usr/bin/env python3
"""
Time Gap Analysis - Master Script

This script runs the complete analysis workflow:
1. Calculates time gaps between stations
2. Exports raw timestamps
3. Creates lists of missing data

Input: numbers.csv (list of serial numbers, one per line)
Output: Multiple CSV/JSON files with results

Usage: python run_analysis.py
"""

import subprocess
import sys
import os
from datetime import datetime

def run_script(script_name, description):
    """Run a Python script and report results"""
    print("=" * 70)
    print(f"Running: {description}")
    print("=" * 70)
    try:
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=False, 
                              text=True, 
                              check=True)
        print(f"✓ {description} completed successfully\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed with error code {e.returncode}\n")
        return False

def main():
    print("\n" + "=" * 70)
    print("TIME GAP ANALYSIS WORKFLOW")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Check if input file exists
    if not os.path.exists('numbers.csv'):
        print("ERROR: numbers.csv not found!")
        print("Please create numbers.csv with one serial number per line.")
        sys.exit(1)
    
    # Count serial numbers
    with open('numbers.csv', 'r', encoding='utf-8-sig') as f:
        sn_count = sum(1 for line in f if line.strip())
    
    print(f"Found {sn_count} serial numbers in numbers.csv")
    print()
    
    # Step 1: Calculate time gaps
    success = run_script('calculate_times.py', 
                        'Step 1/2: Calculate time gaps between stations')
    if not success:
        print("Workflow failed at Step 1")
        sys.exit(1)
    
    # Step 2: Export raw timestamps
    success = run_script('export_raw_timestamps.py', 
                        'Step 2/2: Export raw timestamps from database')
    if not success:
        print("Workflow failed at Step 2")
        sys.exit(1)
    
    # Step 3: Create missing data lists
    print("=" * 70)
    print("Step 3/3: Creating missing data lists")
    print("=" * 70)
    
    import json
    import csv
    
    with open('time_gaps_errors.json', 'r') as f:
        errors = json.load(f)
    
    # Simple text list
    with open('missing_data_serial_numbers.txt', 'w') as f:
        for err in errors:
            f.write(err['serial_number'] + '\n')
    
    # Detailed breakdown
    with open('missing_data_breakdown.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Serial Number',
            'Has VI1→Next?',
            'Has Upgrade→BBD/ASSY1?',
            'Has BBD/ASSY1→FLA/CHIFLASH?',
            'Has Packing→Shipping?',
            'Missing Stations'
        ])
        
        for err in errors:
            writer.writerow([
                err['serial_number'],
                'Yes' if err.get('vi1_to_next') else 'No',
                'Yes' if err.get('upgrade_to_bbd_or_assy1') else 'No',
                'Yes' if err.get('bbd_or_assy1_to_fla_or_chiflash') else 'No',
                'Yes' if err.get('packing_to_shipping') else 'No',
                ', '.join(err.get('missing_stations', []))
            ])
    
    print(f"✓ Created missing_data_serial_numbers.txt ({len(errors)} serials)")
    print(f"✓ Created missing_data_breakdown.csv")
    print()
    
    # Final summary
    print("=" * 70)
    print("WORKFLOW COMPLETE!")
    print("=" * 70)
    print()
    print("Output Files Created:")
    print("  1. time_gaps_summary.csv          - Time gaps in hours")
    print("  2. raw_timestamps.csv             - Raw database timestamps")
    print("  3. time_gaps_results.json         - Detailed JSON results")
    print("  4. missing_data_serial_numbers.txt - Simple list of missing SNs")
    print("  5. missing_data_breakdown.csv     - Details on what's missing")
    print()
    
    # Statistics
    complete_count = sn_count - len(errors)
    print("Statistics:")
    print(f"  ✅ {complete_count} units ({complete_count/sn_count*100:.1f}%) - Complete data")
    print(f"  ⚠️  {len(errors)} units ({len(errors)/sn_count*100:.1f}%) - Incomplete data")
    print()
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

if __name__ == "__main__":
    main()

