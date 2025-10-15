# Database Cleanup TODO

## What We Did
- Dropped `outbound_version` column from `workstation_master_log` table
- Created clean table without duplicates using `CREATE TABLE ... AS SELECT DISTINCT ON`
- Need to replace old table with clean one

## Next Steps

### 1. Complete Database Cleanup
```sql
-- Check the clean table count
SELECT COUNT(*) FROM workstation_master_log_clean;

-- If good, replace the old table
DROP TABLE workstation_master_log;
ALTER TABLE workstation_master_log_clean RENAME TO workstation_master_log;

-- Recreate constraints and indexes
ALTER TABLE workstation_master_log ADD PRIMARY KEY (id);
ALTER TABLE workstation_master_log ADD CONSTRAINT unique_station_times UNIQUE (sn, workstation_name, history_station_start_time, history_station_end_time);
-- Add other indexes as needed (check with \d workstation_master_log)
```

### 2. Update Upload Script
**File:** `upload_workstation_master_log.py`

**Changes needed:**
1. **Remove `outbound_version` from unique constraint:**
   ```python
   UNIQUE (sn, pn, customer_pn, workstation_name,
           history_station_start_time, history_station_end_time, hours,
           service_flow, model, history_station_passing_status,
           passing_station_method, operator, first_station_start_time, data_source)
   ```

2. **Remove `outbound_version` from INSERT statement:**
   ```python
   INSERT INTO workstation_master_log (
       sn, pn, model, workstation_name, history_station_start_time, history_station_end_time,
       history_station_passing_status, operator, customer_pn, hours,
       service_flow, passing_station_method, first_station_start_time, data_source
   ) VALUES %s
   ```

3. **Remove from mapped_row dictionary:**
   ```python
   # DELETE THIS LINE:
   'outbound_version': convert_empty_string(str(row.get('outbound_version', ''))),
   ```

4. **Remove from values tuple:**
   ```python
   # DELETE 'outbound_version' from the values tuple
   ```

## Result
- No more duplicates in database
- Upload script will work with new table structure
- Script will reject true duplicates (same station, same timestamps)
