#!/bin/bash

# Load environment variables from .env file
set -o allexport
source dot.env
set +o allexport

# Run DuckDB commands using the temporary database
duckdb "$TEMP_DB" -c "
    PRAGMA temp_directory='/tmp';       -- Use /tmp for temporary storage
	PRAGMA memory_limit='${MEM_LIMIT}'; -- Limit memory usage to ${MEM_LIMIT}
	PRAGMA threads=${NUM_THREADS};      -- Limit to 2 threads to reduce resource usage

	-- Step 1: Select data from the Parquet file and add the derived column using CASE
    SELECT b.*
		, i.instnm
		, i.sector
		, i.iclevel
		, i.control
		, i.instsize
		, i.efydetot_tot_22
		, i.efyde_tot_22
		, i.typeinst
		, i.insttype
	FROM '${SURVEY_DATA}' b
	LEFT JOIN '${IPEDS_DATA} i
	ON b.IPEDID = i.unitid
" 2>> error.log

# Clean up the temporary DuckDB database file
rm -f "$TEMP_DB"