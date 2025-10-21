-- Setup script for multiple delete files test
-- This creates a DuckLake catalog where one data file has TWO delete files
-- in the same snapshot, which should be merged during query execution.
--
-- To run this script:
--   1. Copy tests/test_data/with_deletes.ducklake to tests/test_data/multiple_deletes.ducklake
--   2. Copy tests/test_data/with_deletes.ducklake.files/ to tests/test_data/multiple_deletes.ducklake.files/
--   3. Run: duckdb tests/test_data/multiple_deletes.ducklake < tests/test_data/setup_multiple_deletes_catalog.sql
--
-- This modifies the snapshot ranges of both delete files so they are both active
-- in the same snapshot (snapshot_id = 4), creating a scenario where:
--   - Data file has 5 rows: [1, 2, 3, 4, 5] at positions [0, 1, 2, 3, 4]
--   - Delete file 1: deletes position [1] (id=2)
--   - Delete file 2: deletes positions [1, 3] (id=2, id=4)
--   - Both files are active in snapshot 4
--   - Expected merged result: positions [1, 3] deleted -> rows with ids [1, 3, 5] remain

-- Update both delete files to be active in the same snapshot range
-- This creates overlapping delete files for testing the merge logic
UPDATE ducklake_delete_file
SET begin_snapshot = 2,
    end_snapshot = NULL
WHERE delete_file_id = 1;

UPDATE ducklake_delete_file
SET begin_snapshot = 2,
    end_snapshot = NULL
WHERE delete_file_id = 2;

-- Verify the setup
SELECT
    'Delete files configuration:' as info;

SELECT
    del.delete_file_id,
    del.data_file_id,
    del.path,
    del.begin_snapshot,
    del.end_snapshot,
    del.delete_count
FROM ducklake_delete_file del
ORDER BY delete_file_id;

SELECT
    'Both delete files should be active in snapshot 4' as note;
