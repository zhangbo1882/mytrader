# Stats Field Bug Fix Summary

## Issue
Task handlers were not properly setting the `stats` field when completing tasks, resulting in all statistics showing zeros:
```json
"stats": {
    "success": 0,
    "failed": 0,
    "skipped": 0
}
```

## Root Cause
In `worker/handlers.py`, all task completion calls only passed `result` but not `stats` to `tm.update_task()`.

## Fix Applied

### Modified File: `worker/handlers.py`

1. **`execute_update_industry_classification`** (line 168-183)
   - Calculate stats from result
   - `success = classify_count`, `failed = failed_indices.length`, `skipped = skipped_indices`

2. **`execute_update_stock_prices`** (line 134-143)
   - Added `stats=final_stats` parameter

3. **`execute_update_financial_reports`** (line 226-236)
   - Added `stats` parameter with calculated values

4. **`execute_update_index_data`** (line 369-377)
   - Added `stats=final_stats` parameter

5. **`execute_test_handler`** (line 521-531)
   - Added `stats=final_stats` parameter

## Tests Added

### File: `tests/worker/unit/test_handlers.py`

New test class `TestStatsFieldFix` with 6 test cases:

1. **`test_industry_classification_stats`**
   - Verifies industry classification correctly calculates stats from result
   - Tests: success=511, failed=0, skipped=0

2. **`test_stock_prices_stats`**
   - Verifies stock prices handler sets stats correctly
   - Tests stats structure and result field

3. **`test_financial_reports_stats`**
   - Verifies financial reports handler sets stats correctly
   - Tests: success=3, failed=0

4. **`test_index_data_stats`**
   - Verifies index data handler sets stats correctly
   - Tests stats structure and result field

5. **`test_test_handler_stats`**
   - Verifies test handler sets stats correctly
   - Tests: success=10, failed=0

6. **`test_industry_classification_with_failures`**
   - Verifies stats correctly reflect failures
   - Tests: success=500, failed=2, skipped=5

## Test Results

```
============================= test session starts ==============================
collected 33 items

tests/worker/unit/test_handlers.py::TestStatsFieldFix::test_industry_classification_stats PASSED
tests/worker/unit/test_handlers.py::TestStatsFieldFix::test_stock_prices_stats PASSED
tests/worker/unit/test_handlers.py::TestStatsFieldFix::test_financial_reports_stats PASSED
tests/worker/unit/test_handlers.py::TestStatsFieldFix::test_index_data_stats PASSED
tests/worker/unit/test_handlers.py::TestStatsFieldFix::test_test_handler_stats PASSED
tests/worker/unit/test_handlers.py::TestStatsFieldFix::test_industry_classification_with_failures PASSED

============================== 33 passed in 7.26s ==============================
```

## Impact

### Before Fix
```json
{
    "task_id": "1a799ceb-496b-4c18-ab27-6b2152fb8a60",
    "status": "completed",
    "stats": {"success": 0, "failed": 0, "skipped": 0},
    "result": {
        "classify_count": 511,
        "members_count": 1533000,
        "failed_indices": []
    }
}
```

### After Fix
```json
{
    "task_id": "1a799ceb-496b-4c18-ab27-6b2152fb8a60",
    "status": "completed",
    "stats": {"success": 511, "failed": 0, "skipped": 0},
    "result": {
        "classify_count": 511,
        "members_count": 1533000,
        "failed_indices": []
    }
}
```

## Deployment

- Worker restarted successfully (process ID: b56743b)
- New tasks will now have correct statistics
- Existing completed tasks will remain as-is (no retroactive fix)
