# Backfilling New FEC Fields

This directory contains migration and backfill scripts to add new FEC fields to the database and populate them from existing raw_data.

## New Fields Added

### Contributions Table
- `amendment_indicator` (AMNDT_IND)
- `report_type` (RPT_TP)
- `transaction_id` (TRAN_ID)
- `entity_type` (ENTITY_TP)
- `other_id` (OTHER_ID)
- `file_number` (FILE_NUM)
- `memo_code` (MEMO_CD)
- `memo_text` (MEMO_TEXT)

### Operating Expenditures Table
- `amendment_indicator` (AMNDT_IND)
- `report_year` (RPT_YR)
- `report_type` (RPT_TP)
- `image_number` (IMAGE_NUM)
- `line_number` (LINE_NUM)
- `form_type_code` (FORM_TP_CD)
- `schedule_type_code` (SCHED_TP_CD)
- `transaction_pgi` (TRANSACTION_PGI)
- `category` (CATEGORY)
- `category_description` (CATEGORY_DESC)
- `memo_code` (MEMO_CD)
- `memo_text` (MEMO_TEXT)
- `entity_type` (ENTITY_TP)
- `file_number` (FILE_NUM)
- `transaction_id` (TRAN_ID)
- `back_reference_transaction_id` (BACK_REF_TRAN_ID)

## Running Migrations and Backfills

### Option 1: Run Everything at Once (Recommended)

```bash
cd backend
python migrations/run_all_migrations_and_backfill.py
```

This will:
1. Add new columns to contributions table
2. Add new columns to operating_expenditures table
3. Backfill contribution fields from raw_data
4. Backfill operating expenditure fields from raw_data

### Option 2: Run Individually

#### Step 1: Add Columns to Database

```bash
cd backend
python migrations/add_contribution_fields.py
python migrations/add_operating_expenditure_fields.py
```

**Note:** If you get a "database is locked" error, stop the FastAPI application first, then run the migrations.

#### Step 2: Backfill Data from raw_data

```bash
cd backend
python migrations/backfill_contribution_fields.py
python migrations/backfill_operating_expenditure_fields.py
```

## How It Works

### Migrations
The migration scripts add new columns to the database tables. They check if columns already exist and skip them if they do.

### Backfills
The backfill scripts:
1. Read existing records that have `raw_data` populated
2. Extract the new field values from the `raw_data` JSON
3. Update the new structured columns with the extracted values
4. Skip records that are already populated (idempotent)

### Future Imports
Going forward, all new bulk imports will automatically populate these fields:
- The parsers have been updated to extract all fields
- New records will have both structured fields AND complete raw_data

## Verification

After running the backfills, you can verify the data was populated:

```bash
cd backend
python audit_contributions.py
```

This will show:
- Field completeness statistics
- Raw data completeness (should show all 20 Schedule A fields)
- Recommendations about data quality

## Performance

- Backfill scripts process records in batches of 1,000
- Uses bulk SQL updates for efficiency
- Progress is reported every 10,000 records
- Safe to run multiple times (idempotent)

## Troubleshooting

### Database Locked Error
If you get "database is locked":
1. Stop the FastAPI application
2. Wait a few seconds for locks to clear
3. Run the migration again

### Missing raw_data
If records don't have raw_data, they cannot be backfilled. These records will be skipped. New imports will include complete raw_data.

### Partial Backfill
The scripts are idempotent - you can run them multiple times safely. They skip records that already have the new fields populated.

