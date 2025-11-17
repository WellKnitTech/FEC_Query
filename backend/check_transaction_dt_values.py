#!/usr/bin/env python3
"""
Diagnostic script to check TRANSACTION_DT values in the bulk CSV file.
"""
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

file_path = os.path.join(os.path.dirname(__file__), "data/bulk/individual_contributions_2026.txt")

if not os.path.exists(file_path):
    print(f"Error: File not found: {file_path}")
    sys.exit(1)

# FEC Schedule A CSV columns (pipe-delimited, no headers)
fec_columns = [
    'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'IMAGE_NUM', 'TRAN_ID', 
    'ENTITY_TP_CODE', 'ENTITY_TP_DESC', 'NAME',
    'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT',
    'TRANSACTION_AMT', 'OTHER_ID', 'CAND_ID', 'TRAN_TP', 'FILE_NUM',
    'MEMO_CD', 'SUB_ID'
]

print("=" * 80)
print("TRANSACTION_DT Value Analysis")
print("=" * 80)

# Read first 10,000 rows for analysis
print(f"\nReading first 10,000 rows from: {file_path}")
chunk = pd.read_csv(
    file_path,
    sep='|',
    header=None,
    names=fec_columns,
    nrows=10000,
    dtype=str,
    low_memory=False,
    on_bad_lines='skip'
)

print(f"Total rows read: {len(chunk)}")

# Basic statistics
print("\n" + "=" * 80)
print("Basic Statistics")
print("=" * 80)
print(f"Total rows: {len(chunk)}")
print(f"Rows with TRANSACTION_DT as empty string: {(chunk['TRANSACTION_DT'] == '').sum()}")
print(f"Rows with TRANSACTION_DT as NaN/None: {chunk['TRANSACTION_DT'].isna().sum()}")
print(f"Rows with non-empty TRANSACTION_DT: {(chunk['TRANSACTION_DT'].notna() & (chunk['TRANSACTION_DT'] != '')).sum()}")

# Value length distribution
print("\n" + "=" * 80)
print("TRANSACTION_DT Value Length Distribution")
print("=" * 80)
length_dist = chunk['TRANSACTION_DT'].astype(str).str.len().value_counts().sort_index()
print(length_dist)

# Check for specific problematic values
print("\n" + "=" * 80)
print("Problematic Value Analysis")
print("=" * 80)
non_empty = chunk[chunk['TRANSACTION_DT'].notna() & (chunk['TRANSACTION_DT'] != '')]
if len(non_empty) > 0:
    # Check for non-numeric values
    non_numeric = non_empty[~non_empty['TRANSACTION_DT'].str.isdigit()]
    if len(non_numeric) > 0:
        print(f"\nNon-numeric TRANSACTION_DT values found: {len(non_numeric)}")
        print("Sample non-numeric values:")
        print(non_numeric[['SUB_ID', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'NAME']].head(10))
    
    # Check for values that aren't 8 digits
    not_8_digits = non_empty[(non_empty['TRANSACTION_DT'].str.len() != 8) | (~non_empty['TRANSACTION_DT'].str.isdigit())]
    if len(not_8_digits) > 0:
        print(f"\nTRANSACTION_DT values that aren't 8-digit numbers: {len(not_8_digits)}")
        print("Sample values:")
        unique_lengths = not_8_digits['TRANSACTION_DT'].str.len().unique()
        print(f"Unique lengths found: {sorted(unique_lengths)}")
        for length in sorted(unique_lengths)[:5]:  # Show first 5 unique lengths
            sample = not_8_digits[not_8_digits['TRANSACTION_DT'].str.len() == length]
            print(f"\nLength {length} (showing first 5):")
            print(sample[['SUB_ID', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'NAME']].head(5))

# Show sample of empty/None values
print("\n" + "=" * 80)
print("Sample Rows with Empty/None TRANSACTION_DT")
print("=" * 80)
empty_mask = chunk['TRANSACTION_DT'].isna() | (chunk['TRANSACTION_DT'] == '')
if empty_mask.any():
    print(f"Found {empty_mask.sum()} rows with empty/None TRANSACTION_DT")
    print("\nFirst 10 rows:")
    print(chunk[empty_mask][['SUB_ID', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'NAME', 'CITY', 'STATE']].head(10))
else:
    print("No empty/None values found in sample")

# Show sample of valid-looking dates
print("\n" + "=" * 80)
print("Sample Valid-Looking TRANSACTION_DT Values")
print("=" * 80)
valid_looking = chunk[
    chunk['TRANSACTION_DT'].notna() & 
    (chunk['TRANSACTION_DT'] != '') & 
    (chunk['TRANSACTION_DT'].str.len() == 8) &
    chunk['TRANSACTION_DT'].str.isdigit()
]
if len(valid_looking) > 0:
    print(f"Found {len(valid_looking)} rows with 8-digit numeric TRANSACTION_DT")
    print("\nFirst 20 rows:")
    print(valid_looking[['SUB_ID', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'NAME']].head(20))
    
    # Try to parse some dates
    print("\n" + "=" * 80)
    print("Date Parsing Test")
    print("=" * 80)
    sample_dates = valid_looking['TRANSACTION_DT'].head(10)
    print("\nTrying to parse sample dates as MMDDYYYY:")
    for date_str in sample_dates:
        try:
            from datetime import datetime
            parsed = datetime.strptime(date_str, '%m%d%Y')
            print(f"  {date_str} -> {parsed.date()}")
        except ValueError as e:
            print(f"  {date_str} -> ERROR: {e}")
else:
    print("No valid-looking 8-digit dates found in sample")

# Check a specific contribution ID that we know has None
print("\n" + "=" * 80)
print("Checking Specific Contribution IDs from Logs")
print("=" * 80)
problem_ids = ['4091020251290543102', '4091020251290543103', '4091020251290543096']
for contrib_id in problem_ids:
    matching = chunk[chunk['SUB_ID'] == contrib_id]
    if len(matching) > 0:
        print(f"\nContribution ID: {contrib_id}")
        print(matching[['SUB_ID', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'NAME', 'CITY', 'STATE']].to_dict('records'))
    else:
        print(f"\nContribution ID {contrib_id} not found in first 10,000 rows")

print("\n" + "=" * 80)
print("Analysis Complete")
print("=" * 80)

