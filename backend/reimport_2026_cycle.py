#!/usr/bin/env python3
"""
Script to re-import 2026 cycle Schedule A (pas2) data to fix missing/incorrect dates.
This will use smart merge to update existing contributions with correct dates.
"""
import asyncio
import sys
import logging

sys.path.insert(0, '.')

from app.services.bulk_data import BulkDataService, DataType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def reimport_2026_cycle(force_download: bool = False):
    """Re-import 2026 cycle Schedule A data"""
    print("=" * 80)
    print("RE-IMPORTING 2026 CYCLE SCHEDULE A DATA")
    print("=" * 80)
    print()
    print("This will:")
    print("  1. Download/use existing 2026 cycle Schedule A (pas2) bulk data")
    print("  2. Parse and import contributions with proper dates")
    print("  3. Use smart merge to update existing contributions:")
    print("     - Will fix NULL dates")
    print("     - Will overwrite incorrect dates with correct ones")
    print("     - Will preserve other existing data")
    print()
    
    if not force_download:
        print("Note: Will use existing downloaded file if available.")
        print("      Use --force-download to re-download from FEC.")
        print()
    
    bulk_service = BulkDataService()
    
    try:
        print("Starting import...")
        print()
        
        # Use INDIVIDUAL_CONTRIBUTIONS (Schedule A) - this is the main contributions data
        result = await bulk_service.download_and_import_data_type(
            data_type=DataType.INDIVIDUAL_CONTRIBUTIONS,  # Schedule A (indiv*.zip)
            cycle=2026,
            batch_size=50000,
            force_download=force_download
        )
        
        if result.get('success'):
            print()
            print("=" * 80)
            print("✅ RE-IMPORT COMPLETE")
            print("=" * 80)
            print(f"Records imported: {result.get('record_count', 0):,}")
            print()
            print("The following should now be fixed:")
            print("  ✅ Contributions with NULL dates should now have dates")
            print("  ✅ Contributions with incorrect dates should be corrected")
            print("  ✅ Missing 2026 cycle contributions should be added")
            print()
            print("You can verify by checking:")
            print("  - Contribution Analysis should show 2026 cycle data")
            print("  - Dates should be in 2025-2026 range")
        else:
            print()
            print("=" * 80)
            print("❌ RE-IMPORT FAILED")
            print("=" * 80)
            print(f"Error: {result.get('error', 'Unknown error')}")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Error during re-import: {e}", exc_info=True)
        print()
        print("=" * 80)
        print("❌ RE-IMPORT FAILED")
        print("=" * 80)
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Re-import 2026 cycle Schedule A data to fix dates')
    parser.add_argument('--force-download', action='store_true', 
                       help='Force re-download of bulk data file (even if it exists)')
    
    args = parser.parse_args()
    
    exit_code = asyncio.run(reimport_2026_cycle(force_download=args.force_download))
    sys.exit(exit_code)

