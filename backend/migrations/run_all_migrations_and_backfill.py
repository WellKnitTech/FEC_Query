"""
Combined script to run migrations and backfill data
1. Runs add_contribution_fields.py migration
2. Runs add_operating_expenditure_fields.py migration  
3. Runs backfill_contribution_fields.py to populate from raw_data
4. Runs backfill_operating_expenditure_fields.py to populate from raw_data
"""
import subprocess
import sys
from pathlib import Path

def run_script(script_name, description):
    """Run a migration or backfill script"""
    print("\n" + "=" * 80)
    print(f"{description}")
    print("=" * 80)
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"⚠️  Script not found: {script_path}")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent)
        )
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        if result.returncode == 0:
            print(f"✓ {description} completed successfully")
            return True
        else:
            print(f"✗ {description} failed with exit code {result.returncode}")
            return False
    except Exception as e:
        print(f"✗ Error running {script_name}: {e}")
        return False


def main():
    """Run all migrations and backfills"""
    print("=" * 80)
    print("RUNNING ALL MIGRATIONS AND BACKFILLS")
    print("=" * 80)
    print()
    print("This script will:")
    print("  1. Add new columns to contributions table")
    print("  2. Add new columns to operating_expenditures table")
    print("  3. Backfill contribution fields from raw_data")
    print("  4. Backfill operating expenditure fields from raw_data")
    print()
    
    results = []
    
    # Run migrations
    results.append((
        "add_contribution_fields.py",
        run_script("add_contribution_fields.py", "Adding Contribution Fields Migration")
    ))
    
    results.append((
        "add_operating_expenditure_fields.py",
        run_script("add_operating_expenditure_fields.py", "Adding Operating Expenditure Fields Migration")
    ))
    
    # Run backfills
    results.append((
        "backfill_contribution_fields.py",
        run_script("backfill_contribution_fields.py", "Backfilling Contribution Fields")
    ))
    
    results.append((
        "backfill_operating_expenditure_fields.py",
        run_script("backfill_operating_expenditure_fields.py", "Backfilling Operating Expenditure Fields")
    ))
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    success_count = sum(1 for _, success in results if success)
    total_count = len(results)
    
    for script_name, success in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"  {status}: {script_name}")
    
    print()
    print(f"Completed {success_count}/{total_count} operations")
    
    if success_count == total_count:
        print("✓ All operations completed successfully!")
        return 0
    else:
        print("⚠️  Some operations failed. Please review the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

