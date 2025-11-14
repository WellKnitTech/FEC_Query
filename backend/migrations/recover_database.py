"""
Database recovery script for SQLite corruption issues
This script attempts to recover data from a corrupted database.
"""
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

# Database paths
DB_PATH = Path(__file__).parent.parent / "fec_data.db"
BACKUP_PATH = Path(__file__).parent.parent / f"fec_data.db.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
RECOVERED_PATH = Path(__file__).parent.parent / "fec_data.db.recovered"


def recover_database():
    """Attempt to recover the corrupted database"""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        return False
    
    print(f"Database size: {DB_PATH.stat().st_size / (1024*1024):.2f} MB")
    
    # Step 1: Create backup
    print("\n1. Creating backup...")
    try:
        shutil.copy2(DB_PATH, BACKUP_PATH)
        print(f"✓ Backup created: {BACKUP_PATH}")
    except Exception as e:
        print(f"✗ Failed to create backup: {e}")
        return False
    
    # Step 2: Checkpoint WAL file if it exists
    print("\n2. Checkpointing WAL file...")
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("PRAGMA wal_checkpoint(FULL)")
        conn.close()
        print("✓ WAL checkpointed")
    except Exception as e:
        print(f"⚠ WAL checkpoint warning: {e}")
    
    # Step 3: Attempt recovery using .recover
    print("\n3. Attempting database recovery...")
    try:
        # SQLite's .recover command dumps all recoverable data
        recovered_conn = sqlite3.connect(str(RECOVERED_PATH))
        
        # Use .recover to dump and restore
        with open(DB_PATH, 'rb') as corrupted_db:
            # Read the corrupted database and try to extract data
            pass
        
        # Alternative: Use dump and restore
        print("   Dumping recoverable data...")
        dump_file = Path(__file__).parent.parent / "database_dump.sql"
        
        # Try to dump what we can
        conn = sqlite3.connect(str(DB_PATH))
        with open(dump_file, 'w') as f:
            for line in conn.iterdump():
                try:
                    f.write(f"{line}\n")
                except:
                    # Skip corrupted parts
                    continue
        conn.close()
        
        print(f"✓ Dump created: {dump_file}")
        print(f"   Note: Some data may be lost due to corruption")
        
        # Restore to new database
        print("   Restoring to new database...")
        recovered_conn = sqlite3.connect(str(RECOVERED_PATH))
        with open(dump_file, 'r') as f:
            recovered_conn.executescript(f.read())
        recovered_conn.close()
        
        print(f"✓ Recovered database created: {RECOVERED_PATH}")
        return True
        
    except Exception as e:
        print(f"✗ Recovery failed: {e}")
        return False


def verify_recovered():
    """Verify the recovered database"""
    print("\n4. Verifying recovered database...")
    try:
        conn = sqlite3.connect(str(RECOVERED_PATH))
        result = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        
        if result and result[0] == "ok":
            print("✓ Recovered database integrity check passed")
            return True
        else:
            print(f"⚠ Integrity check: {result}")
            return False
    except Exception as e:
        print(f"✗ Verification failed: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("SQLite Database Recovery Tool")
    print("=" * 60)
    
    if recover_database():
        if verify_recovered():
            print("\n" + "=" * 60)
            print("RECOVERY SUCCESSFUL!")
            print("=" * 60)
            print(f"\nNext steps:")
            print(f"1. Stop the application")
            print(f"2. Replace the corrupted database:")
            print(f"   mv {DB_PATH} {DB_PATH}.corrupted")
            print(f"   mv {RECOVERED_PATH} {DB_PATH}")
            print(f"3. Restart the application")
        else:
            print("\n⚠ Recovery completed but verification failed")
            print("You may need to restore from a backup or re-import data")
    else:
        print("\n✗ Recovery failed")
        print("You may need to:")
        print("1. Restore from a known good backup")
        print("2. Re-import data from FEC API")
        print(f"3. Check disk health: badblocks or fsck")

