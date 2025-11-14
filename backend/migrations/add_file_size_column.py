"""
Migration script to add file_size column to bulk_data_metadata table
Run this script to update existing databases.
"""
import sqlite3
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "fec_data.db"


def migrate():
    """Add file_size column to bulk_data_metadata table"""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("The column will be added automatically when the table is created.")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # Check if column already exists
        cursor.execute("PRAGMA table_info(bulk_data_metadata)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'file_size' in columns:
            print("Column 'file_size' already exists. Migration not needed.")
            return
        
        # Add the column
        print("Adding file_size column to bulk_data_metadata table...")
        cursor.execute("""
            ALTER TABLE bulk_data_metadata 
            ADD COLUMN file_size INTEGER
        """)
        
        conn.commit()
        print("✓ Successfully added file_size column")
        
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
            print("Column already exists (detected via error). Migration not needed.")
        else:
            print(f"Error during migration: {e}")
            conn.rollback()
            raise
    except Exception as e:
        print(f"Unexpected error during migration: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()

