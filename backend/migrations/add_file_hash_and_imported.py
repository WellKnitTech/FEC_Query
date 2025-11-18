"""
Migration script to add file_hash and imported columns to bulk_data_metadata table
Run this script to update existing databases.
"""
import sqlite3
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "fec_data.db"


def migrate():
    """Add file_hash and imported columns to bulk_data_metadata table"""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("The columns will be added automatically when the table is created.")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # Check if columns already exist
        cursor.execute("PRAGMA table_info(bulk_data_metadata)")
        columns = [col[1] for col in cursor.fetchall()]
        
        changes_made = False
        
        # Add file_hash column
        if 'file_hash' not in columns:
            print("Adding file_hash column to bulk_data_metadata table...")
            cursor.execute("""
                ALTER TABLE bulk_data_metadata 
                ADD COLUMN file_hash TEXT
            """)
            changes_made = True
        else:
            print("Column 'file_hash' already exists. Skipping.")
        
        # Add imported column
        if 'imported' not in columns:
            print("Adding imported column to bulk_data_metadata table...")
            cursor.execute("""
                ALTER TABLE bulk_data_metadata 
                ADD COLUMN imported INTEGER DEFAULT 0
            """)
            changes_made = True
        else:
            print("Column 'imported' already exists. Skipping.")
        
        # Create index on file_hash if it doesn't exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_file_hash'")
        if not cursor.fetchone():
            print("Creating index on file_hash...")
            cursor.execute("""
                CREATE INDEX idx_file_hash ON bulk_data_metadata(file_hash)
            """)
            changes_made = True
        else:
            print("Index 'idx_file_hash' already exists. Skipping.")
        
        # Create index on imported if it doesn't exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_imported'")
        if not cursor.fetchone():
            print("Creating index on imported...")
            cursor.execute("""
                CREATE INDEX idx_imported ON bulk_data_metadata(imported)
            """)
            changes_made = True
        else:
            print("Index 'idx_imported' already exists. Skipping.")
        
        if changes_made:
            conn.commit()
            print("✓ Successfully added file_hash and imported columns with indexes")
        else:
            print("✓ All columns and indexes already exist. Migration not needed.")
        
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
            print("Columns already exist (detected via error). Migration not needed.")
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

