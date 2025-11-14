"""
Migration script to add resume support columns to bulk_import_jobs table
Run this script to update existing databases.
"""
import sqlite3
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "fec_data.db"


def migrate():
    """Add file_position, data_type, and file_path columns to bulk_import_jobs table"""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("The columns will be added automatically when the table is created.")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # Check if columns already exist
        cursor.execute("PRAGMA table_info(bulk_import_jobs)")
        columns = [col[1] for col in cursor.fetchall()]
        
        columns_to_add = []
        if 'file_position' not in columns:
            columns_to_add.append(('file_position', 'INTEGER', '0'))
        if 'data_type' not in columns:
            columns_to_add.append(('data_type', 'VARCHAR', 'NULL'))
        if 'file_path' not in columns:
            columns_to_add.append(('file_path', 'VARCHAR', 'NULL'))
        
        if not columns_to_add:
            print("All resume columns already exist. Migration not needed.")
            return
        
        # Add the columns
        for col_name, col_type, default in columns_to_add:
            print(f"Adding {col_name} column to bulk_import_jobs table...")
            if default == 'NULL':
                cursor.execute(f"""
                    ALTER TABLE bulk_import_jobs 
                    ADD COLUMN {col_name} {col_type}
                """)
            else:
                cursor.execute(f"""
                    ALTER TABLE bulk_import_jobs 
                    ADD COLUMN {col_name} {col_type} DEFAULT {default}
                """)
        
        conn.commit()
        print("✓ Successfully added resume support columns")
        
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

