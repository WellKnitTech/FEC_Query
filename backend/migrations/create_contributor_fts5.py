"""
Migration to create FTS5 full-text search table for contributor names

This creates a virtual FTS5 table that enables fast full-text search
on contributor names, making donor searches 10-100x faster.

Run this migration to enable FTS5 search:
    python migrations/create_contributor_fts5.py
"""
import sqlite3
import os
import sys
from pathlib import Path

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import config

def migrate():
    """Create FTS5 virtual table for contributor names"""
    # Extract database path from DATABASE_URL
    db_url = config.DATABASE_URL
    if db_url.startswith("sqlite+aiosqlite:///"):
        db_path = db_url.replace("sqlite+aiosqlite:///", "")
    elif db_url.startswith("sqlite:///"):
        db_path = db_url.replace("sqlite:///", "")
    else:
        db_path = db_url
    
    # Handle relative paths
    if db_path.startswith("./"):
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), db_path[2:])
    elif not os.path.isabs(db_path):
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), db_path)
    
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        print(f"Looking for database file...")
        # Try common locations
        possible_paths = [
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "fec_data.db"),
            "./fec_data.db",
            "fec_data.db"
        ]
        for path in possible_paths:
            if os.path.exists(path):
                db_path = path
                print(f"Found database at {db_path}")
                break
        else:
            print("Could not find database file")
            return
    
    print(f"Creating FTS5 table for contributor names in {db_path}...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if FTS5 extension is available
        cursor.execute("PRAGMA compile_options")
        compile_options = [row[0] for row in cursor.fetchall()]
        if "ENABLE_FTS5" not in str(compile_options):
            print("WARNING: FTS5 extension may not be enabled in this SQLite build")
            print("FTS5 search will fall back to regular search")
        
        # Drop existing FTS5 table if it exists (for re-migration)
        cursor.execute("DROP TABLE IF EXISTS contributions_fts")
        
        # Create FTS5 virtual table
        # FTS5 tables are read-only from SQLAlchemy, so we use raw SQL
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS contributions_fts USING fts5(
                contributor_name,
                content='contributions',
                content_rowid='id'
            )
        """)
        
        print("  Created FTS5 virtual table 'contributions_fts'")
        
        # Populate FTS5 table with existing contributor names
        print("  Populating FTS5 table with existing contributor names...")
        cursor.execute("""
            INSERT INTO contributions_fts(rowid, contributor_name)
            SELECT id, contributor_name
            FROM contributions
            WHERE contributor_name IS NOT NULL AND contributor_name != ''
        """)
        
        row_count = cursor.rowcount
        print(f"  Populated {row_count} contributor names into FTS5 table")
        
        # Create triggers to keep FTS5 table in sync with contributions table
        print("  Creating triggers to keep FTS5 table in sync...")
        
        # Trigger for INSERT
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS contributions_fts_insert AFTER INSERT ON contributions
            BEGIN
                INSERT INTO contributions_fts(rowid, contributor_name)
                VALUES (new.id, new.contributor_name);
            END
        """)
        
        # Trigger for UPDATE
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS contributions_fts_update AFTER UPDATE ON contributions
            WHEN old.contributor_name IS NOT new.contributor_name
            BEGIN
                UPDATE contributions_fts
                SET contributor_name = new.contributor_name
                WHERE rowid = new.id;
            END
        """)
        
        # Trigger for DELETE
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS contributions_fts_delete AFTER DELETE ON contributions
            BEGIN
                DELETE FROM contributions_fts WHERE rowid = old.id;
            END
        """)
        
        print("  Created sync triggers")
        
        # Create index on FTS5 table for better performance
        # FTS5 automatically creates indexes, but we can optimize
        cursor.execute("""
            INSERT INTO contributions_fts(contributions_fts) VALUES('rebuild')
        """)
        
        print("  Rebuilt FTS5 index")
        
        conn.commit()
        print("✓ Successfully created and populated FTS5 table")
        print("\nFTS5 search is now enabled. Donor searches will be significantly faster.")
        
    except sqlite3.OperationalError as e:
        if "no such module: fts5" in str(e).lower():
            print("ERROR: FTS5 extension is not available in this SQLite build")
            print("The system will fall back to regular search")
            print("To enable FTS5, you may need to recompile SQLite with FTS5 support")
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

