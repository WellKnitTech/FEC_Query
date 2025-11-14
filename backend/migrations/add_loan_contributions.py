"""
Migration script to add loan_contributions column to financial_totals table
Run this script to update existing databases.
"""
import asyncio
import sqlite3
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "fec_data.db"


async def migrate():
    """Add loan_contributions column to financial_totals table"""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("The column will be added automatically when the table is created.")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # Check if column already exists
        cursor.execute("PRAGMA table_info(financial_totals)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'loan_contributions' in columns:
            print("Column 'loan_contributions' already exists. Migration not needed.")
            return
        
        # Add the column
        print("Adding loan_contributions column to financial_totals table...")
        cursor.execute("""
            ALTER TABLE financial_totals 
            ADD COLUMN loan_contributions REAL DEFAULT 0.0
        """)
        
        conn.commit()
        print("✓ Successfully added loan_contributions column")
        
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
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
    asyncio.run(migrate())

