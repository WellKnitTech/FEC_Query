"""
Migration script to add missing FEC fields to operating_expenditures table
Adds: amendment_indicator, report_year, report_type, image_number, line_number,
      form_type_code, schedule_type_code, transaction_pgi, category, category_description,
      memo_code, memo_text, entity_type, file_number, transaction_id, back_reference_transaction_id
Run this script to update existing databases.
"""
import sqlite3
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "fec_data.db"


def migrate():
    """Add missing FEC fields to operating_expenditures table"""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("The columns will be added automatically when the table is created.")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # Check if columns already exist
        cursor.execute("PRAGMA table_info(operating_expenditures)")
        columns = [col[1] for col in cursor.fetchall()]
        
        columns_to_add = [
            ('amendment_indicator', 'VARCHAR', 'NULL'),
            ('report_year', 'INTEGER', 'NULL'),
            ('report_type', 'VARCHAR', 'NULL'),
            ('image_number', 'VARCHAR', 'NULL'),
            ('line_number', 'VARCHAR', 'NULL'),
            ('form_type_code', 'VARCHAR', 'NULL'),
            ('schedule_type_code', 'VARCHAR', 'NULL'),
            ('transaction_pgi', 'VARCHAR', 'NULL'),
            ('category', 'VARCHAR', 'NULL'),
            ('category_description', 'VARCHAR', 'NULL'),
            ('memo_code', 'VARCHAR', 'NULL'),
            ('memo_text', 'TEXT', 'NULL'),
            ('entity_type', 'VARCHAR', 'NULL'),
            ('file_number', 'VARCHAR', 'NULL'),
            ('transaction_id', 'VARCHAR', 'NULL'),
            ('back_reference_transaction_id', 'VARCHAR', 'NULL'),
        ]
        
        existing_columns = [col[0] for col in columns_to_add if col[0] in columns]
        if existing_columns:
            print(f"Some columns already exist: {existing_columns}")
            columns_to_add = [col for col in columns_to_add if col[0] not in columns]
        
        if not columns_to_add:
            print("All operating expenditure fields already exist. Migration not needed.")
            return
        
        # Add the columns
        for col_name, col_type, default in columns_to_add:
            print(f"Adding {col_name} column to operating_expenditures table...")
            cursor.execute(f"""
                ALTER TABLE operating_expenditures 
                ADD COLUMN {col_name} {col_type}
            """)
        
        # Create indexes
        print("Creating indexes...")
        indexes_to_create = [
            ('idx_op_exp_report_type', 'report_type'),
            ('idx_op_exp_category', 'category'),
            ('idx_op_exp_entity_type', 'entity_type'),
            ('idx_op_exp_transaction_id', 'transaction_id'),
        ]
        
        for idx_name, col_name in indexes_to_create:
            try:
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS {idx_name} ON operating_expenditures({col_name})
                """)
                print(f"  Created index {idx_name}")
            except sqlite3.OperationalError as e:
                if "already exists" in str(e).lower():
                    print(f"  Index {idx_name} already exists")
                else:
                    print(f"  Warning: Could not create index {idx_name}: {e}")
        
        conn.commit()
        print("✓ Successfully added operating expenditure fields and indexes")
        
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

