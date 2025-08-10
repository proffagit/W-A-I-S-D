import sqlite3
import os
from typing import Dict, Union


def create_sqlite_db(
    db_name: str,
    table_schema: Dict[str, Dict[str, Union[str, bool, int]]],
    db_path: str = "."
) -> bool:
    """
    Creates a SQLite database if it doesn't exist with specified tables and columns.
    
    Parameters:
    -----------
    db_name : str
        Name of the database file (without .db extension)
    table_schema : Dict[str, Dict[str, Union[str, bool, int]]]
        Dictionary mapping table names to their column definitions.
        Table names are the keys, and each table's columns are defined as:
        - column_name: {
            'type': str (e.g., 'TEXT', 'INTEGER', 'REAL', 'BLOB'),
            'primary_key': bool (optional, default False),
            'not_null': bool (optional, default False),
            'unique': bool (optional, default False),
            'default': str/int/float (optional),
            'foreign_key': str (optional, format: 'table(column)')
          }
    db_path : str
        Path where the database file should be created (default: current directory)
    
    Returns:
    --------
    bool
        True if database was created successfully, False otherwise
    
    Example:
    --------
    table_schema = {
        'users': {
            'id': {'type': 'INTEGER', 'primary_key': True, 'not_null': True},
            'username': {'type': 'TEXT', 'not_null': True, 'unique': True},
            'email': {'type': 'TEXT', 'not_null': True},
            'age': {'type': 'INTEGER', 'default': 0},
            'created_at': {'type': 'TIMESTAMP', 'default': 'CURRENT_TIMESTAMP'}
        },
        'posts': {
            'id': {'type': 'INTEGER', 'primary_key': True, 'not_null': True},
            'title': {'type': 'TEXT', 'not_null': True},
            'content': {'type': 'TEXT'},
            'user_id': {'type': 'INTEGER', 'foreign_key': 'users(id)'}
        }
    }
    
    create_sqlite_db('my_app', table_schema)
    """
    
    try:
        # Ensure db_name has .db extension
        if not db_name.endswith('.db'):
            db_name += '.db'
        
        # Create full path to database
        full_db_path = os.path.join(db_path, db_name)
        
        # Check if database already exists
        db_exists = os.path.exists(full_db_path)
        
        # Connect to database (creates it if it doesn't exist)
        conn = sqlite3.connect(full_db_path)
        cursor = conn.cursor()
        
        print(f"{'Connected to existing' if db_exists else 'Created new'} database: {full_db_path}")
        
        # Create tables
        for table_name in table_schema.keys():
            # Check if table already exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            if cursor.fetchone():
                print(f"Table '{table_name}' already exists. Skipping creation.")
                continue
            
            # Build CREATE TABLE statement
            column_definitions = []
            foreign_keys = []
            
            for col_name, col_props in table_schema[table_name].items():
                col_def = f"{col_name} {col_props['type']}"
                
                # Add constraints
                if col_props.get('primary_key', False):
                    col_def += " PRIMARY KEY"
                
                if col_props.get('not_null', False):
                    col_def += " NOT NULL"
                
                if col_props.get('unique', False):
                    col_def += " UNIQUE"
                
                if 'default' in col_props:
                    default_val = col_props['default']
                    if isinstance(default_val, str) and default_val != 'CURRENT_TIMESTAMP':
                        col_def += f" DEFAULT '{default_val}'"
                    else:
                        col_def += f" DEFAULT {default_val}"
                
                column_definitions.append(col_def)
                
                # Handle foreign keys
                if 'foreign_key' in col_props:
                    fk_ref = col_props['foreign_key']
                    foreign_keys.append(f"FOREIGN KEY ({col_name}) REFERENCES {fk_ref}")
            
            # Combine column definitions and foreign keys
            all_definitions = column_definitions + foreign_keys
            
            # Create the table
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(all_definitions)}
            )
            """
            
            cursor.execute(create_table_sql)
            print(f"Created table '{table_name}' with {len(table_schema[table_name])} columns")
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        print(f"Database '{db_name}' setup completed successfully!")
        return True
        
    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
        if 'conn' in locals():
            conn.close()
        return False
    
    except Exception as e:
        print(f"An error occurred: {e}")
        if 'conn' in locals():
            conn.close()
        return False


def get_table_info(db_name: str, table_name: str = None, db_path: str = ".") -> Dict:
    """
    Get information about tables in the database.
    
    Parameters:
    -----------
    db_name : str
        Name of the database file
    table_name : str, optional
        Specific table name to get info for. If None, returns info for all tables.
    db_path : str
        Path where the database file is located
    
    Returns:
    --------
    Dict
        Dictionary containing table information
    """
    try:
        if not db_name.endswith('.db'):
            db_name += '.db'
        
        full_db_path = os.path.join(db_path, db_name)
        
        if not os.path.exists(full_db_path):
            return {"error": f"Database {db_name} not found"}
        
        conn = sqlite3.connect(full_db_path)
        cursor = conn.cursor()
        
        result = {}
        
        if table_name:
            # Get info for specific table
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            result[table_name] = {
                "columns": [{"name": col[1], "type": col[2], "not_null": bool(col[3]), 
                           "default": col[4], "primary_key": bool(col[5])} for col in columns]
            }
        else:
            # Get info for all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                result[table_name] = {
                    "columns": [{"name": col[1], "type": col[2], "not_null": bool(col[3]), 
                               "default": col[4], "primary_key": bool(col[5])} for col in columns]
                }
        
        conn.close()
        return result
        
    except sqlite3.Error as e:
        return {"error": f"SQLite error: {e}"}
    except Exception as e:
        return {"error": f"Error: {e}"}