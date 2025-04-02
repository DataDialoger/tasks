import sqlite3
import os

def connect_to_sqlite_db(db_path):
    """Connect to a SQLite database at the given path."""
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' does not exist.")
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def get_tables(connection):
    """Get all tables from the connected database."""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        return tables
    except sqlite3.Error as e:
        print(f"Error getting tables: {e}")
        return []

def main():
    """Main function to run the SQLite agent."""
    print("SQLite Database Agent")
    
    # Ask for database path
    db_path = input("Enter the path to your SQLite database: ")
    
    # Connect to the database
    connection = connect_to_sqlite_db(db_path)
    if not connection:
        print("Failed to connect to database.")
        return
    
    print(f"Successfully connected to database: {db_path}")
    
    # Get tables
    tables = get_tables(connection)
    
    if not tables:
        print("No tables found in the database.")
        connection.close()
        return
    
    # Display tables
    print("\nAvailable tables:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    # Ask user to select a table
    while True:
        try:
            selection = input("\nSelect a table (enter the number): ")
            table_index = int(selection) - 1
            
            if 0 <= table_index < len(tables):
                selected_table = tables[table_index]
                print(f"You selected: {selected_table}")
                break
            else:
                print(f"Please enter a number between 1 and {len(tables)}.")
        except ValueError:
            print("Please enter a valid number.")
    
    # Close the connection
    connection.close()
    print("Connection closed.")

if _name_ == "_main_":
    main()
