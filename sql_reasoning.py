import sqlite3
import re
import os
import datetime
from tabulate import tabulate

class RealTimeDBManager:
    def _init_(self, db_path=None):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.tables = {}
        self.history = []
        self.last_query_time = 0
        
    def connect(self, db_path=None):
        if db_path:
            self.db_path = db_path
            
        if not self.db_path:
            while not self.db_path:
                path = input("Enter database path (or :memory: for in-memory database): ").strip()
                if path:
                    self.db_path = path
        
        if self.db_path != ':memory:' and not os.path.exists(self.db_path):
            confirmation = input(f"Database '{self.db_path}' doesn't exist. Create it? (y/n): ")
            if confirmation.lower() != 'y':
                print("Database connection cancelled.")
                return False
                
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            self.refresh_metadata()
            print(f"✓ Connected to database: {self.db_path}")
            print(f"  Available tables: {', '.join(self.tables.keys()) if self.tables else 'None'}")
            return True
        except sqlite3.Error as e:
            print(f"✗ Database connection error: {e}")
            return False
            
    def close(self):
        if self.conn:
            self.conn.close()
            print(f"✓ Connection to {self.db_path} closed")
            self.conn = None
            self.cursor = None
            self.tables = {}
            
    def refresh_metadata(self):
        if not self.conn:
            return {}
            
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        self.tables = {table[0]: self.get_columns_with_types(table[0]) for table in self.cursor.fetchall() if not table[0].startswith('sqlite')}
        return self.tables
        
    def _get_columns_with_types(self, table):
        self.cursor.execute(f"PRAGMA table_info({table})")
        return {col[1]: col[2] for col in self.cursor.fetchall()}

    def parse_user_input(self, user_input):
        original_input = user_input
        
        words = re.findall(r'\b\w+\b|[><=!~]+', user_input.lower())
        
        # Check if it's a direct SQL query
        if user_input.strip().upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP')):
            return {
                'type': 'direct_sql',
                'query': user_input,
                'original': original_input
            }
            
        # Process as natural language
        intent = self._detect_intent(words)
        tables = self._extract_tables(words)
        
        if tables:
            primary_table = tables[0]
            columns = self._extract_columns(words, primary_table)
            conditions, values = self._extract_conditions(words, primary_table)
            
            return {
                'type': 'nl_query',
                'intent': intent,
                'tables': tables,
                'primary_table': primary_table,
                'columns': columns,
                'conditions': conditions,
                'values': values,
                'original': original_input
            }
        else:
            # Check for schema-related commands
            if any(word in words for word in ['show', 'list', 'describe', 'schema']):
                if 'tables' in words:
                    return {
                        'type': 'schema_query',
                        'command': 'list_tables',
                        'original': original_input
                    }
                elif any(word in words for word in ['schema', 'structure', 'describe']):
                    # Find table name
                    for word in words:
                        if word in self.tables:
                            return {
                                'type': 'schema_query',
                                'command': 'describe_table',
                                'table': word,
                                'original': original_input
                            }
            
            return {
                'type': 'unknown',
                'original': original_input
            }

    def _detect_intent(self, words):
        intent_keywords = {
            'SELECT': ['select', 'get', 'show', 'find', 'search', 'display', 'list', 'query', 'view', 'retrieve', 'fetch'],
            'INSERT': ['insert', 'add', 'create', 'new', 'register', 'save', 'store', 'put'],
            'UPDATE': ['update', 'change', 'modify', 'edit', 'alter', 'replace', 'set'],
            'DELETE': ['delete', 'remove', 'drop', 'clear', 'erase', 'eliminate']
        }
        
        for intent, keywords in intent_keywords.items():
            if any(word in words for word in keywords):
                return intent
        
        # Default to SELECT if we found tables but no intent
        for word in words:
            if word in self.tables:
                return 'SELECT'
                
        return None

    def _extract_tables(self, words):
        tables = []
        
        # First, try exact matches
        for word in words:
            if word in self.tables:
                tables.append(word)
                
        # If no tables found, try plurals/singulars
        if not tables:
            for word in words:
                # Check for plural forms
                if word.endswith('s') and word[:-1] in self.tables:
                    tables.append(word[:-1])
                # Check for singular forms
                elif word + 's' in self.tables:
                    tables.append(word + 's')
                    
        return tables

    def _extract_columns(self, words, table):
        if not table or table not in self.tables:
            return []
            
        table_cols = list(self.tables[table].keys())
        
        # Extract exact column matches
        columns = [word for word in words if word in table_cols]
        
        # Handle "all" keywords
        if any(word in words for word in ['all', 'everything']) or '*' in words or not columns:
            return ['*']
            
        return columns

    def _extract_conditions(self, words, table):
        if not table or table not in self.tables:
            return [], []
            
        table_cols = list(self.tables[table].keys())
        conditions = []
        values = []
        operators = ['=', '>', '<', '>=', '<=', '!=', 'like', 'contains', 'starts', 'ends']
        
        i = 0
        while i < len(words) - 2:
            if words[i] in table_cols:
                op = words[i + 1]
                if op in operators:
                    # Handle special operators
                    if op == 'contains':
                        conditions.append(f"{words[i]} LIKE ?")
                        values.append(f"%{words[i + 2]}%")
                    elif op == 'starts':
                        conditions.append(f"{words[i]} LIKE ?")
                        values.append(f"{words[i + 2]}%")
                    elif op == 'ends':
                        conditions.append(f"{words[i]} LIKE ?")
                        values.append(f"%{words[i + 2]}")
                    elif op == 'like':
                        conditions.append(f"{words[i]} LIKE ?")
                        values.append(f"%{words[i + 2]}%")
                    else:
                        conditions.append(f"{words[i]} {op} ?")
                        values.append(words[i + 2])
                    i += 3
                else:
                    i += 1
            else:
                i += 1
                
        return conditions, values

    def analyze_query(self, parsed_query):
        if parsed_query['type'] == 'direct_sql':
            # Explain direct SQL
            query = parsed_query['query'].strip()
            first_word = query.split()[0].upper()
            
            explanation = {
                'action': first_word,
                'explanation': f"Executing raw SQL {first_word} statement",
                'details': "This will be executed exactly as written without any interpretation.",
                'risk_level': 'medium' if first_word in ['UPDATE', 'DELETE', 'DROP', 'ALTER'] else 'low',
                'query': query
            }
            
        elif parsed_query['type'] == 'nl_query':
            # Explain natural language query
            intent = parsed_query['intent']
            tables = parsed_query['tables']
            columns = parsed_query['columns']
            conditions = parsed_query['conditions']
            
            if not intent or not tables:
                return {
                    'action': 'Unknown',
                    'explanation': "Could not understand the query",
                    'details': "Try rephrasing with clearer table and action words.",
                    'risk_level': 'none'
                }
                
            table = tables[0]
            
            if intent == 'SELECT':
                sql_query = f"SELECT {', '.join(columns)} FROM {table}"
                if conditions:
                    sql_query += f" WHERE {' AND '.join(conditions)}"
                    
                explanation = {
                    'action': 'SELECT',
                    'explanation': f"Retrieving data from '{table}'",
                    'details': f"Fetching {('all columns' if columns == ['*'] else ', '.join(columns))} " + 
                              (f"where {self._conditions_to_text(conditions)}" if conditions else "with no conditions"),
                    'risk_level': 'low',
                    'query': sql_query,
                    'params': parsed_query['values']
                }
                
            elif intent == 'INSERT':
                if columns and columns != ['*']:
                    explanation = {
                        'action': 'INSERT',
                        'explanation': f"Adding new record to '{table}'",
                        'details': f"Will prompt for values for: {', '.join(columns)}",
                        'risk_level': 'low',
                        'needs_input': columns
                    }
                else:
                    # Get all columns for the table if none specified
                    all_columns = list(self.tables[table].keys())
                    explanation = {
                        'action': 'INSERT',
                        'explanation': f"Adding new record to '{table}'",
                        'details': f"Will prompt for values for all columns: {', '.join(all_columns)}",
                        'risk_level': 'low',
                        'needs_input': all_columns
                    }
                    
            elif intent == 'UPDATE':
                if columns and columns != ['*']:
                    sql_base = f"UPDATE {table} SET {', '.join([f'{col} = ?' for col in columns])}"
                    if conditions:
                        sql_base += f" WHERE {' AND '.join(conditions)}"
                        
                    explanation = {
                        'action': 'UPDATE',
                        'explanation': f"Modifying records in '{table}'",
                        'details': f"Will update {', '.join(columns)} " + 
                                  (f"where {self._conditions_to_text(conditions)}" if conditions else "for ALL records"),
                        'risk_level': 'high' if not conditions else 'medium',
                        'needs_input': columns,
                        'query_base': sql_base,
                        'condition_params': parsed_query['values']
                    }
                else:
                    explanation = {
                        'action': 'UPDATE',
                        'explanation': f"Cannot update table '{table}'",
                        'details': "Please specify which columns to update",
                        'risk_level': 'none'
                    }
                    
            elif intent == 'DELETE':
                sql_query = f"DELETE FROM {table}"
                if conditions:
                    sql_query += f" WHERE {' AND '.join(conditions)}"
                    
                explanation = {
                    'action': 'DELETE',
                    'explanation': f"Removing records from '{table}'",
                    'details': (f"Will delete records where {self._conditions_to_text(conditions)}" if conditions 
                               else "⚠ Will delete ALL records in the table"),
                    'risk_level': 'critical' if not conditions else 'high',
                    'query': sql_query,
                    'params': parsed_query['values']
                }
                
            else:
                explanation = {
                    'action': 'Unknown',
                    'explanation': "Unsupported operation",
                    'details': "Only SELECT, INSERT, UPDATE, and DELETE are supported.",
                    'risk_level': 'none'
                }
                
        elif parsed_query['type'] == 'schema_query':
            if parsed_query['command'] == 'list_tables':
                explanation = {
                    'action': 'SCHEMA',
                    'explanation': "Listing all tables in database",
                    'details': f"Will show structure of {len(self.tables)} tables",
                    'risk_level': 'none'
                }
            elif parsed_query['command'] == 'describe_table':
                table = parsed_query['table']
                explanation = {
                    'action': 'SCHEMA',
                    'explanation': f"Describing structure of '{table}'",
                    'details': f"Will show columns, types, keys and sample data",
                    'risk_level': 'none'
                }
                
        else:
            explanation = {
                'action': 'Unknown',
                'explanation': "Could not understand the request",
                'details': "Try rephrasing or use direct SQL syntax",
                'risk_level': 'none'
            }
            
        return explanation
    
    def _conditions_to_text(self, conditions):
        readable_conditions = []
        for cond in conditions:
            # Replace SQL operators with readable text
            readable = cond.replace(' LIKE ?', ' contains [value]')
            readable = readable.replace(' = ?', ' equals [value]')
            readable = readable.replace(' > ?', ' is greater than [value]')
            readable = readable.replace(' < ?', ' is less than [value]')
            readable = readable.replace(' >= ?', ' is at least [value]')
            readable = readable.replace(' <= ?', ' is at most [value]')
            readable = readable.replace(' != ?', ' is not [value]')
            readable_conditions.append(readable)
            
        return ', '.join(readable_conditions)

    def execute_analyzed_query(self, analysis):
        if not self.conn:
            print("✗ Not connected to a database")
            return False
            
        action = analysis['action']
        
        # Handle schema queries
        if action == 'SCHEMA':
            if analysis['explanation'].startswith('Listing all tables'):
                self._display_all_tables_info()
                return True
            elif analysis['explanation'].startswith('Describing structure'):
                table = analysis['explanation'].split("'")[1]
                self._display_table_info(table)
                return True
        
        # Handle data manipulation queries
        if 'query' in analysis:
            # Direct execution with existing query
            query = analysis['query']
            params = analysis.get('params', [])
            
            try:
                start_time = datetime.datetime.now()
                self.cursor.execute(query, params)
                
                # Handle results based on query type
                if query.strip().upper().startswith('SELECT'):
                    rows = self.cursor.fetchall()
                    headers = [desc[0] for desc in self.cursor.description]
                    
                    if rows:
                        print(f"\n✓ Query returned {len(rows)} results:")
                        print(tabulate(rows, headers=headers, tablefmt='grid'))
                        
                        # Show query stats
                        end_time = datetime.datetime.now()
                        duration = (end_time - start_time).total_seconds()
                        print(f"  Query executed in {duration:.3f} seconds")
                        
                        # Offer to export results
                        export = input("Export results to CSV? (y/n): ").lower()
                        if export == 'y':
                            self._export_results_to_csv(rows, headers)
                    else:
                        print("✗ No records found matching your query")
                else:
                    self.conn.commit()
                    affected = self.cursor.rowcount
                    print(f"✓ {action} operation successful: {affected} rows affected")
                    
                # Log the query to history
                self._log_query(query, params, affected=self.cursor.rowcount)
                self.refresh_metadata()  # Update metadata after changes
                return True
                
            except sqlite3.Error as e:
                print(f"✗ Database error: {e}")
                return False
                
        elif 'needs_input' in analysis:
            # Handle queries needing user input (INSERT, UPDATE)
            if action == 'INSERT':
                # Collect values for all columns
                values = []
                for col in analysis['needs_input']:
                    col_type = self.tables[analysis['explanation'].split("'")[1]][col]
                    val = input(f"Enter value for '{col}' ({col_type}): ").strip()
                    
                    # Handle empty inputs for NULLs
                    if not val:
                        values.append(None)
                    else:
                        values.append(val)
                
                # Build and execute the INSERT query
                table = analysis['explanation'].split("'")[1]
                columns = analysis['needs_input']
                placeholders = ', '.join(['?' for _ in columns])
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
                
                try:
                    self.cursor.execute(query, values)
                    self.conn.commit()
                    print(f"✓ Record inserted successfully with ID: {self.cursor.lastrowid}")
                    self._log_query(query, values, affected=1)
                    self.refresh_metadata()
                    return True
                except sqlite3.Error as e:
                    print(f"✗ Insert error: {e}")
                    return False
                    
            elif action == 'UPDATE':
                # Collect new values for columns
                update_values = []
                for col in analysis['needs_input']:
                    col_type = self.tables[analysis['explanation'].split("'")[1]][col]
                    val = input(f"Enter new value for '{col}' ({col_type}): ").strip()
                    
                    # Handle empty inputs for NULLs
                    if not val:
                        update_values.append(None)
                    else:
                        update_values.append(val)
                
                # Execute the UPDATE with both update values and condition values
                query = analysis['query_base']
                all_params = tuple(update_values) + tuple(analysis['condition_params'])
                
                try:
                    self.cursor.execute(query, all_params)
                    self.conn.commit()
                    affected = self.cursor.rowcount
                    print(f"✓ Records updated successfully: {affected} rows affected")
                    self._log_query(query, all_params, affected=affected)
                    self.refresh_metadata()
                    return True
                except sqlite3.Error as e:
                    print(f"✗ Update error: {e}")
                    return False
        
        return False
        
    def _display_all_tables_info(self):
        if not self.tables:
            print("No tables found in database")
            return
            
        tables_info = []
        for table, columns in self.tables.items():
            # Get row count
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = self.cursor.fetchone()[0]
            
            # Get primary key info
            self.cursor.execute(f"PRAGMA table_info({table})")
            table_info = self.cursor.fetchall()
            pk_cols = [col[1] for col in table_info if col[5] > 0]
            
            # Get foreign key info
            self.cursor.execute(f"PRAGMA foreign_key_list({table})")
            fk_info = self.cursor.fetchall()
            fk_cols = [f"{col[3]} -> {col[2]}.{col[4]}" for col in fk_info]
            
            tables_info.append({
                'name': table,
                'columns': columns,
                'rows': row_count,
                'primary_keys': pk_cols,
                'foreign_keys': fk_cols
            })
        
        print(f"\n📊 Database: {self.db_path}")
        print(f"Found {len(tables_info)} tables:")
        
        for info in tables_info:
            print(f"\n📋 Table: {info['name']}")
            print(f"  Columns: {len(info['columns'])}")
            print(f"  Rows: {info['rows']}")
            
            # Display column information
            col_data = []
            for col, type_name in info['columns'].items():
                pk = "✓" if col in info['primary_keys'] else ""
                fk = ""
                for foreign_key in info['foreign_keys']:
                    if foreign_key.startswith(f"{col} ->"):
                        fk = "✓"
                col_data.append([col, type_name, pk, fk])
                
            print("\n  Column Structure:")
            print(tabulate(col_data, headers=["Column", "Type", "PK", "FK"], tablefmt="simple"))
            
            # Show sample data if available
            if info['rows'] > 0:
                try:
                    self.cursor.execute(f"SELECT * FROM {info['name']} LIMIT 3")
                    rows = self.cursor.fetchall()
                    if rows:
                        headers = [desc[0] for desc in self.cursor.description]
                        print("\n  Sample Data:")
                        print(tabulate(rows, headers=headers, tablefmt="simple"))
                except sqlite3.Error:
                    pass
            
            print("\n" + "-" * 50)
    
    def _display_table_info(self, table):
        if table not in self.tables:
            print(f"Table '{table}' not found in database")
            return
            
        # Get table info
        self.cursor.execute(f"PRAGMA table_info({table})")
        columns_info = self.cursor.fetchall()
        
        # Get row count
        self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
        row_count = self.cursor.fetchone()[0]
        
        # Get foreign key info
        self.cursor.execute(f"PRAGMA foreign_key_list({table})")
        fk_info = self.cursor.fetchall()
        
        # Get index info
        self.cursor.execute(f"PRAGMA index_list({table})")
        index_list = self.cursor.fetchall()
        
        print(f"\n📋 Table: {table}")
        print(f"  Rows: {row_count}")
        
        # Display column information
        col_data = []
        for col in columns_info:
            col_id, name, type_name, notnull, default_val, pk = col
            col_data.append([
                name, 
                type_name, 
                "✓" if pk else "", 
                "✓" if notnull else "",
                default_val if default_val is not None else ""
            ])
            
        print("\n  Column Structure:")
        print(tabulate(col_data, headers=["Column", "Type", "PK", "Not NULL", "Default"], tablefmt="simple"))
        
        # Display foreign key information
        if fk_info:
            fk_data = []
            for fk in fk_info:
                fk_data.append([
                    fk[3],  # from column
                    f"{fk[2]}.{fk[4]}"  # to table.column
                ])
            print("\n  Foreign Keys:")
            print(tabulate(fk_data, headers=["Column", "References"], tablefmt="simple"))
        
        # Display index information
        if index_list:
            index_data = []
            for idx in index_list:
                # Get columns in this index
                self.cursor.execute(f"PRAGMA index_info({idx[1]})")
                idx_cols = self.cursor.fetchall()
                col_names = [columns_info[col[2]][1] for col in idx_cols]
                
                index_data.append([
                    idx[1],  # index name
                    ", ".join(col_names),  # columns
                    "✓" if idx[2] else "",  # unique
                ])
            print("\n  Indexes:")
            print(tabulate(index_data, headers=["Name", "Columns", "Unique"], tablefmt="simple"))
        
        # Show sample data if available
        if row_count > 0:
            try:
                self.cursor.execute(f"SELECT * FROM {table} LIMIT 5")
                rows = self.cursor.fetchall()
                if rows:
                    headers = [desc[0] for desc in self.cursor.description]
                    print("\n  Sample Data:")
                    print(tabulate(rows, headers=headers, tablefmt="simple"))
            except sqlite3.Error as e:
                print(f"  Error fetching sample data: {e}")
                
    def _export_results_to_csv(self, rows, headers):
        import csv
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"query_results_{timestamp}.csv"
        
        try:
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f"✓ Results exported to {filename}")
            return True
        except Exception as e:
            print(f"✗ Export error: {e}")
            return False
            
    def _log_query(self, query, params, affected=0):
        timestamp = datetime.datetime.now()
        self.history.append({
            'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'query': query,
            'params': params,
            'affected': affected
        })
        self.last_query_time = timestamp
        
    def show_query_history(self):
        if not self.history:
            print("No queries executed yet")
            return
            
        print("\n📜 Query History:")
        for i, entry in enumerate(self.history, 1):
            print(f"{i}. [{entry['timestamp']}] {entry['query'][:60]}{'...' if len(entry['query']) > 60 else ''}")
            print(f"   Params: {entry['params']}")
            print(f"   Affected: {entry['affected']} rows")
            print()

    def run_interactive(self):
        if not self.conn:
            connected = self.connect()
            if not connected:
                return
                
        print("\n🔍 Real-Time Database Manager")
        print("------------------------------")
        print("Database:", self.db_path)
        print("Tables:", ", ".join(self.tables.keys()) if self.tables else "None")
        
        print("\nUsage examples:")
        print("  - Natural language: 'show all users where age > 25'")
        print("  - Direct SQL: 'SELECT * FROM users WHERE age > 25'")
        print("  - Schema commands: 'show tables', 'describe users'")
        print("\nType 'exit' to quit, 'history' to see query history")

        while True:
            print("")
            user_input = input("📝 > ").strip()
            
            if user_input.lower() == 'exit':
                break
            elif user_input.lower() == 'history':
                self.show_query_history()
                continue
            elif user_input.lower() == 'help':
                self._show_help()
                continue
            elif not user_input:
                continue
                
            # Parse and analyze the query
            parsed = self.parse_user_input(user_input)
            if parsed['type'] == 'unknown':
                print("I don't understand that request. Try 'help' for usage examples.")
                continue
                
            analysis = self.analyze_query(parsed)
            
            # Print explanation with risk level indication
            risk_level = analysis.get('risk_level', 'none')
            risk_icon = {
                'none': '  ',
                'low': '🟢',
                'medium': '🟡',
                'high': '🟠',
                'critical': '🔴'
            }.get(risk_level, '  ')
            
            print(f"\n{risk_icon} {analysis['explanation']}")
            print(f"  {analysis['details']}")
            
            # For high-risk operations, make confirmation more explicit
            if risk_level in ['high', 'critical']:
                confirm = input(f"⚠ This is a {risk_level} risk operation. Type 'confirm' to proceed: ")
                if confirm.lower() != 'confirm':
                    print("Operation cancelled.")
                    continue
            else:
                confirm = input("Execute this operation? (y/n): ")
                if confirm.lower() != 'y':
                    print("Operation cancelled.")
                    continue
                    
            # Execute the analyzed query
            self.execute_analyzed_query(analysis)
            
    def _show_help(self):
        print("\n📋 Help Information")
        print("------------------")
        print("1. Query Types:")
        print("   - Natural language: 'show all users where age > 25'")
        print("   - Direct SQL: 'SELECT * FROM users WHERE age > 25'")
        print("   - Schema exploration: 'show tables', 'describe users'")
        print("   - Operations: 'add new user', 'update user set name where id = 1'")
        print("\n2. Commands:")
        print("   - exit: Close the application")
        print("   - history: Show history of executed queries")
        print("   - help: Show this help information")
        print("\n3. Supported Operations:")
        print("   - SELECT: Retrieve data from tables")
        print("   - INSERT: Add new records")
        print("   - UPDATE: Modify existing records")
        print("   - DELETE: Remove records")
        print("   - SCHEMA: View database structure")

# Main program
if _name_ == "_main_":
    # Create the database manager
    db_manager = RealTimeDBManager()
    
    try:
        # Run the interactive session
        db_manager.run_interactive()
    except KeyboardInterrupt:
        print("\nProgram interrupted by user")
    finally:
        # Make sure to close the connection
        if db_manager.conn:
            db_manager.close()
