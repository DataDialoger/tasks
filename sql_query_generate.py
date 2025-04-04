import re
from typing import Dict, List, Optional, Union, Any

class QueryGPT:
    """
    An agent that converts natural language questions to SQL queries.
    Similar to Uber's QueryGPT, this agent analyzes the question and database schema
    to generate appropriate SQL.
    """
    
    def __init__(self, schema: Optional[Dict[str, List[Dict[str, Any]]]] = None, 
                 db_config: Optional[Dict[str, str]] = None):
        """
        Initialize the QueryGPT agent.
        
        Args:
            schema: Database schema information. If None, the agent will 
                request schema information when needed.
            db_config: Dictionary containing database connection parameters:
                - host: Database host
                - port: Database port
                - user: Database username
                - password: Database password
                - database: Database name
        """
        self.schema = schema
        self.db_config = db_config
        self.connection = None
        self.table_descriptions = {}
        self.metadata = {}
        self.recently_used_tables = []
        
    def set_schema(self, schema: Dict[str, List[Dict[str, Any]]]):
        """
        Set the database schema for the agent to use.
        
        Args:
            schema: Dict mapping table names to lists of column information
        """
        self.schema = schema
        # Extract table descriptions for better matching
        for table_name, columns in schema.items():
            columns_str = ", ".join([col["name"] for col in columns])
            self.table_descriptions[table_name] = f"Table '{table_name}' containing columns: {columns_str}"
    
    def set_metadata(self, metadata: Dict[str, Any]):
        """
        Set additional metadata about the database for improved query generation.
        
        Args:
            metadata: Dict containing metadata like table relationships, common joins, etc.
        """
        self.metadata = metadata
    
    def process_query(self, user_question: str) -> Dict[str, Any]:
        """
        Process a natural language question and convert it to SQL.
        
        Args:
            user_question: Natural language question from the user
            
        Returns:
            Dict containing the SQL query, explanation, and reasoning
            
        Raises:
            ValueError: If schema is not provided or invalid
            ConnectionError: If database connection fails
        """
        # Validate schema
        if not self.schema or not isinstance(self.schema, dict):
            raise ValueError("Valid schema information is required")
            
        # Validate database connection
        if self.db_config:
            try:
                import psycopg2
                self.connection = psycopg2.connect(**self.db_config)
            except Exception as e:
                raise ConnectionError(f"Failed to connect to database: {str(e)}")
        
        # Check if the query might be unsafe
        if self._is_unsafe_query(user_question):
            return {
                "sql": None,
                "safe": False,
                "explanation": "This request appears to involve data modification which is not allowed for safety reasons.",
                "reasoning": "The request contains terms that suggest data modification (INSERT, UPDATE, DELETE, etc.) which could potentially alter or destroy data."
            }
        
        # Step 1: Analyze the query to understand what it's asking for
        query_analysis = self._analyze_query(user_question)
        
        # Step 2: Identify relevant tables and columns
        tables, columns = self._identify_schema_elements(query_analysis)
        
        # Step 3: Determine query type and components
        query_type, query_components = self._determine_query_components(query_analysis, tables, columns)
        
        # Step 4: Generate the SQL query
        sql_query = self._generate_sql(query_type, query_components)
        
        # Step 5: Generate explanation and reasoning
        explanation = self._generate_explanation(query_type, query_components)
        reasoning = self._generate_reasoning(user_question, query_analysis, query_type, query_components)
        
        # Remember recently used tables for context
        self.recently_used_tables = tables[:5]  # Keep track of up to 5 tables
        
        return {
            "sql": sql_query,
            "safe": True,
            "explanation": explanation,
            "reasoning": reasoning
        }
    
    def _is_unsafe_query(self, query: str) -> bool:
        """
        Check if the query might be unsafe (attempting to modify data).
        
        Args:
            query: The natural language query
            
        Returns:
            Bool indicating if the query appears unsafe
        """
        query_lower = query.lower()
        
        # Check for explicit data modification keywords
        unsafe_keywords = [
            "insert", "update", "delete", "drop", "truncate", "alter", "create",
            "modify", "remove", "destroy", "wipe", "erase"
        ]
        
        # Common patterns that suggest data modification
        unsafe_patterns = [
            r"add\s+(?:a\s+)?new",
            r"delete\s+(?:all|the)",
            r"remove\s+(?:all|the)",
            r"update\s+(?:all|the)",
            r"modify\s+(?:all|the)",
            r"change\s+(?:all|the)",
            r"drop\s+(?:all|the)",
        ]
        
        # Check for keywords that directly suggest data modification
        for keyword in unsafe_keywords:
            if keyword in query_lower.split():
                return True
        
        # Check for patterns that might suggest data modification
        for pattern in unsafe_patterns:
            if re.search(pattern, query_lower):
                return True
                
        return False
    
    def _analyze_query(self, query: str) -> Dict[str, Any]:
        """
        Analyze the query to extract its key components and intent with enhanced NLP.
        
        Args:
            query: The natural language query
            
        Returns:
            Dict containing query analysis information
        """
        query_lower = query.lower()
        
        # Enhanced intent detection with pattern matching
        intent = "SELECT"  # Default intent
        
        # More comprehensive intent detection
        intent_patterns = {
            "COUNT": [
                r"how many", r"number of", r"count of", r"total number",
                r"how many.*are there", r"what is the count"
            ],
            "AVERAGE": [
                r"average", r"avg", r"mean", r"typical", 
                r"what is the average", r"calculate.*average"
            ],
            "SUM": [
                r"sum", r"total", r"add up", r"combined",
                r"what is the total", r"calculate.*sum"
            ],
            "MAX": [
                r"maximum", r"max", r"highest", r"largest", r"most",
                r"what is the maximum", r"find.*highest"
            ],
            "MIN": [
                r"minimum", r"min", r"lowest", r"smallest", r"least",
                r"what is the minimum", r"find.*lowest"
            ],
            "DISTINCT": [
                r"unique", r"distinct", r"different",
                r"list of unique", r"show distinct"
            ]
        }
        
        for intent_type, patterns in intent_patterns.items():
            if any(re.search(pattern, query_lower) for pattern in patterns):
                intent = intent_type
                break
        
        # Enhanced component detection
        has_grouping = bool(re.search(
            r"group by|per|each|by|for each|aggregated by|broken down by", 
            query_lower
        ))
        
        # Improved ordering detection
        has_ordering = bool(re.search(
            r"order by|sort by|sorted by|arrange by|rank by|"
            r"top \d+|bottom \d+|highest|lowest|most|least|"
            r"ascending|descending|alphabetical", 
            query_lower
        ))
        
        order_direction = "DESC" if re.search(
            r"descending|highest|most|top|max|maximum|largest|greatest", 
            query_lower
        ) else "ASC"
        
        # Enhanced limit detection
        has_limit = False
        limit_value = None
        limit_match = re.search(
            r"(?:top|first|last|limit to|show only|display only)\s+(\d+)", 
            query_lower
        )
        if limit_match:
            has_limit = True
            limit_value = int(limit_match.group(1))
        
        # Advanced condition parsing
        has_conditions = bool(re.search(
            r"where|if|when|with|that (?:have|has)|"
            r"greater than|less than|equal to|more than|fewer than|"
            r"before|after|between|from.*to|since|until|"
            r"like|contains|matches|starts with|ends with|"
            r"in the range|within|outside|excluding|including", 
            query_lower
        ))
        
        # Improved time detection
        is_time_based = bool(re.search(
            r"when|date|time|year|month|day|hour|minute|second|"
            r"week|quarter|recent|last|this|previous|next|"
            r"current|past|future|today|yesterday|tomorrow|"
            r"monday|tuesday|wednesday|thursday|friday|saturday|sunday", 
            query_lower
        ))
        
        return {
            "intent": intent,
            "has_grouping": has_grouping,
            "has_ordering": has_ordering,
            "order_direction": order_direction,
            "has_limit": has_limit,
            "limit_value": limit_value,
            "has_conditions": has_conditions,
            "is_time_based": is_time_based,
            "original_query": query
        }
    
    def _identify_schema_elements(self, query_analysis: Dict[str, Any]) -> tuple:
        """
        Identify relevant tables and columns from the schema based on the query
        using fuzzy matching and semantic analysis.
        
        Args:
            query_analysis: The query analysis dictionary
            
        Returns:
            Tuple of (tables, columns) that are relevant to the query
        """
        query = query_analysis["original_query"].lower()
        
        relevant_tables = []
        relevant_columns = []
        
        # Enhanced table matching with fuzzy logic
        for table_name, columns in self.schema.items():
            table_lower = table_name.lower()
            
            # Check for exact matches
            if table_lower in query or (table_lower.endswith('s') and table_lower[:-1] in query):
                relevant_tables.append(table_name)
                # When table is explicitly mentioned, include all its columns
                for col in columns:
                    relevant_columns.append({
                        "table": table_name,
                        "column": col["name"],
                        "data_type": col.get("data_type", "")
                    })
                continue
                
            # Check for partial matches and synonyms
            table_words = table_lower.split('_')
            for word in table_words:
                if len(word) > 3 and word in query:
                    relevant_tables.append(table_name)
                    break
                    
            # Check table descriptions
            table_desc = self.table_descriptions.get(table_name, "").lower()
            if any(word in table_desc for word in query.split()):
                relevant_tables.append(table_name)
        
        # If no tables found, use previously used tables or try to infer
        if not relevant_tables:
            if self.recently_used_tables:
                relevant_tables = self.recently_used_tables
            else:
                # Try to find relevant tables based on column mentions
                for table_name, columns in self.schema.items():
                    for column in columns:
                        column_name = column["name"].lower()
                        if column_name in query:
                            relevant_tables.append(table_name)
                            break
        
        # Remove duplicates while preserving order
        relevant_tables = list(dict.fromkeys(relevant_tables))
        
        # Ensure we have at least one table
        if not relevant_tables and self.schema:
            # Default to first table if no specific table is identified
            relevant_tables = [list(self.schema.keys())[0]]
        
        # Enhanced column matching
        for table_name in relevant_tables:
            table_columns = self.schema.get(table_name, [])
            for column in table_columns:
                column_name = column["name"].lower()
                
                # Check for exact matches
                if column_name in query:
                    relevant_columns.append({
                        "table": table_name,
                        "column": column["name"],
                        "data_type": column.get("data_type", "")
                    })
                    continue
                    
                # Check for partial matches
                col_words = column_name.split('_')
                for word in col_words:
                    if len(word) > 3 and word in query:
                        relevant_columns.append({
                            "table": table_name,
                            "column": column["name"],
                            "data_type": column.get("data_type", "")
                        })
                        break
                
                # Check column descriptions
                col_desc = column.get("description", "").lower()
                if any(word in col_desc for word in query.split()):
                    relevant_columns.append({
                        "table": table_name,
                        "column": column["name"],
                        "data_type": column.get("data_type", "")
                    })
                
                # Special handling for LIKE conditions
                if "like" in query and column_name in ["email", "name", "description"]:
                    relevant_columns.append({
                        "table": table_name,
                        "column": column["name"],
                        "data_type": column.get("data_type", "")
                    })
        
        return relevant_tables, relevant_columns
    
    def _determine_query_components(self, query_analysis: Dict[str, Any], 
                                   tables: List[str], 
                                   columns: List[Dict[str, str]]) -> tuple:
        """
        Determine the query type and components based on the analysis.
        
        Args:
            query_analysis: The query analysis dictionary
            tables: List of relevant tables
            columns: List of relevant columns
            
        Returns:
            Tuple of (query_type, query_components)
        """
        query = query_analysis["original_query"].lower()
        
        # Start with a basic SELECT query
        query_type = "SELECT"
        
        # Prepare components
        select_columns = []
        from_tables = tables
        where_conditions = []
        group_by = []
        order_by = []
        limit = None
        
        # Determine columns to select
        if query_analysis["intent"] in ["COUNT", "AVERAGE", "SUM", "MAX", "MIN"]:
            # For aggregation queries
            if columns:
                # Find an appropriate column to aggregate
                agg_column = None
                
                # First try to find a numeric column if the intent requires one
                if query_analysis["intent"] in ["AVERAGE", "SUM", "MAX", "MIN"]:
                    for col in columns:
                        if col["data_type"] in ["integer", "number", "float", "decimal"]:
                            agg_column = col
                            break
                
                # If no appropriate column found, use the first one
                if not agg_column and columns:
                    agg_column = columns[0]
                
                if agg_column:
                    func_name = {
                        "COUNT": "COUNT",
                        "AVERAGE": "AVG",
                        "SUM": "SUM",
                        "MAX": "MAX",
                        "MIN": "MIN"
                    }[query_analysis["intent"]]
                    
                    select_columns.append({
                        "type": "aggregation",
                        "function": func_name,
                        "table": agg_column["table"],
                        "column": agg_column["column"],
                        "alias": f"{func_name.lower()}_{agg_column['column']}"
                    })
            else:
                # If no columns identified, just count all rows
                select_columns.append({
                    "type": "aggregation",
                    "function": "COUNT",
                    "table": "",
                    "column": "*",
                    "alias": "count"
                })
        else:
            # For regular SELECT queries
            if columns:
                for col in columns:
                    select_columns.append({
                        "type": "column",
                        "table": col["table"],
                        "column": col["column"]
                    })
            else:
                # If no columns specified, select all
                select_columns.append({
                    "type": "all",
                    "table": "",
                    "column": "*"
                })
        
        # Determine GROUP BY
        if query_analysis["has_grouping"]:
            # For grouped queries, add columns that aren't aggregated to GROUP BY
            for col in columns:
                # Check if this column is in select and not aggregated
                is_aggregated = False
                for sel_col in select_columns:
                    if sel_col.get("type") == "aggregation" and sel_col.get("column") == col["column"]:
                        is_aggregated = True
                        break
                
                if not is_aggregated:
                    group_by.append({
                        "table": col["table"],
                        "column": col["column"]
                    })
        
        # Determine ORDER BY
        if query_analysis["has_ordering"]:
            # For ordered queries, use the aggregated column or a relevant column
            if select_columns:
                # Prioritize aggregated columns for ordering
                agg_cols = [col for col in select_columns if col.get("type") == "aggregation"]
                if agg_cols:
                    order_col = agg_cols[0]
                else:
                    order_col = select_columns[0]
                
                order_by.append({
                    "table": order_col.get("table", ""),
                    "column": order_col.get("column", "*"),
                    "direction": query_analysis["order_direction"]
                })
        
        # Determine LIMIT
        if query_analysis["has_limit"] and query_analysis["limit_value"]:
            limit = query_analysis["limit_value"]
        
        # Create where conditions based on query text
        if query_analysis["has_conditions"]:
            query_text = query_analysis["original_query"].lower()
            
            # Look for common condition patterns
            for col in columns:
                col_name = col["column"].lower()
                table_name = col["table"]
                
                # Special handling for date/time conditions
                if col["data_type"] in ["timestamp", "date", "datetime"]:
                    if "after" in query_text or "since" in query_text:
                        # Get current date/time for dynamic comparison
                        from datetime import datetime
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        where_conditions.append({
                            "table": table_name,
                            "column": col["column"],
                            "operator": ">",
                            "value": current_time
                        })
                    elif "before" in query_text:
                        where_conditions.append({
                            "table": table_name,
                            "column": col["column"],
                            "operator": "<",
                            "value": current_time
                        })
                
                # Check for equality conditions
                equality_patterns = [
                    fr"{col_name}\s+is\s+(\w+)",
                    fr"{col_name}\s+equals\s+(\w+)",
                    fr"{col_name}\s+=\s+(\w+)",
                    fr"{col_name}\s+after\s+(\d+)",
                    fr"{col_name}\s+before\s+(\d+)",
                    fr"{col_name}\s+greater than\s+(\d+)",
                    fr"{col_name}\s+less than\s+(\d+)"
                ]
                
                # Check for LIKE conditions
                like_patterns = [
                    fr"{col_name}\s+contains\s+(\w+)",
                    fr"{col_name}\s+like\s+(\w+)",
                    fr"{col_name}\s+matches\s+(\w+)",
                    fr"{col_name}\s+with\s+(\w+)"
                ]
                
                for pattern in equality_patterns:
                    match = re.search(pattern, query_text)
                    if match:
                        where_conditions.append({
                            "table": table_name,
                            "column": col["column"],
                            "operator": "=",
                            "value": match.group(1)
                        })
                
                # Check for greater/less than conditions
                gt_patterns = [
                    fr"{col_name}\s+greater\s+than\s+(\d+)",
                    fr"{col_name}\s+>\s+(\d+)",
                    fr"{col_name}\s+more\s+than\s+(\d+)"
                ]
                
                for pattern in gt_patterns:
                    match = re.search(pattern, query_text)
                    if match:
                        where_conditions.append({
                            "table": table_name,
                            "column": col["column"],
                            "operator": ">",
                            "value": match.group(1)
                        })
                
                lt_patterns = [
                    fr"{col_name}\s+less\s+than\s+(\d+)",
                    fr"{col_name}\s+<\s+(\d+)",
                    fr"{col_name}\s+fewer\s+than\s+(\d+)"
                ]
                
                for pattern in lt_patterns:
                    match = re.search(pattern, query_text)
                    if match:
                        where_conditions.append({
                            "table": table_name,
                            "column": col["column"],
                            "operator": "<",
                            "value": match.group(1)
                        })
                
                # Handle LIKE conditions
                for pattern in like_patterns:
                    match = re.search(pattern, query_text)
                    if match:
                        value = match.group(1)
                        # Special handling for email patterns
                        if col_name == "email" and ("gmail" in query_text or "yahoo" in query_text):
                            value = f"%{value}%"
                        where_conditions.append({
                            "table": table_name,
                            "column": col["column"],
                            "operator": "LIKE",
                            "value": value
                        })
        
        # Bundle all components
        query_components = {
            "select": select_columns,
            "from": from_tables,
            "where": where_conditions,
            "group_by": group_by,
            "order_by": order_by,
            "limit": limit
        }
        
        return query_type, query_components
    
    def _generate_sql(self, query_type: str, components: Dict[str, Any]) -> str:
        """
        Generate the SQL query string from the components with improved
        handling of complex queries and better formatting.
        
        Args:
            query_type: The type of query (SELECT, etc.)
            components: Dictionary of query components
            
        Returns:
            SQL query string
        """
        query_parts = []
        
        # SELECT clause with improved formatting
        select_items = []
        for col in components["select"]:
            if col.get("type") == "all":
                select_items.append("*")
            elif col.get("type") == "aggregation":
                table_prefix = f"{col['table']}." if col['table'] and col['column'] != '*' else ""
                aggregation = f"{col['function']}({table_prefix}{col['column']})"
                
                if col.get("alias"):
                    aggregation += f" AS {col['alias']}"
                
                select_items.append(aggregation)
            else:
                table_prefix = f"{col['table']}." if col['table'] else ""
                select_items.append(f"{table_prefix}{col['column']}")
        
        query_parts.append(f"SELECT\n    {',\n    '.join(select_items)}")
        
        # FROM clause with improved join handling
        if len(components["from"]) == 1:
            query_parts.append(f"FROM\n    {components['from'][0]}")
        else:
            from_clause = f"FROM\n    {components['from'][0]}"
            
            # If we have table relationship metadata, use it
            if self.metadata and "relationships" in self.metadata:
                relationships = self.metadata["relationships"]
                
                # Add joins for related tables
                for i in range(1, len(components["from"])):
                    curr_table = components["from"][i]
                    join_found = False
                    
                    # Check all possible join directions
                    for prev_table in components["from"][:i]:
                        relationship_key = f"{prev_table}_{curr_table}"
                        reverse_key = f"{curr_table}_{prev_table}"
                        
                        if relationship_key in relationships:
                            rel = relationships[relationship_key]
                            from_clause += f"\n    JOIN {curr_table} ON {prev_table}.{rel['from_column']} = {curr_table}.{rel['to_column']}"
                            join_found = True
                            break
                        elif reverse_key in relationships:
                            rel = relationships[reverse_key]
                            from_clause += f"\n    JOIN {curr_table} ON {curr_table}.{rel['from_column']} = {prev_table}.{rel['to_column']}"
                            join_found = True
                            break
                    
                    # If no specific join found, use LEFT JOIN by default
                    if not join_found:
                        from_clause += f"\n    LEFT JOIN {curr_table} ON 1=1"  # Placeholder for unknown join
            else:
                # Without metadata, use CROSS JOINs
                for i in range(1, len(components["from"])):
                    from_clause += f"\n    CROSS JOIN {components['from'][i]}"
            
            query_parts.append(from_clause)
        
        # WHERE clause with improved formatting
        if components["where"]:
            where_conditions = []
            for cond in components["where"]:
                table_prefix = f"{cond['table']}." if cond['table'] else ""
                
                # Enhanced value formatting
                value = cond["value"]
                if isinstance(value, str) and not value.isdigit():
                    # Handle special cases for LIKE conditions
                    if cond["operator"].upper() == "LIKE":
                        value = f"'%{value}%'"
                    else:
                        value = f"'{value}'"
                
                where_conditions.append(f"{table_prefix}{cond['column']} {cond['operator']} {value}")
            
            query_parts.append(f"WHERE\n    {' AND\n    '.join(where_conditions)}")
        
        # GROUP BY clause with improved formatting
        if components["group_by"]:
            group_cols = []
            for col in components["group_by"]:
                table_prefix = f"{col['table']}." if col['table'] else ""
                group_cols.append(f"{table_prefix}{col['column']}")
            
            query_parts.append(f"GROUP BY\n    {',\n    '.join(group_cols)}")
        
        # ORDER BY clause with improved formatting
        if components["order_by"]:
            order_cols = []
            for col in components["order_by"]:
                table_prefix = f"{col['table']}." if col['table'] else ""
                column_name = col['column']
                if column_name == "*":
                    # For ordering by *, use first non-aggregated column if available
                    if components["select"] and components["select"][0]["column"] != "*":
                        column_name = components["select"][0]["column"]
                    else:
                        column_name = "1"  # Default to first column
                
                order_cols.append(f"{table_prefix}{column_name} {col['direction']}")
            
            query_parts.append(f"ORDER BY\n    {',\n    '.join(order_cols)}")
        
        # LIMIT clause
        if components["limit"] is not None:
            query_parts.append(f"LIMIT {components['limit']}")
        
        # Combine with proper line breaks and indentation
        return "\n".join(query_parts) + ";"
    
    def _generate_explanation(self, query_type: str, components: Dict[str, Any]) -> str:
        """
        Generate a human-readable explanation of the SQL query.
        
        Args:
            query_type: The type of query
            components: Dictionary of query components
            
        Returns:
            Human-readable explanation string
        """
        if query_type == "SELECT":
            # Start with what data is being retrieved
            if components["select"][0].get("type") == "all":
                explanation = f"This query retrieves all columns from the {', '.join(components['from'])} table(s)"
            elif components["select"][0].get("type") == "aggregation":
                agg_func = components["select"][0]["function"].lower()
                if agg_func == "count" and components["select"][0]["column"] == "*":
                    explanation = f"This query counts all rows in the {', '.join(components['from'])} table(s)"
                else:
                    column_name = components["select"][0]["column"]
                    explanation = f"This query calculates the {agg_func} of {column_name} from the {', '.join(components['from'])} table(s)"
            else:
                columns = [col.get("column") for col in components["select"]]
                explanation = f"This query retrieves {', '.join(columns)} from the {', '.join(components['from'])} table(s)"
            
            # Add filtering explanation if where conditions exist
            if components["where"]:
                conditions = []
                for cond in components["where"]:
                    operator_text = {
                        "=": "equals",
                        ">": "is greater than",
                        "<": "is less than",
                        ">=": "is greater than or equal to",
                        "<=": "is less than or equal to",
                        "!=": "is not equal to"
                    }.get(cond["operator"], cond["operator"])
                    
                    conditions.append(f"{cond['column']} {operator_text} {cond['value']}")
                
                explanation += f" where {' and '.join(conditions)}"
            
            # Add grouping explanation
            if components["group_by"]:
                group_cols = [col["column"] for col in components["group_by"]]
                explanation += f", grouped by {', '.join(group_cols)}"
            
            # Add ordering explanation
            if components["order_by"]:
                direction = "descending" if components["order_by"][0]["direction"] == "DESC" else "ascending"
                explanation += f", ordered by {components['order_by'][0]['column']} in {direction} order"
            
            # Add limit explanation
            if components["limit"] is not None:
                explanation += f", limited to {components['limit']} results"
        
        return explanation + "."
    
    def _generate_reasoning(self, original_query: str, query_analysis: Dict[str, Any], 
                          query_type: str, components: Dict[str, Any]) -> str:
        """
        Generate reasoning explaining why the SQL was constructed this way.
        
        Args:
            original_query: Original natural language query
            query_analysis: Query analysis dictionary
            query_type: Type of query
            components: Query components
            
        Returns:
            Reasoning explanation string
        """
        reasoning_parts = []
        
        # Explain the initial query understanding
        reasoning_parts.append(f"I analyzed the question \"{original_query}\" to understand the user's intent.")
        
        # Explain table selection
        if components["from"]:
            reasoning_parts.append(f"I identified {', '.join(components['from'])} as the relevant table(s) based on the question context.")
        
        # Explain query type selection
        if query_analysis["intent"] in ["COUNT", "AVERAGE", "SUM", "MAX", "MIN"]:
            reasoning_parts.append(f"The question indicates a need for {query_analysis['intent'].lower()} aggregation based on keywords used.")
        
        # Explain column selection
        if components["select"][0].get("type") == "all":
            reasoning_parts.append("I selected all columns (*) since the question doesn't specify which fields to retrieve.")
        elif components["select"][0].get("type") == "aggregation":
            agg_column = components["select"][0]["column"]
            if agg_column == "*":
                reasoning_parts.append("I used COUNT(*) to count all rows since the question asks for a count of entries.")
            else:
                reasoning_parts.append(f"I applied {components['select'][0]['function']} to the {agg_column} column based on the question's intent.")
        else:
            columns = [col.get("column") for col in components["select"]]
            reasoning_parts.append(f"I selected the specific columns {', '.join(columns)} which are relevant to the question.")
        
        # Explain conditions
        if components["where"]:
            reasoning_parts.append(f"I added {len(components['where'])} filter condition(s) to match the criteria in the question.")
        
        # Explain grouping
        if components["group_by"]:
            group_cols = [col["column"] for col in components["group_by"]]
            reasoning_parts.append(f"I grouped by {', '.join(group_cols)} since the question asks for results organized by these dimensions.")
        
        # Explain ordering
        if components["order_by"]:
            direction = "descending" if components["order_by"][0]["direction"] == "DESC" else "ascending"
            reasoning_parts.append(f"I ordered results by {components['order_by'][0]['column']} in {direction} order as implied by the question.")
        
        # Explain limiting
        if components["limit"] is not None:
            reasoning_parts.append(f"I limited results to {components['limit']} rows based on the question's request for a specific number of results.")
        
        # Add safety considerations
        reasoning_parts.append("The query is read-only (SELECT) to ensure data safety and prevent any database modifications.")
        reasoning_parts.append("Parameters should be properly escaped when executing this query to prevent SQL injection.")
        
        return " ".join(reasoning_parts)


def generate_sql(user_question: str, schema: Dict = None, metadata: Dict = None) -> Dict:
    """
    Function to generate SQL from natural language questions.
    
    Args:
        user_question: The natural language question to convert to SQL
        schema: Optional database schema information
        metadata: Optional database metadata (relationships, etc.)
        
    Returns:
        Dictionary containing the SQL query and explanations
    """
    agent = QueryGPT(schema)
    
    if metadata:
        agent.set_metadata(metadata)
        
    result = agent.process_query(user_question)
    
    return {
        "sql": result["sql"],
        "explanation": result["explanation"],
        "reasoning": result["reasoning"]
    }

# Example schema for demonstration
sample_schema = {
    "users": [
        {"name": "id", "data_type": "integer", "description": "User ID"},
        {"name": "name", "data_type": "varchar", "description": "User's full name"},
        {"name": "email", "data_type": "varchar", "description": "User's email address"},
        {"name": "created_at", "data_type": "timestamp", "description": "When user was created"},
        {"name": "role", "data_type": "varchar", "description": "User role"}
    ],
    "orders": [
        {"name": "id", "data_type": "integer", "description": "Order ID"},
        {"name": "user_id", "data_type": "integer", "description": "User who placed the order"},
        {"name": "product_id", "data_type": "integer", "description": "Product ordered"},
        {"name": "quantity", "data_type": "integer", "description": "Quantity ordered"},
        {"name": "price", "data_type": "decimal", "description": "Price per unit"},
        {"name": "order_date", "data_type": "timestamp", "description": "When order was placed"}
    ],
    "products": [
        {"name": "id", "data_type": "integer", "description": "Product ID"},
        {"name": "name", "data_type": "varchar", "description": "Product name"},
        {"name": "category", "data_type": "varchar", "description": "Product category"},
        {"name": "price", "data_type": "decimal", "description": "Product price"},
        {"name": "stock", "data_type": "integer", "description": "Current stock level"}
    ]
}

# Example metadata with relationships
sample_metadata = {
    "relationships": {
        "users_orders": {
            "from_column": "id",
            "to_column": "user_id",
            "relationship_type": "one_to_many"
        },
        "products_orders": {
            "from_column": "id",
            "to_column": "product_id",
            "relationship_type": "one_to_many"
        }
    }
}

def execute_query(conn, sql):
    """Execute SQL query and return results"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        return columns, results
    except Exception as e:
        print(f"Error executing query: {e}")
        return None, None

def interactive_cli():
    """Run interactive command-line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Natural Language to SQL Query Generator')
    parser.add_argument('--db-config', type=str, help='Path to database config JSON file')
    args = parser.parse_args()

    # Load database config if provided
    db_config = None
    if args.db_config:
        import json
        with open(args.db_config) as f:
            db_config = json.load(f)

    # Initialize with sample schema/metadata or real database
    agent = QueryGPT(schema=sample_schema, db_config=db_config)
    agent.set_metadata(sample_metadata)

    print("Natural Language to SQL Query Generator")
    print("Type your question or 'exit' to quit\n")
    
    while True:
        try:
            question = input("> ")
            if question.lower() in ('exit', 'quit'):
                break
                
            result = agent.process_query(question)
            print(f"\nSQL: {result['sql']}")
            print(f"\nExplanation: {result['explanation']}")
            
            if agent.connection and result['sql']:
                columns, results = execute_query(agent.connection, result['sql'])
                if columns:
                    print("\nResults:")
                    print("\t".join(columns))
                    for row in results:
                        print("\t".join(str(x) for x in row))
            
            print("\n" + "-"*80 + "\n")
            
        except Exception as e:
            print(f"Error: {e}")
            continue

if __name__ == "__main__":
    interactive_cli()
