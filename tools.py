from langchain.tools import tool

db_conn = None


@tool
def list_tables(_:str = "") -> str:
    """List all tables in the database"""
    cursor = db_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    return ", ".join(tables)


@tool
def check_table_exists(input: str) -> bool:
    """Check if a given table exists"""
    try:
        cursor = db_conn.cursor()
        table_name = input.strip()
        table_name = table_name.replace('"', "")
        cursor.execute(f"PRAGMA table_info({table_name})")
        return True if cursor.fetchall() else False
    except Exception as e:
        return False


@tool
def describe_table(input: str) -> str:
    """describe a given table"""
    table_name = input.strip()
    table_name = table_name.replace('"', "")

    cursor = db_conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    lines = []
    for (_, column_name, col_type, _, _, _) in columns:
        values = [str(value[0]) for value in cursor.execute(f"SELECT {column_name} from {table_name} LIMIT 10")]
        string_values = ", ".join(values)
        lines.append(f"column name: {column_name}, column type: {col_type}, column values: {string_values}")
    return  "\n".join(lines)



@tool
def execute_sql(sql_query: str) -> str:
    """ Executes a SQL query in the SQLite database and returns the result."""
    cursor = db_conn.cursor()
    try:
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        if not rows:
            return "Query executed successfully, but returned no results."

        col_names = [description[0] for description in cursor.description]
        result = [", ".join(col_names)]
        result += [", ".join(str(cell) for cell in row) for row in rows]

        return "\n".join(result)

    except Exception as e:
        return f"SQL execution failed: {str(e)}"