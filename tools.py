from langchain.tools import tool

db_conn = None

@tool
def check_table_exists(input: str) -> str:
    """
    check if a table exists in the local sqlite in memory database.
    Input format: table_name
    """
    if not db_conn:
        return "No database connection available"

    cursor = db_conn.cursor()
    table_name = input.strip()
    table_name = table_name.replace('"', "")

    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    if not columns:
        return "Table does not exists in database"
    return "Table  exists in database"
    

@tool
def structured_table_overview(input: str) -> str:
    """
    returns a structured overview of the table, containing: column type, column name and example values
    Input format: table_name
    """
    cursor = db_conn.cursor()
    table_name = input.strip()
    table_name = table_name.replace('"', "")

    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    rows = cursor.fetchall()

    description_list = []
    for (_, column_name, col_type, _, _, _) in columns:
        values = [str(value[0]) for value in cursor.execute(f"SELECT {column_name} from {table_name} LIMIT 10")]
        string_values = ", ".join(values)
        description_list.append(f"column name: {column_name}, column type: {col_type}, column values: {string_values}")
    return  "\n".join(description_list)

@tool
def describe_table(input: str) -> str:
    """
    Infers human-readable descriptions of each column of a local sqlite table
    imput format: table_name
    """
    table_name = input.strip()
    table_name = table_name.replace('"', "")

    # You are a data expert, data steward, responsible for the governance and documentation of tables and columns of a database.
    # You are given a list, with a table schema containing (field type, field name and sample values), and your task is to describe each field`s purpose clearly and accurately.
    # Do not guess if you are not sure. Always incluse a example of a field. Never make something up or leave something behind.
    # Always format your output like this:
    pre_prompt= """
    Column: <column_name>
    Description: <field purpose>
    Example: <field example>
    """
    description_string = structured_table_overview(table_name)

    return pre_prompt + "\n\n" + description_string