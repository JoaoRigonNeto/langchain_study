from langchain_ollama import ChatOllama
from langgraph.graph import StateGraph, END
import tools  
from sqlite_local import create_in_memory_db
from typing import TypedDict

llm = ChatOllama(model="llama3", temperature=0.0)
tools.db_conn = create_in_memory_db()

class GraphState(TypedDict, total=False):
    question: str
    table: str
    table_exists: bool
    schema: str
    relevant_columns: str
    sql_query: str
    sql_answer: str
    final_answer: str

def guess_table_node(state: GraphState) -> GraphState:
    table_list = tools.list_tables.invoke("")
    prompt = f"""
        You are a data expert, data steward. Here is a list of available tables: {table_list}
        Given the question: {state["question"]}, which table is the most relevant to answer it?
        Do not guess if you are not sure. Never make something up.
        Responde with only the table name. 
    """
    response = llm.invoke(prompt)
    return {"table": response.content.strip()}

def check_table_node(state: GraphState) -> GraphState:
    result = tools.check_table_exists.invoke(state["table"])
    return {"table_exists": result}

def describe_table_node(state: GraphState) -> GraphState:
    if state["table_exists"] == "exists":
        schema = tools.describe_table.invoke(state["table"])
        return {"schema": schema}
    return {"schema": "Table not found."}

def infere_column_node(state: GraphState) -> GraphState:
    if state["table_exists"] == "missing":
        return {"schema": "Table not found."}
    schema = tools.describe_table.invoke(state["table"])
    prompt = f"""
    schema: {schema}
    You are a data expert, data steward.
    Given the question: {state["question"]}, which fields are the most relevant to answer it?
    Do not guess if you are not sure. Never make something up.
    Responde with comma-separated list of clumn names, no explanation. 
    """
    return {"relevant_columns": llm.invoke(prompt).content.strip()}

def generate_sql_node(state: GraphState) -> GraphState:
    prompt = f"""
    You are a data expert, data steward, responsible for the governance and documentation of tables and columns of a database.
    your task is to generate a sql query to answer the question in the most clear and accurate manner.
    Do not guess if you are not sure. Never make something up or leave information out.
    Only answer the sql query with no explanation

    here is the context to generate the queryL

    table name:{state["table"]}

    table schema: {state["schema"]}
    
    question: {state["question"]}

    relevant columns: {state["relevant_columns"]}

    SQL query: 
    """
    return {"sql_query": llm.invoke(prompt).content.strip()}

def execute_sql_node(state: GraphState) -> GraphState:
    result = tools.execute_sql.invoke(state["sql_query"])
    return {"sql_answer": result}

def answer_node(state: GraphState) -> GraphState:
    prompt = f"""
    You are a data expert, data analyst.

    You were asked this question: {state["question"]}

    You wrote this sql query to answer it {state["sql_query"]}

    The query result is this: {state["sql_answer"]}

    Please summarize the answer to the user in natural language, clearly and accurately.
    Avoid technical jargon or SQL terms. If the result is a number, explain what it means.
    If the result is empty or an error, explain that nicely.
    Do not guess if you are not sure. Never make something up or leave information out.
    at the end of the explanation always present the sql query that was wrote.
    """
    response = llm.invoke(prompt)
    return {"final_answer": response.content.strip()}

graph = StateGraph(GraphState)
graph.add_node("guess_table", guess_table_node)
graph.add_node("check_table", check_table_node)
graph.add_node("describe_table", describe_table_node)
graph.add_node("infere_column", infere_column_node)
graph.add_node("generate_sql", generate_sql_node)
graph.add_node("execute_sql", execute_sql_node)
graph.add_node("answer", answer_node)

graph.set_entry_point("guess_table")
graph.add_edge("guess_table", "check_table")
graph.add_edge("check_table", "describe_table")
graph.add_edge("describe_table", "infere_column")
graph.add_edge("describe_table", "infere_column")
graph.add_edge("infere_column", "generate_sql")
graph.add_edge("generate_sql", "execute_sql")
graph.add_edge("execute_sql", "answer")
graph.add_edge("answer", END)

graph_executor = graph.compile()

result = graph_executor.invoke({"question": "who spend the most with us?"})
print(result["final_answer"])
