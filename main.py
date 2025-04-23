from langchain.agents import initialize_agent, AgentType
from langchain_ollama import ChatOllama
import tools  
from sqlite_local import create_in_memory_db

llm = ChatOllama(model="llama3", temperature=0.0)
db_conn = create_in_memory_db()
tools.db_conn = db_conn
cursor = db_conn.cursor()


agent = initialize_agent(
    tools=[tools.check_table_exists, tools.structured_table_overview, tools.describe_table],
    llm= llm, 
    agent= AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True
)

response = agent.invoke("What field in the customers table can tell me if a customer has been a client for more than 6 months?")
print(response)