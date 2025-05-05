"""SQL conversion node for Insightly agent"""

import logging

import duckdb
from pydantic import BaseModel, Field
from pydantic import Field, BaseModel
from langchain_core.runnables.config import RunnableConfig

from insightly.classes import AgentState, NodeBase, T


class ConvertToSQL(BaseModel):
    """Converts natural language questions to SQL queries.

    Attributes
    ----------
    sql_query: str
        The SQL query generated from the natural language question.
    """

    sql_query: str = Field(
        description="The SQL query generated from the natural language question."
    )


class SQLConverterNode(NodeBase):
    """Class to convert natural language questions to SQL queries.
    This class is used to convert natural language questions into SQL queries
    based on the provided database schema.
    """

    def __init__(self, OutputClass: type[T]) -> None:
        """
        Initialize the RelevanceChecker class.

        """
        super().__init__(OutputClass=OutputClass)

    def init_query(self, state: AgentState, config: RunnableConfig):
        """
        Convert natural language questions to SQL queries.
        This function uses the ChatOpenAI model to convert the question into an SQL query
        based on the provided database schema.

        Parameters
        ----------
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the SQL query.
        """
        logger = logging.getLogger("Insightly")
        logger.info("Convert natural language to SQL")
        question = state["question"]
        schema = config["insightly"].get_schema()
        logger.info(f"Converting question to SQL: {question}")
        system = """You are an assistant that converts natural language questions into SQL queries based on the following schema:
database name: {db_name}
All tables should begin with the database name (i.e. {db_name}.foods).
{schema}

Provide only the SQL query without any explanations. Alias columns appropriately to match the expected keys in the result.

For example, alias 'food.name' as 'food_name' and 'food.price' as 'price'.
""".format(
            schema=schema, db_name=config["insightly"].db_name
        )
        return system

    def run(self, state: AgentState, config: RunnableConfig) -> AgentState:
        """
        Convert natural language questions to SQL queries.
        This function uses the ChatOpenAI model to convert the question into an SQL query
        based on the provided database schema.

        Parameters
        ----------
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the SQL query.
        """
        system = self.init_query(state, config)
        result: ConvertToSQL = self.run_chatgpt(state["question"], system, ConvertToSQL)
        state["sql_query_info"]["sql_query"] = result.sql_query
        return state


class ExecuteSQL(NodeBase):
    """Class to execute SQL queries.
    This class is used to execute SQL queries on the database and retrieve the results.
    """

    def __init__(self, OutputClass: type[T]) -> None:
        """
        Initialize the RelevanceChecker class.

        """
        super().__init__(OutputClass=OutputClass)

    def init_query(self, state: AgentState, config: RunnableConfig) -> str:
        """
        Initialize the SQL query for execution.
        This function retrieves the SQL query from the state and prepares
        """
        sql_query = state["sql_query_info"]["sql_query"].strip()
        print(f"Executing SQL query: {sql_query}")
        return sql_query

    def post_query(
        self, result: duckdb.DuckDBPyRelation, state: AgentState, config: RunnableConfig
    ):
        """Post querym select required info to the state

        Parameters
        ----------
        result : duckdb.DuckDBPyRelation
            The result of the SQL query execution.
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the SQL query result.
        """
        sql_query: str = state["sql_query_info"]["sql_query"]
        if sql_query.lower().startswith("select"):
            dataframe = result.df()
            state["sql_query_info"]["query_result"] = dataframe
            print("SUCCESSFUL EXECUTION OF SQL QUERY")
            # duckdb add the new df to the database
            config["insightly"].add_df_to_duckdb(
                dataframe, state["sql_query_info"]["table_name"]
            )
            state["sql_query_info"]["sql_error"] = False
            print("SQL SELECT query executed successfully.")
        else:
            state["sql_query_info"][
                "query_result"
            ] = "The action has been successfully completed."
            state["sql_query_info"]["sql_error"] = False
            print("SQL command executed successfully.")

    def run(self, state: AgentState, config: RunnableConfig) -> AgentState:
        """Run the SQL query and execute it on the database.

        Parameters
        ----------
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the SQL query result.
        """
        sql_query: str = self.init_query(state, config)
        try:
            result: duckdb.DuckDBPyRelation = config["insightly"].execute_query(
                sql_query
            )
            return self.post_query(result, state, config)
        except Exception as e:
            state["sql_query_info"][
                "query_result"
            ] = f"Error executing SQL query: {str(e)}"
            state["sql_query_info"]["sql_error"] = True
            print(f"Error executing SQL query: {str(e)}")
        return state
