"""SQL conversion node for Insightly agent"""

import logging

import duckdb
from pydantic import BaseModel, Field
from pydantic import Field, BaseModel
from langchain_core.runnables.config import RunnableConfig

from insightly.classes import AgentState, ChatGPTNodeBase, Node, T, PlotType
from insightly.plotting import BarPlot, ScatterPlot
from insightly.utils import get_singleton


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


class SQLConverterNode(ChatGPTNodeBase):
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
        schema = get_singleton().get_schema()
        logger.info(f"Converting question to SQL: {question}")
        system = """You are an assistant that converts natural language questions into SQL queries based on the following schema:
database name: {db_name}
All tables should begin with the database name (i.e. {db_name}.foods).
{schema}

Provide only the SQL query without any explanations. Alias columns appropriately to match the expected keys in the result.

For example, alias 'food.name' as 'food_name' and 'food.price' as 'price'.
""".format(
            schema=schema, db_name=get_singleton().db_name
        )
        return system

    def post_query(
        self, result: ConvertToSQL, state: AgentState, config: RunnableConfig
    ) -> AgentState:
        """Post query to get an output and an AgentState.

        Parameters
        ----------
        result : ConvertToSQL
            The result of the SQL conversion.
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the SQL query.
        """
        state["sql_query_info"]["sql_query"] = result.sql_query
        return state


class ExecuteSQL(Node):
    """Class to execute SQL queries.
    This class is used to execute SQL queries on the database and retrieve the results.
    """

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
            get_singleton().add_df_to_duckdb(
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
            result: duckdb.DuckDBPyRelation = get_singleton().execute_query(sql_query)
            return self.post_query(result, state, config)
        except Exception as e:
            state["sql_query_info"][
                "query_result"
            ] = f"Error executing SQL query: {str(e)}"
            state["sql_query_info"]["sql_error"] = True
            print(f"Error executing SQL query: {str(e)}")
        return state

class Columns(BaseModel):
    """Class to represent the columns for a scatter plot.
    
    Attributes
    ----------
    columns: list[str]
        The columns to be used in the scatter plot.
    """
    columns: list[str] = Field(
        description="The columns to be used in the scatter plot."
    )

class GetColumnsNode(ChatGPTNodeBase):
    """Class to get the columns for any plot
    
    This class is used to get the columns for a scatter plot based on the provided database schema.
    """
    def init_query(self, state: AgentState, config: RunnableConfig) -> str:
        """
        Initialize the columns for the scatter plot.
        
        This function retrieves the question and schema from the state and prepares
        the system prompt for the ChatOpenAI model to get the columns for the scatter plot.
        
        Parameters
        ----------
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.
        
        Returns
        -------
        str
            The system prompt to be used for the ChatOpenAI model.
        """
        logger = logging.getLogger("Insightly")
        question = state["question"]
        schema = get_singleton().get_schema(table_name=state['sql_query_info']['table_name'])
        logger.debug(f"Getting columns: {question}")
        logger.debug("current schema: ", schema)
        system = """You are an assistant that chooses the appropriate columns for a scatter plot based on the following schema:

{schema}

Provide only the columns to be used in the scatter plot without any explanations.
The columns should be suitable for a scatter plot, typically two numerical columns.
Only return the names of the columns with no SQL, in the order they should be used in the plot (i.e. x1, y1, x2, y2).
""".format(schema=schema)
        return system

    def post_query(
        self, result: Columns, state: AgentState, config: RunnableConfig
    ) -> AgentState:
        """turn the columns into a plot, add the plot and columns to the sstate
        and return the state

        Parameters
        ----------
        result : Columns
            The result of the column selection.
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.
        
        Returns
        -------
        AgentState
            The updated state of the agent with the selected columns and plot.
        """
        logger = logging.getLogger("Insightly")
        state["plot_query_info"]["columns"] = result.columns
        # create a plot dependent on the type of plot type passed earlier
        if state["plot_query_info"]["plot_type"] == PlotType.SCATTER:
            scatter_plot = ScatterPlot()
            state["plot_query_info"]["result"] = scatter_plot.generate(state["sql_query_info"]["query_result"],
                                                                    state["plot_query_info"]["columns"])
        elif state["plot_query_info"]["plot_type"] == PlotType.BAR:
            bar_plot = BarPlot()
            state["plot_query_info"]["result"] = bar_plot.generate(state["sql_query_info"]["query_result"],
                                                                state["plot_query_info"]["columns"])
        logger.info(f"Selected columns for scatter plot: {state['plot_query_info']['columns']}")
        return state

class HumanResponse(BaseModel):
    """The final response once the SQL query has been confirmed a success
    
    Attributes
    ----------
    response: str
        The human response to the question.
    """
    response: str = Field(description="The human response to the question.")

class HumanResponseNode(ChatGPTNodeBase):

    def __init__(self, OutputClass: type[T]) -> None:
        """
        Initialize the HumanResponseNode class.
        
        This class is used to get a human response to
        the SQL query result and provide a normal response based on the question.
        """
        super().__init__(OutputClass=OutputClass)

    def init_query(self, state: AgentState, config: RunnableConfig):
        """initialize the query to generate a response understandable by a human
        
        Parameters
        ----------
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.
        
        Returns
        -------
        str
            The system prompt to be used for the ChatOpenAI model.
        """
        logger = logging.getLogger("Insightly")
        question = state["question"]
        answer: str = state["sql_query_info"]["query_result"]
        logger.debug(f"Waiting for human response to the question: {question}")
        system = """You are an assistant that retrieves the result of a question asked
    by a human and provides a normal response based on the question. The answer is {answer}""".format(answer=answer)
        return system

    def post_query(
        self, result: HumanResponse, state: AgentState, config: RunnableConfig
    ) -> AgentState:
        """Post query to get an output and an AgentState.

        Parameters
        ----------
        result : HumanResponse
            The result of the human response.
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.
        
        Returns
        -------
        AgentState
            The updated state of the agent with the human response.
        """
        state["sql_query_info"]["success_response"] = result.response
        print(f"Received human response: {state['sql_query_info']['success_response']}")
        return state