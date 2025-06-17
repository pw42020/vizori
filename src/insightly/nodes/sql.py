"""SQL conversion node for Insightly agent"""

from loguru import logger
from typing import Dict

import duckdb
from pydantic import BaseModel, Field
from pydantic import Field, BaseModel
from langchain_core.runnables.config import RunnableConfig

from insightly.classes import AgentState, ChatGPTNodeBase, T, VariableType, NewField
from insightly.plotting import BarPlot, ScatterPlot
from insightly.insightly import Insightly


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
        logger.info("Convert natural language to SQL")
        question = state.question
        schema = Insightly().schema.to_string()
        logger.info(f"Converting question to SQL: {question}")
        system = """You are an assistant that converts natural language questions into SQL queries based on the following schema:
database name: {db_name}
{schema}

All tables should begin with the database name (i.e. {db_name}.foods).

Provide only the SQL query without any explanations. Alias columns appropriately to match the expected keys in the result.

For example, alias 'food.name' as 'food_name' and 'food.price' as 'price'.

If any columns need to be added to the schema to answer the question, add them to the additions field with their types.
""".format(
            schema=schema, db_name=Insightly().db_name
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
        state.sql_query = result.sql_query
        return state


class NewFields(BaseModel):
    additions: list[NewField] = Field(
        description="List of new fields to be added to the schema."
    )


class NewFieldsNode(ChatGPTNodeBase):
    """Class to get new fields to add to the schema

    This class is used to get new fields to add to the schema based on the provided database schema.
    """

    def init_query(self, state: AgentState, config: RunnableConfig) -> str:
        """
        Initialize the new fields for the schema.

        This function retrieves the question and schema from the state and prepares
        the system prompt for the ChatOpenAI model to get the new fields for the schema.

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
        question = state.question
        schema = Insightly().schema.to_string()
        logger.debug(f"Getting new fields: {question}")
        logger.debug("current schema: ", schema)
        system = """You are an assistant that chooses any new fields to add to the schema based on the following schema:
{schema}

And the SQL query: 
{sql_query}

Available types to choose from are: {types}

Provide only the new fields to be added to the schema without any explanations.
""".format(
            schema=schema,
            sql_query=state.sql_query,
            types=", ".join([member.value for member in VariableType]),
        )
        return system

    def post_query(
        self, result: NewFields, state: AgentState, config: RunnableConfig
    ) -> AgentState:
        """turn the new fields into additions to the schema and add them to the state
        and return the state

        Parameters
        ----------
        result : NewFields
            The result of the new fields.
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the new fields added to the schema.
        """
        print(f"RESULT: {result}")
        Insightly().add_to_schema(result.additions)
        logger.info(f"Added new fields to schema: {result.additions}")
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
        question = state.question
        schema = Insightly().schema.to_string()
        logger.debug(f"Getting columns: {question}")
        logger.debug("current schema: ", schema)
        system = """You are an assistant that chooses the appropriate columns for a scatter plot based on the following schema:

{schema}

Provide only the columns to be used in the scatter plot without any explanations.
The columns should be suitable for a scatter plot, typically two numerical columns.
Only return the names of the columns with no SQL, in the order they should be used in the plot (i.e. x1, y1, x2, y2).
""".format(
            schema=schema
        )
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
        state.columns = result.columns
        # create a plot dependent on the type of plot type passed earlier
        # if state.plot_type"] == PlotType.SCATTER:
        #     scatter_plot = ScatterPlot()
        #     state.result"] = scatter_plot.generate(
        #         state.query_result"],
        #         state.columns"],
        #     )
        # elif state.plot_type"] == PlotType.BAR:
        #     bar_plot = BarPlot()
        #     state.result"] = bar_plot.generate(
        #         state.query_result"],
        #         state.columns"],
        #     )
        logger.info(f"Selected columns for scatter plot: {state.columns}")
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
        question = state.question
        error: str = state.error_occurred
        sql_query: str = state.sql_query
        system = f"""You are an assistant that has helped provide a sql query to a user to help
        create a response to their question. The question is: {question}.
        The sql query you provided is: {sql_query}.
        Did an error occur? {"Yes" if error else "No"}.

        If an error did occur, apologize and let the user know you will try again.
        If no error occurred, provide a concise and clear answer and explanation to the SQL
        query you recommend to run.
        """
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
        state.response = result.response
        logger.info(
            "Received human response: {response}".format(response=state.response)
        )
        return state
