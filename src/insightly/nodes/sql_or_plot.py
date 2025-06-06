"""Check if the question is meant as an SQL query or a plot.
This module contains a function to determine whether a given question
is intended to be an SQL query or a plot based on the provided schema.
"""

from loguru import logger
from random import randint

from pydantic import BaseModel, Field
from langchain_core.runnables.config import RunnableConfig

from insightly.classes import (
    SqlQueryInfo,
    PlotQueryInfo,
    QueryType,
    PlotType,
    AgentState,
    ChatGPTNodeBase,
    T,
)
from insightly.insightly import Insightly


class CheckIfSQLOrPlotReturn(BaseModel):
    """Check if the question is meant as an SQL query or a plot.

    Attributes
    ----------
    meant_as_query : QueryType
        Indicates whether the question requires an SQL query or a plot.
    type_of_plot : PlotType
        The type of plot to be generated if the question requires a plot.
    """

    meant_as_query: QueryType = Field(
        description="Indicates whether the question requires an SQL query or a plot."
    )
    type_of_plot: PlotType = Field(
        description="The type of plot to be generated if the question requires a plot."
    )


class SQLOrPlotNode(ChatGPTNodeBase):
    """Class to check if the question is meant as an SQL query or a plot.

    This class is used to determine if a question is related to data retrieval

    """

    def __init__(self, OutputClass: type[T]) -> None:
        """
        Initialize the RelevanceChecker class.

        """
        super().__init__(OutputClass=OutputClass)

    def init_query(self, state: AgentState, config: RunnableConfig) -> str:
        """Check if the question is meant as an SQL query or a plot.

        Parameters
        ----------
        state : AgentState
            The state of the agent containing the question and other information.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        str
            The system prompt to be used for the ChatOpenAI model.
        """
        question = state["question"]
        logger.info(
            f"Checking if the question requires an SQL query or a plot: {question}"
        )
        schema = Insightly().get_schema()
        system = """
You are an assistant that determines whether a given question requires an SQL query or a plot based on the following schema:
{schema}

Respond with 'sql' if the question is related to data retrieval or manipulation that can be expressed in SQL.
If the question is related to data visualization, choose one of the following plot types with no explanation: {plot_types}.
""".format(
            schema=schema, plot_types=", ".join([member.value for member in PlotType])
        )
        return system

    def post_query(
        self, result: CheckIfSQLOrPlotReturn, state: AgentState, config: RunnableConfig
    ) -> AgentState:
        """Post query to get an output and an AgentState.

        Parameters
        ----------
        result : CheckIfSQLOrPlotReturn
            The result of the query indicating whether it is an SQL query or a plot.
        state : AgentState
            The state of the agent containing the question and other information.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the SQL query or plot information.
        """
        state["meant_as_query"] = result.meant_as_query == QueryType.SQL
        logger.info("MEANT AS QUERY: {}".format(state["meant_as_query"]))
        # generate a random table name for the SQL query and the typed dictionaries
        state["sql_query_info"] = SqlQueryInfo(
            sql_query="",
            query_result="",
            table_name=f"transformation_{randint(0, 10000)}",
            query_rows=[],
        )
        state["plot_query_info"] = PlotQueryInfo(
            plot_type=result.type_of_plot.value, query_result="", query_rows=[]
        )
        # check if the meant_as_query is a boolean
        assert state["meant_as_query"] in [
            True,
            False,
        ], "Meant as query should be a boolean"
        logger.info(f"Determined type: {state['meant_as_query']}")
        return state
