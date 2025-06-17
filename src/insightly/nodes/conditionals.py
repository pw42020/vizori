import duckdb
from loguru import logger
from langgraph.graph import END

from insightly.nodes.state import State
from insightly.insightly import Insightly
from insightly.classes import ConditionalNode
from insightly.classes import AgentState, PlotType
from insightly.utils import MAX_NUM_ATTEMPTS


class RelevanceConditionalNode(ConditionalNode):
    """Conditional node to check relevance."""

    def run(self, state: AgentState) -> str:
        """Run the conditional node."""
        logger.info("Checking relevance of the question.")
        if state.relevance == "relevant":
            return State.CHECK_IF_SQL_OR_PLOT
        else:
            logger.warning("Question is not relevant. Ending workflow.")
            return State.GENERATE_FUNNY_RESPONSE


class CheckErrorInSQLConditionalNode(ConditionalNode):
    """Conditional node to check for errors in SQL."""

    def run(self, state: AgentState) -> str:
        """Run the conditional node."""
        logger.debug("Checking for errors in SQL.")
        # check if there is an error in SQL syntax
        sql_syntax_correct: bool = True
        try:
            # use EXPLAIN to check for syntax errors without executing the query
            Insightly().execute_query(f"EXPLAIN {state.sql_query}")
        except duckdb.ParserException as e:
            logger.error(f"SQL Syntax Error: {e}")
            sql_syntax_correct = False
        if sql_syntax_correct:
            logger.debug("SQL syntax is correct.")
            return State.GENERATE_SUCCESS_RESPONSE
        else:
            return State.REGENERATE_QUERY


class CheckNumberOfAttemptsConditionalNode(ConditionalNode):
    """Conditional node to check the number of attempts."""

    def run(self, state: AgentState) -> str:
        """Run the conditional node."""
        logger.debug("Checking the number of attempts.")
        if state.attempts < MAX_NUM_ATTEMPTS:
            return State.REGENERATE_QUERY
        else:
            state.response = (
                "I couldn't create an answer for you based off of your question. "
                "Please try again with a differently-phrased question."
            )
            return END


class GetColumnsIfPlotConditionalNode(ConditionalNode):
    """Conditional node to check if the question is meant to be plotted."""

    def run(self, state: AgentState) -> str:
        """Run the conditional node."""
        logger.debug("Checking if the question is meant to be plotted.")
        if state.plot_type != PlotType.NONE:
            return State.GET_COLUMNS
        else:
            return END
