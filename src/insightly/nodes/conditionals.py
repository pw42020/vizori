from loguru import logger
from langgraph.graph import END

from insightly.nodes.state import State
from insightly.classes import ConditionalNode
from insightly.classes import AgentState
from insightly.utils import MAX_NUM_ATTEMPTS


class RelevanceConditionalNode(ConditionalNode):
    """Conditional node to check relevance."""

    def run(self, state: AgentState) -> str:
        """Run the conditional node."""
        logger.info("Checking relevance of the question.")
        if state["relevance"] == "relevant":
            return State.CHECK_IF_SQL_OR_PLOT
        else:
            logger.warning("Question is not relevant. Ending workflow.")
            return State.GENERATE_FUNNY_RESPONSE


class CheckErrorInSQLConditionalNode(ConditionalNode):
    """Conditional node to check for errors in SQL."""

    def run(self, state: AgentState) -> str:
        """Run the conditional node."""
        logger.debug("Checking for errors in SQL.")
        if not state.get("sql_error", False):
            if state["meant_as_query"]:
                # if the question is meant to be answered with SQL statement,
                # generate a human response
                return State.GENERATE_SUCCESS_RESPONSE
            # if not meant as query, get columns to plot
            return State.GET_COLUMNS
        else:
            return State.REGENERATE_QUERY


class CheckNumberOfAttemptsConditionalNode(ConditionalNode):
    """Conditional node to check the number of attempts."""

    def run(self, state: AgentState) -> str:
        """Run the conditional node."""
        logger.debug("Checking the number of attempts.")
        if state["attempts"] < MAX_NUM_ATTEMPTS:
            return State.REGENERATE_QUERY
        else:
            return END
