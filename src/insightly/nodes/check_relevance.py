"""
used with the LangGraph framework and is a prompt
to ensure that the answer given by the LLM is structured
"""

import logging

from pydantic import BaseModel, Field
from pydantic import Field, BaseModel
from langchain_core.runnables.config import RunnableConfig

from insightly.classes import AgentState, ChatGPTNodeBase, T
from insightly.insightly import Insightly


class CheckRelevance(BaseModel):
    """Checks the relevance of the question to the database schema.

    Attributes
    ----------
    relevance: str
        Indicates whether the question is related to the database schema.
    """

    relevance: str = Field(
        description="Indicates whether the question is related to the database schema. 'relevant' or 'not_relevant'."
    )


class CheckRelevanceNode(ChatGPTNodeBase):
    """Class to check the relevance of a question to the database schema.
    This class is used to determine if a question is related to the database schema
    and is used in the Insightly class.

    Attributes
    ----------
    insightly: Insightly
        The Insightly object to be used in the class.

    """

    def __init__(self, OutputClass: type[T]) -> None:
        """
        Initialize the RelevanceChecker class.

        """
        super().__init__(OutputClass=OutputClass)

    def init_query(self, state: AgentState, config: RunnableConfig) -> AgentState:
        """
        Check the relevance of the question to the database schema.

        Parameters
        ----------
        state : AgentState
            The current state of the agent.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the relevance information.
        """
        logger = logging.getLogger("Insightly")
        question: str = state["question"]
        schema: str = Insightly().get_schema()
        logger.info(f"Checking relevance of the question: {question}")
        system: str = (
            """You are an assistant that determines whether a given question is related to the following database schema.

Schema:
{schema}

Respond with only "relevant" or "not_relevant".
    """.format(
                schema=schema
            )
        )
        return system

    def post_query(
        self, result: CheckRelevance, state: AgentState, config: RunnableConfig
    ) -> AgentState:
        """add relevance result to the state

        Parameters
        ----------
        result : CheckRelevance
            The result of the query indicating whether it is relevant or not.
        state : AgentState
            The state of the agent containing the question and other information.
        config : RunnableConfig
            The configuration for the runnable.

        Returns
        -------
        AgentState
            The updated state of the agent with the relevance information.
        """
        state["relevance"] = result.relevance
        return state
