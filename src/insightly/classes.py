"""Insightly classes for the Insightly agent."""

from enum import Enum
from typing import TypedDict, Optional, Generic, TypeVar
from abc import ABC, abstractmethod
from pydantic import BaseModel
from langchain_core.runnables.config import RunnableConfig
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

T = TypeVar("T", bound=BaseModel)


class PlotType(str, Enum):
    """Plot type for the visualization of the data.

    Attributes
    ----------
    BAR : str
        Bar plot.
    SCATTER : str
        Scatter plot.
    LINE : str
        Line plot.
    HISTOGRAM : str
        Histogram plot.
    PIE : str
        Pie plot.
    HEATMAP : str
        Heatmap plot.
    """

    SCATTER = "SCATTER"
    BAR = "BAR"
    # LINE = "LINE"
    # HISTOGRAM = "HISTOGRAM"
    # PIE = "PIE"
    # HEATMAP = "HEATMAP"


class SqlQueryInfo(TypedDict):
    """Information regarding the SQL query performed on the request.

    Attributes
    ----------
    sql_query : str
        The SQL query that was executed.
    query_result : str
        The result of the SQL query.
    table_name : str
        The name of the table that was queried.
    query_rows : list
        The rows returned from the SQL query.
    sql_error : bool
        Indicates whether there was an error in the SQL query.
    """

    sql_query: str
    query_result: str
    table_name: str
    query_rows: list
    sql_error: bool


class PlotQueryInfo(TypedDict):
    """Plot query information for the visualization of the data.

    Attributes
    ----------
    plot_query: str
        The SQL query that was executed
    columns: list
        The columns that were used in the plot
    result: str
        The result of the SQL query
    """

    plot_type: str
    columns: list[str]
    result: str


class AgentState(TypedDict):
    """Agent state for the SQL query and plot information.

    Attributes
    ----------
    question: str
        The question that was asked.
    meant_as_query: bool
        Indicates whether the question was meant as a query or to be plotted.
    sql_query_info: SqlQueryInfo
        Information regarding the SQL query performed on the request.
    plot_query_info: PlotQueryInfo
        Information regarding the plot query performed on the request.
    attempts: int
        The number of attempts made to answer the question.
    relevance: str
        Indicates whether the question is related to the database schema.
    """

    question: str
    meant_as_query: bool
    sql_query_info: Optional[SqlQueryInfo] = None
    plot_query_info: Optional[PlotQueryInfo] = None
    attempts: int
    relevance: str

class Node(ABC):
    """Abstract base class for Insightly"""

    @abstractmethod
    def run(self, state: AgentState, config: RunnableConfig) -> AgentState:
        """Run the node to get an output and an AgentState."""
        pass    

# abstract base class that initializes a run() method and requires insightly() object
# initialize NodeBase for abstract methods and generic type
class ChatGPTNodeBase(Node, ABC):
    """Abstract base class for Insightly classes.
    This class defines the interface for Insightly classes and requires the implementation of the run() method.
    """

    OutputClass: BaseModel  # generic type for the class

    def __init__(self, OutputClass: BaseModel) -> None:
        """
        Initialize the NodeBase class.
        """
        self.OutputClass = OutputClass

    def run_chatgpt(self, question: str, system: str) -> BaseModel:
        convert_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system),
                ("human", f"Question: {question}"),
            ]
        )
        llm = ChatOpenAI(temperature=0)
        structured_llm = llm.with_structured_output(self.OutputClass)
        sql_generator = convert_prompt | structured_llm
        result = sql_generator.invoke({})
        return result

    @abstractmethod
    def init_query(self, state: AgentState, config: RunnableConfig) -> str:
        """Initialize the prompt question for the Insightly class."""
        pass

    @abstractmethod
    def post_query(
        self, result: T, state: AgentState, config: RunnableConfig
    ) -> AgentState:
        """Post query to get an output and an AgentState."""
        pass

    def run(self, state: AgentState, config: RunnableConfig) -> AgentState:
        """Run the node to get an output and an AgentState."""
        system = self.init_query(state, config)
        print(f"Running node with question: {system}")
        result: T = self.run_chatgpt(
            question=state["question"],
            system=system,
        )
        return self.post_query(result, state, config)


class QueryType(str, Enum):
    """Query type for the question.

    Attributes
    ----------
    SQL : str
        Indicates that the question requires an SQL query.
    SCATTER : str
        Indicates that the question requires a scatter plot.
    BAR : str
        Indicates that the question requires a bar plot.
    """

    SQL = "sql"
    SCATTER = "scatter"
    BAR = "bar"
