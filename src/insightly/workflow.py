
from langgraph.graph import StateGraph, END

from insightly.nodes.check_relevance import CheckRelevanceNode, CheckRelevance
from insightly.nodes.sql import SQLConverterNode, ConvertToSQL
from insightly.nodes.sql_or_plot import SQLOrPlotNode, CheckIfSQLOrPlotReturn
from insightly.nodes.sql import ExecuteSQL, HumanResponse, HumanResponseNode, GetColumnsNode, Columns
from insightly.nodes.response import RegenerateQueryNode, RewrittenQuestion, FunnyResponse, FunnyResponseNode
from insightly.classes import AgentState
from insightly.nodes.state import State
from insightly.nodes.conditionals import *

workflow = StateGraph(AgentState)

def create_and_compile_workflow() -> None:
    # initialize individual nodes
    relevance_checker = CheckRelevanceNode(CheckRelevance)
    sql_converter = SQLConverterNode(ConvertToSQL)
    sql_or_plot_checker = SQLOrPlotNode(CheckIfSQLOrPlotReturn)
    execute_sql = ExecuteSQL()
    regenerate_query_node = RegenerateQueryNode(RewrittenQuestion)
    funny_response_node = FunnyResponseNode(FunnyResponse)
    human_response_node = HumanResponseNode(HumanResponse)
    get_columns_node = GetColumnsNode(Columns)

    # initialize conditional nodes
    relevance_router = RelevanceConditionalNode()
    check_error_in_sql_router = CheckErrorInSQLConditionalNode()
    check_number_of_attempts_router = CheckNumberOfAttemptsConditionalNode()

    # connecting all non-conditional nodes to workflow
    workflow.add_node(State.CHECK_RELEVANCE, relevance_checker.run)
    workflow.add_node(State.CHECK_IF_SQL_OR_PLOT, sql_or_plot_checker.run)
    workflow.add_node(State.CONVERT_NL_TO_SQL, sql_converter.run)
    workflow.add_node(State.EXECUTE_SQL, execute_sql.run)
    workflow.add_node(State.GENERATE_SUCCESS_RESPONSE, human_response_node.run)
    workflow.add_node(State.GENERATE_FUNNY_RESPONSE, funny_response_node.run)
    workflow.add_node(State.GET_COLUMNS, get_columns_node.run)
    workflow.add_node(State.REGENERATE_QUERY, regenerate_query_node.run)
    workflow.add_node(State.CHECK_IF_ERROR, check_error_in_sql_router.run)

    # adding edges to the workflow
    # if the question is relevant, make a query. If not, generate a funny response
    workflow.add_conditional_edges(State.CHECK_RELEVANCE, relevance_router.run)

    # check if it is a SQL query or plot, and either way filter for plot or do
    # SQL query
    workflow.add_edge(State.CHECK_IF_SQL_OR_PLOT, State.CONVERT_NL_TO_SQL)
    
    # execute the SQL generated from the natural language conversion step
    workflow.add_edge(State.CONVERT_NL_TO_SQL, State.EXECUTE_SQL)

    # once SQL has been executed, first check if there was an error
    workflow.add_conditional_edges(State.EXECUTE_SQL, check_error_in_sql_router.run)

    # if there was no error, try again to regenerate the query and convert
    # the new question to SQL
    workflow.add_edge(State.REGENERATE_QUERY, State.CONVERT_NL_TO_SQL)

    # regenerate attempt if you still have attempts left -- already errored by this point
    workflow.add_conditional_edges(State.REGENERATE_QUERY, check_number_of_attempts_router.run)

    # workflow.add_edge(State.GET_COLUMNS, State.GENERATE_SCATTER_PLOT)
    workflow.add_edge(State.GET_COLUMNS, END)
    workflow.add_edge(State.GENERATE_FUNNY_RESPONSE, END)


    # set the entry point
    workflow.set_entry_point(State.CHECK_RELEVANCE)

    app = workflow.compile()
    return workflow, app

def ask(app, question: str) -> AgentState:
    """
    Queries the DuckDB database with a natural language question.

    Parameters
    ----------
    question : str
        The natural language question to query.

    Returns
    -------
    AgentState
        The state of the agent after processing the query.
    """
    # run the workflow with the given question
    result: AgentState = app.invoke({"question": question, "attempts": 0})
    return result