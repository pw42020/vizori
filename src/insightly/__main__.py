import logging
from typing import Any
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END

from insightly.insightly import Insightly
from insightly.nodes.check_relevance import CheckRelevanceNode, CheckRelevance
from insightly.nodes.sql import SQLConverterNode, ConvertToSQL
from insightly.nodes.sql_or_plot import SQLOrPlotNode, CheckIfSQLOrPlotReturn
from insightly.nodes.sql import ExecuteSQL, HumanResponse, HumanResponseNode, GetColumnsNode, Columns
from insightly.nodes.response import RegenerateQueryNode, RewrittenQuestion, FunnyResponse, FunnyResponseNode
from insightly.classes import AgentState
from insightly.nodes.state import State
from insightly.utils import get_singleton
from insightly.nodes.conditionals import *

load_dotenv()

logger = logging.getLogger("Insightly")

ROOT_PATH: str = str(Path(__file__).resolve()).split("src")[0]


def main() -> None:
    """
    Main function to demonstrate the usage of the Insightly class.
    """

    path_to_csv: str = f"{ROOT_PATH}/data/titanic/train.csv"
    insightly: Insightly = get_singleton()
    insightly.read_csv_to_duckdb(path_to_csv, "titanic")
    # table = insightly.retrieve_table("db.foods")
    # print(table.df())
    # logger = logging.getLogger("Insightly")

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

    workflow = StateGraph(AgentState)
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

    # question = "What is the average age of passengers who survived?"
    question = "Create a bar plot that shows the number of passengers, grouped by 10 year age buckets"
    result: dict[str, dict[str, Any]] = app.invoke(
        {"question": question, "attempts": 0}
    )
    if result.get("meant_as_query", False):
        # if the SQL query was executed successfully, print the result
        print(result["sql_query_info"]["success_response"])
        print("Result:", result["sql_query_info"]["query_result"])
    else:
        # if the plot was generated successfully, show the plot that was returned
        result["plot_query_info"]["result"].show()


if __name__ == "__main__":
    main()
