import logging
from typing import TypedDict, Any

import duckdb
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END

from insightly.insightly import Insightly
from insightly.nodes.check_relevance import CheckRelevanceNode, CheckRelevance
from insightly.nodes.sql import SQLConverterNode, ConvertToSQL
from insightly.nodes.sql_or_plot import SQLOrPlotNode, CheckIfSQLOrPlotReturn
from insightly.nodes.sql import ExecuteSQL
from insightly.classes import AgentState
from insightly.nodes.state import State

load_dotenv()

logger = logging.getLogger("Insightly")


def relevance_router(state: AgentState) -> State:
    if state["relevance"] == "relevant":
        return State.CHECK_IF_SQL_OR_PLOT
    else:
        return State.GENERATE_FUNNY_RESPONSE


class ConfigSchema(TypedDict):
    insightly: Insightly


def main() -> None:
    """
    Main function to demonstrate the usage of the Insightly class.
    """

    path_to_csv: str = "/Users/patrickwalsh/dev/dataly-backend/data/titanic/train.csv"

    insightly = Insightly()
    insightly.read_csv_to_duckdb(path_to_csv, "titanic")
    # table = insightly.retrieve_table("db.foods")
    # print(table.df())
    logger = logging.getLogger("Insightly")
    relevance_checker = CheckRelevanceNode[CheckRelevance]()
    sql_converter = SQLConverterNode[ConvertToSQL]()
    sql_or_plot_checker = SQLOrPlotNode[CheckIfSQLOrPlotReturn]()
    execute_sql = ExecuteSQL[duckdb.DuckDBPyRelation]()

    workflow = StateGraph(AgentState, ConfigSchema)
    workflow.add_node(State.CHECK_RELEVANCE, relevance_checker.run)
    workflow.add_node(State.CONVERT_NL_TO_SQL, sql_converter.run)
    workflow.add_node(State.EXECUTE_SQL, execute_sql.run)

    # if the question is relevant, make a query. If not, generate a funny response
    workflow.add_conditional_edges(State.CHECK_RELEVANCE, relevance_router)

    # checking if the question is meant to be answered with SQL statement or a plot
    workflow.add_node(State.CHECK_IF_SQL_OR_PLOT, sql_or_plot_checker.run)
    workflow.add_edge(State.CHECK_IF_SQL_OR_PLOT, State.CONVERT_NL_TO_SQL)

    # if the question is meant to be answered with SQL,
    # convert the natural language question to SQL query
    # workflow.add_edge(State.CONVERT_NL_TO_SQL, State.EXECUTE_SQL)
    workflow.set_entry_point(State.CHECK_RELEVANCE)
    workflow.add_edge(State.CONVERT_NL_TO_SQL, END)

    app = workflow.compile()

    question = "What is the average age of passengers who survived?"
    result: dict[str, dict[str, Any]] = app.invoke(
        {"question": question, "attempts": 0}, config=dict(insightly=insightly)
    )


if __name__ == "__main__":
    main()
