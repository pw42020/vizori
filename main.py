import sys
import os
import logging
from pathlib import Path
from typing import Any

from firebase_functions import https_fn
from firebase_admin import initialize_app
import plotly.graph_objects as go

ROOT_PATH = Path(__file__).parent

from insightly.insightly import Insightly
from insightly.workflow import create_and_compile_workflow, ask

initialize_app()


@https_fn.on_request()
def ask_question(req: https_fn.Request) -> https_fn.Response:
    """Ask a question to the Insightly app and get a response.

    Parameters
    ----------
    req : https_fn.Request
        The request object containing the question to ask.

    Returns
    -------
    https_fn.Response
        The response object containing the result of the question.
    """
    logger = logging.getLogger("Insightly")
    path_to_csv: str = f"{ROOT_PATH}/data/titanic/train.csv"
    insightly = Insightly()
    insightly.read_csv_to_duckdb(path_to_csv, "titanic")

    _, app = create_and_compile_workflow()

    # question = "What is the average age of passengers who survived?"
    question = "Create a bar plot that shows the number offunctions passengers, grouped by 10 year age buckets"
    result: dict[str, dict[str, Any]] = ask(app, question)
    if result.get("meant_as_query", False):
        # if the SQL query was executed successfully, print the result
        logger.info(result["sql_query_info"]["success_response"])
        logger.info(
            "Result: {res}".format(res=result["sql_query_info"]["query_result"])
        )
        return https_fn.Response(result["sql_query_info"]["success_response"])
    else:
        # if the plot was generated successfully, show the plot that was returned
        figure: go.Figure = result["plot_query_info"]["result"]
        # turn figure into html and submit it as Jinja template
        figure_html = figure.to_html(full_html=False, include_plotlyjs="cdn")

        return https_fn.Response(figure_html)
