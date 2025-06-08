"""FastAPI application that retrieves queries from a CSV file using Insightly."""

import os
from pathlib import Path
from typing import Any

ROOT_PATH: str = str(Path(__file__).resolve()).split("app/", maxsplit=1)[0]

from loguru import logger
from dotenv import load_dotenv
import plotly.graph_objects as go
from supabase import Client, create_client

from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse

from insightly.workflow import create_and_compile_workflow, ask
from insightly.insightly import Insightly
from insightly.classes import AgentState

# loading environment variables that store the supabase URL and API key
load_dotenv()

app = FastAPI()


def create_supabase_client() -> Client:
    """create the supabase client using the environment variables.

    Returns
    -------
    Client
        The supabase client connected to the backend
    """
    api_url: str = os.getenv("SUPABASE_URL")
    key: str = os.getenv("SUPABASE_ADMIN")
    supabase: Client = create_client(api_url, key)
    return supabase


app = FastAPI()

# Initialize supabase client
supabase = create_supabase_client()


@app.get("/query")
def ask_question(question: str) -> Any:
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
    path_to_csv: str = f"{ROOT_PATH}/data/titanic/train.csv"
    insightly = Insightly()
    insightly.read_csv_to_duckdb(path_to_csv, "titanic")

    _, app = create_and_compile_workflow()

    # question = "What is the average age of passengers who survived?"
    result: AgentState = ask(app, question)
    if result.get("meant_as_query", False):
        # if the SQL query was executed successfully, print the result
        logger.info(result["sql_query_info"]["success_response"])
        logger.info(
            "Result: {res}".format(res=result["sql_query_info"]["query_result"])
        )

        return JSONResponse(content=result)
    else:
        # if the plot was generated successfully, show the plot that was returned
        figure: go.Figure = result["plot_query_info"]["result"]
        # turn figure into html and submit it as Jinja template
        figure_html = figure.to_html(full_html=True, include_plotlyjs="cdn")

        return HTMLResponse(content=figure_html)
