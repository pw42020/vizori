import sys
import os
from pathlib import Path
from firebase_functions import https_fn
from firebase_admin import initialize_app

ROOT_PATH = Path(__file__).parent

sys.path.append(f"{ROOT_PATH}/src")

# from insightly.insightly import Insightly
from insightly.insightly import Insightly

initialize_app()


@https_fn.on_request()
def on_request_example(req: https_fn.Request) -> https_fn.Response:
    path_to_csv: str = f"{ROOT_PATH}/data/titanic/train.csv"
    insightly = Insightly()
    insightly.read_csv_to_duckdb(path_to_csv, "titanic")
    # table = insightly.retrieve_table("db.foods")
    # print(table.df())
    # logger = logging.getLogger("Insightly")

    question = "What is the average age of passengers who survived?"
    # question = "Create a bar plot that shows the number of passengers, grouped by 10 year age buckets"
    result: dict[str, dict[str, Any]] = insightly.ask(question)
    if result.get("meant_as_query", False):
        # if the SQL query was executed successfully, print the result
        print(result["sql_query_info"]["success_response"])
        print("Result:", result["sql_query_info"]["query_result"])
    else:
        # if the plot was generated successfully, show the plot that was returned
        result["plot_query_info"]["result"].show()
    return https_fn.Response("Hello world!")