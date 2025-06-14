from loguru import logger
from typing import Any
from pathlib import Path

from dotenv import load_dotenv
from insightly.insightly import Insightly
from insightly.workflow import create_and_compile_workflow, ask

load_dotenv()

ROOT_PATH: str = str(Path(__file__).resolve()).split("src")[0]


def main() -> None:
    """
    Main function to demonstrate the usage of the Insightly class.
    """

    path_to_csv: str = f"{ROOT_PATH}/data/titanic/train.csv"
    # print(insightly is insightly)
    insightly1 = Insightly()
    insightly1.read_csv_to_duckdb(path_to_csv, "titanic")
    insightly2 = Insightly()
    logger.debug(insightly1 is insightly2)
    logger.debug(Insightly().get_schema())

    _, app = create_and_compile_workflow()

    # Assuming you have already created and compiled your graph as 'app'
    png_graph = app.get_graph().draw_mermaid_png()
    # save png graph to file
    with open("workflow_graph.png", "wb") as f:
        f.write(png_graph)

    question = (
        "Can you plot the correlation between age and survival rate on the Titanic?"
        # "What's my favorite color?"
    )
    # question = "Create a bar plot that shows the number of passengers, grouped by 10 year age buckets"
    result: dict[str, dict[str, Any]] = ask(app, question)
    print(result)
    # if the SQL query was executed successfully, print the result
    if result.get("relevance") == "relevant":
        logger.info("SQL Query: ", result["sql_query"])
        logger.info(result["response"])
    else:
        logger.warning("Question is not relevant to the database schema.")


if __name__ == "__main__":
    main()
