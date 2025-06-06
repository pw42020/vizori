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

    # table = insightly.retrieve_table("db.foods")
    # print(table.df())
    # logger = logging.getLogger("Insightly")

    question = (
        "Can you plot the correlation between age and survival rate on the Titanic?"
    )
    # question = "Create a bar plot that shows the number of passengers, grouped by 10 year age buckets"
    result: dict[str, dict[str, Any]] = ask(app, question)
    if result.get("meant_as_query", False):
        # if the SQL query was executed successfully, print the result
        logger.info(result["sql_query_info"]["success_response"])
        logger.info("Result:", result["sql_query_info"]["query_result"])
    else:
        # if the plot was generated successfully, show the plot that was returned
        result["plot_query_info"]["result"].show()


if __name__ == "__main__":
    main()
