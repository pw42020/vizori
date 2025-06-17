from loguru import logger
from typing import Any
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from insightly.classes import Schema, Table, VariableType
from insightly.insightly import Insightly
from insightly.workflow import create_and_compile_workflow, ask

load_dotenv()

ROOT_PATH: str = str(Path(__file__).resolve()).split("src")[0]


def read_csv_to_duckdb(
    conn: duckdb.DuckDBPyConnection, path_to_csv: str, table_name: str = "table"
) -> None:
    """
    Reads a CSV file into a DuckDB table.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        The DuckDB connection object.
    path_to_csv : str
        The path to the CSV file.
    table_name : str, optional
        The name of the table to create in DuckDB (default is "titanic").

    Returns
    -------
    None
    """
    # self.db_name = table_name
    conn.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM '{path_to_csv}'
            """
    )
    # set the list of tables for the database for later usage
    tables_tuple = conn.execute("PRAGMA show_tables;").fetchall()
    tables = [t[0] for t in tables_tuple]
    logger.info("tables: {tables}".format(tables=tables))

    return tables


def create_schema_from(conn: duckdb.DuckDBPyConnection, tables: list[str]) -> None:
    # schema = Schema()
    # get the schema with each of the table names

    tables_for_schema: dict[str, Table] = {}
    for table in tables:
        # Fetch column info
        table_fields: list[list[str, str]] = conn.execute(
            f"PRAGMA table_info('memory.{table}')"
        ).fetchall()

        # Format into VariableType
        table_fields_dict: dict[str, VariableType] = {}
        for field in table_fields:
            table_fields_dict[field[1]] = getattr(VariableType, field[2])
        tables_for_schema.update({table: Table(fields=table_fields_dict)})

    return Schema(tables=tables_for_schema)


def main() -> None:
    """
    Main function to demonstrate the usage of the Insightly class.
    """

    path_to_csv: str = f"{ROOT_PATH}/data/titanic/train.csv"
    # print(insightly is insightly)
    insightly1 = Insightly()
    conn = duckdb.connect()
    tables: list[str] = read_csv_to_duckdb(conn, path_to_csv, "titanic")
    schema: Schema = create_schema_from(conn, tables)

    insightly1.add_schema(schema)
    insightly1.create_table_from_schema("titanic")

    _, app = create_and_compile_workflow()

    # Assuming you have already created and compiled your graph as 'app'
    # png_graph = app.get_graph().draw_mermaid_png()
    # # save png graph to file
    # with open("workflow_graph.png", "wb") as f:
    #     f.write(png_graph)

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
