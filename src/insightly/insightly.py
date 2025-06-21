from __future__ import annotations
from loguru import logger
from typing import Optional

import duckdb

from insightly.classes import Schema, Table, NewField


class Singleton(type):
    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            instance = super(Singleton, cls).__call__()
            cls._instances[cls] = instance
        return cls._instances[cls]


class Insightly(metaclass=Singleton):
    """
    A class to represent a connection to a DuckDB database.

    Attributes
    ----------
    conn : duckdb.DuckDBPyConnection
        The DuckDB connection object.
    """

    conn: duckdb.DuckDBPyConnection = None
    db_name: Optional[str] = None
    tables: list[str] = []
    schema: Schema
    # _instance: Optional[Insightly] = None

    def __init__(self) -> None:
        self.conn = duckdb.connect(database=":memory:")
        self.db_name = "memory"
        self.tables = []
        self._instance = None

    def add_schema(self, schema: Schema) -> None:
        self.schema = schema

    def create_table_from_schema(self, table_name: str) -> None:
        """
        Creates a DuckDB table from the schema.

        Parameters
        ----------
        table_name : str
            The name of the table to create in DuckDB.

        Returns
        -------
        None
        """
        if self.schema is None:
            raise ValueError(
                "Schema is not set. Please set the schema before creating a table."
            )

        # Create the table using the schema
        fields = ", ".join(
            f"{name} {var_type.value}"
            for name, var_type in self.schema.tables[table_name].fields.items()
        )
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields})"
        logger.debug(create_table_query)
        self.conn.execute(create_table_query)
        logger.info(f"Table '{table_name}' created with schema: {fields}")

    # def __new__(cls):
    #     """
    #     Create a new instance of the Insightly class if it doesn't already exist.
    #     Used for singleton design pattern across different nodes.
    #     """
    #     if cls._instance is None:
    #         logger = logging.getLogger("Insightly")
    #         logger.critical("Creating new instance")
    #         cls._instance = super(Insightly, cls).__new__(cls)
    #     return cls._instance

    # reading the CSV file into a DuckDB table
    # def read_csv_to_duckdb(self, path_to_csv: str, table_name: str = "table") -> None:
    #     """
    #     Reads a CSV file into a DuckDB table.

    #     Parameters
    #     ----------
    #     conn : duckdb.DuckDBPyConnection
    #         The DuckDB connection object.
    #     path_to_csv : str
    #         The path to the CSV file.
    #     table_name : str, optional
    #         The name of the table to create in DuckDB (default is "titanic").

    #     Returns
    #     -------
    #     None
    #     """
    #     # self.db_name = table_name
    #     self.conn.execute(
    #         f"""
    #         CREATE TABLE IF NOT EXISTS {table_name} AS
    #         SELECT * FROM '{path_to_csv}'
    #         """
    #     )
    #     # set the list of tables for the database for later usage
    #     tables_tuple = self.conn.execute("PRAGMA show_tables;").fetchall()
    #     self.tables = [t[0] for t in tables_tuple]
    #     logger.info("tables: {tables}".format(tables=self.tables))

    # # reading the CSV file into a DuckDB table
    # def read_mysql_db(self, path_to_db: str, db_name: str) -> None:
    #     """
    #     Reads a CSV file into a DuckDB table.

    #     Parameters
    #     ----------
    #     conn : duckdb.DuckDBPyConnection
    #         The DuckDB connection object.
    #     path_to_db : str
    #         The path to the db file.
    #     db_name : str, optional
    #         The name of the table to create in DuckDB (default is "titanic").

    #     Returns
    #     -------
    #     None
    #     """
    #     self.db_name = db_name
    #     query: str = f"""
    #         CALL sqlite_attach('{path_to_db}');
    #         ATTACH '{path_to_db}' AS {db_name} (TYPE sqlite);
    #     """
    #     logger.debug(query)
    #     self.conn.execute(query)

    #     # set the list of tables for the database for later usage
    #     tables_tuple = self.conn.execute("PRAGMA show_tables;").fetchall()
    #     self.tables = [t[0] for t in tables_tuple]

    # def retrieve_table(self, table_name: str) -> duckdb.DuckDBPyRelation:
    #     """
    #     Retrieves a DuckDB table as a relation.

    #     Parameters
    #     ----------
    #     conn : duckdb.DuckDBPyConnection
    #         The DuckDB connection object.
    #     table_name : str, optional
    #         The name of the table to retrieve (default is "titanic").

    #     Returns
    #     -------
    #     duckdb.DuckDBPyRelation
    #         The relation representing the DuckDB table.
    #     """
    #     return self.conn.table(table_name)

    # # get the schema using duckdb
    # def get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
    #     # get the schema with each of the table names
    #     tables = [table_name] if table_name is not None else self.tables
    #     schemas = {}

    #     for table in tables:
    #         # Fetch column info
    #         info = self.conn.execute(
    #             f"PRAGMA table_info('{self.db_name}.{table}')"
    #         ).fetchall()

    #         # Format into a string
    #         schema_string = ", ".join(f"{col[1]} {col[2]}" for col in info)

    #         schemas[table] = schema_string

    #     schema = ""
    #     for key, value in schemas.items():
    #         schema += "Table name: " + key + "\n"
    #         schema += value + "\n"

    #     return str(schema)

    # def add_df_to_duckdb(self, df: pd.DataFrame, table_name: str) -> None:
    #     """
    #     Inserts a DataFrame into a DuckDB table.

    #     Parameters
    #     ----------
    #     conn : duckdb.DuckDBPyConnection
    #         The DuckDB connection object.
    #     df : pd.DataFrame
    #         The DataFrame to insert. 
    #     table_name : str
    #         The name of the table to insert into.

    #     Returns
    #     -------
    #     None
    #     """
    #     # Create the table if it doesn't exist
    #     self.conn.execute(
    #         f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0"
    #     )

    #     # Insert the DataFrame into the table
    #     self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")

    #     # Commit the changes
    #     self.conn.commit()

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query on the DuckDB connection.

        Parameters
        ----------
        query : str
            The SQL query to execute

        Returns
        -------
        pd.DataFrame
            The result of the executed query as a relation.
        """
        executed_query = self.conn.sql(query)
        # commit
        self.conn.commit()
        return executed_query

    def add_to_schema(self, additions: list[NewField]) -> None:
        """
        Adds new columns to the schema.

        Parameters
        ----------
        additions : dict[str, dict[str, VariableType]]
            A dictionary where keys are table names and values are dictionaries
            with column names as keys and their types as values.

        Returns
        -------
        None
        """
        for new_field in additions:
            if new_field.table_name in self.schema.tables:
                logger.info(
                    f"Adding new columns to existing table: {new_field.table_name}"
                )
                self.schema.tables[new_field.table_name].fields.update(
                    {new_field.field_name: new_field.field_type}
                )
            else:
                logger.info(f"Adding new table to schema: {new_field.table_name}")
                self.schema.tables.update(
                    {
                        new_field.table_name: Table(
                            fields={new_field.field_name: new_field.field_type}
                        )
                    }
                )
