from enum import Enum


class State(str, Enum):
    CHECK_RELEVANCE: str = "check_relevance"
    CHECK_IF_SQL_OR_PLOT: str = "check_if_sql_or_plot"
    CONVERT_NL_TO_SQL: str = "convert_nl_to_sql"
    GET_COLUMNS: str = "get_columns"
    GENERATE_SCATTER_PLOT: str = "generate_scatter_plot"
    GENERATE_FUNNY_RESPONSE: str = "generate_funny_response"
    REGENERATE_QUERY: str = "regenerate_query"
    EXECUTE_SQL: str = "execute_sql"
    CHECK_IF_ERROR: str = "check_if_error"
    GENERATE_SUCCESS_RESPONSE: str = "generate_human_response"
    GET_COLUMNS_IF_PLOT: str = "get_columns_if_plot"
