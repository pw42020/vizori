from loguru import logger
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from insightly.plots.plot import Plot


class BarPlot(Plot):
    def generate(self, df: pd.DataFrame, columns: list[str]) -> go.Figure:
        # columns = state.get("columns", [])
        # if len(columns) != 2:
        #     raise ValueError("Bar plot requires exactly one column.")
        logger.debug("GENERATE BAR PLOT")
        logger.debug(df)

        # Generate a bar plot (this is a placeholder, actual plotting code would go here)
        return px.bar(df, x=columns[0], y=columns[1])
