
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from insightly.plots.plot import Plot

class ScatterPlot(Plot):
    def generate(self, df: pd.DataFrame, columns: list[str]) -> go.Figure:
        # columns = state.get("columns", [])
        if len(columns) != 2:
            raise ValueError("Scatter plot requires exactly two columns.")
        
        # Generate a scatter plot (this is a placeholder, actual plotting code would go here)
        return px.scatter(df, x=columns[0], y=columns[1])