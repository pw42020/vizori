from abc import ABC, abstractmethod

import pandas as pd
import plotly.graph_objects as go

class Plot(ABC):
    @abstractmethod
    def generate(self, df: pd.DataFrame, columns: list[str]) -> go.Figure:
        """Plot the data and return the path to the plot.

        Parameters
        ----------
        can put anything you want into this bad boy

        Returns
        -------
        str
            The path to the plot.
        """
        pass
