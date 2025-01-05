from collections import deque
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import threading


class PlotyVisualiserV2:
    def __init__(self, buffer_size=100):
        """Initialize the visualizer with a deque buffer and a Dash app."""
        self.data_buffer = deque(maxlen=buffer_size)
        self.app = Dash(__name__)
        self.setup_layout()

    def setup_layout(self):
        """Setup the Dash app layout with a graph and an interval component."""
        self.app.layout = html.Div(
            [
                dcc.Graph(id="live-chart"),
                dcc.Interval(
                    id="interval-component", interval=1000, n_intervals=0
                ),  # Update every second
            ]
        )

        @self.app.callback(
            Output("live-chart", "figure"), [Input("interval-component", "n_intervals")]
        )
        def update_chart(n):
            if not self.data_buffer:
                return go.Figure()

            # Create a DataFrame from the buffer
            df = pd.DataFrame(self.data_buffer, columns=["timestamp", "value"])

            # Group by timestamp and use the mean (or last value) for duplicate timestamps
            df = (
                df.groupby("timestamp", as_index=False)
                .mean()  # Change 'mean' to 'last' if you prefer the last value
                .sort_values("timestamp")
            )

            # Create the figure
            figure = go.Figure(
                data=[go.Scatter(x=df["timestamp"], y=df["value"], mode="lines")],
                layout=go.Layout(title="Stock Market Analysis", template="plotly_dark"),
            )
            return figure

    def append_data(self, timestamp, value):
        """Append new data to the buffer."""
        self.data_buffer.append((timestamp, value))

    def run_server(self):
        """Run the Dash server in a separate thread."""
        threading.Thread(
            target=self.app.run_server, kwargs={"debug": True, "use_reloader": False}
        ).start()
