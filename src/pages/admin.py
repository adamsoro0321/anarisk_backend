import time
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
from core.data_loader import DataLoader
from core.risk_compute import RiskComputer

dash.register_page(__name__, path="/admin",order=5) 
is_processing = False
layout = html.Div(
    [
        dbc.Container(
            [
                dbc.Button(
                    "Executer",
                    color="success",
                    id="calculate-indicators-button",
                    className="me-1",
                ),
            ],
            
            fluid=True,
        )
    ]
)