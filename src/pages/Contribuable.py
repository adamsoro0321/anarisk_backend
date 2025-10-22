import time
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc

dash.register_page(__name__, path="/contribuables")

is_processing = False
layout = html.Div(
    [
        html.Div(
            [
                dbc.Label("IFU", html_for="input-ifu"),
                html.Div(
                    [
                        dbc.Input(
                            type="text",
                            id="input-ifu",
                            value="",
                            placeholder="Enter IFU",
                        ),
                        dbc.Button(
                            "Verifier",
                            id="submit-button",
                            color="success",
                            className="me-1",
                            n_clicks=0,
                        ),
                    ],
                    className="mt-2 mb-2  d-flex align-items-center",
                ),
                dbc.Spinner(
                    html.Div(id="loading-output"),
                    color="success",
                    type="grow",
                    spinner_style={"width": "2rem", "height": "2rem"},
                    fullscreen=True,
                ),
            ],
            style={"marginBottom": "20px"},
            className="success",
        ),
    ],
    className="container mt-4",
)


@callback(
    Output("loading-output", "children"),
    Input("submit-button", "n_clicks"),
    State("input-ifu", "value"),
    prevent_initial_call=True,
)
def update_output(n_clicks, ifu_value):
    print(f"Callback {n_clicks} triggered with IFU: {ifu_value}")
    time.sleep(5)  # Simulate a processing delay
    ##recherche tous les informations du contribuable et calcule les indicateurs
    return "Please enter an IFU and click submit."
