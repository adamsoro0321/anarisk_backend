import dash
from dash import html


dash.register_page(__name__, path="/dashboard",title="Dashboard", order=3)

layout = html.Div(
    [
        html.H1("This is our Dashboard page"),
        html.Div("This is our Dashboard page content."),
    ]
)
