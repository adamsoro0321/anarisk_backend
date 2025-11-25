import dash
from dash import html, dcc, callback, Input, Output
from flask_login import logout_user, current_user
import dash_bootstrap_components as dbc

dash.register_page(__name__, path='/logout')

layout = html.Div([
    dcc.Location(id='logout-url', refresh=True),
    html.Div(id='logout-trigger')
])

@callback(
    Output('logout-url', 'pathname'),
    Input('logout-trigger', 'children')
)
def logout(trigger):
    if current_user.is_authenticated:
        logout_user()
    return '/login'
