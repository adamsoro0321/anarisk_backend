import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
from flask_login import login_user, current_user
from werkzeug.security import check_password_hash
from globals import Session
from models import User

dash.register_page(__name__, path='/login')

layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(
                dbc.Card(
                    [
                        dbc.CardHeader("Connexion - AnaRisk"),
                        dbc.CardBody(
                            [
                                html.Div(id="login-alert"),
                                dbc.Label("Email"),
                                dbc.Input(id="email-input", type="email", placeholder="Entrez votre email"),
                                html.Br(),
                                dbc.Label("Mot de passe"),
                                dbc.Input(id="password-input", type="password", placeholder="Entrez votre mot de passe"),
                                html.Br(),
                                dbc.Button("Se connecter", id="login-button", color="success", className="w-100"),
                            ]
                        ),
                    ],
                    className="shadow-sm",
                ),
                width={"size": 4, "offset": 4},
            ),
            className="mt-5",
        )
    ],
    fluid=True,
)

@callback(
    Output("login-alert", "children"),
    Output("url", "pathname", allow_duplicate=True),
    Input("login-button", "n_clicks"),
    State("email-input", "value"),
    State("password-input", "value"),
    prevent_initial_call=True
)
def login(n_clicks, email, password):
    if not n_clicks:
        return dash.no_update, dash.no_update
    
    if not email or not password:
        return dbc.Alert("Veuillez remplir tous les champs.", color="warning"), dash.no_update

    if not Session:
        return dbc.Alert("Erreur de connexion à la base de données.", color="danger"), dash.no_update

    session = Session()
    try:
        user = session.query(User).filter_by(mail=email).first()
        
        if user and user.check_password(password):
            if user.status != 'active':
                 return dbc.Alert("Compte désactivé.", color="warning"), dash.no_update
            
            login_user(user)
            return dbc.Alert("Connexion réussie!", color="success"), "/"
        else:
            return dbc.Alert("Email ou mot de passe incorrect.", color="danger"), dash.no_update
    except Exception as e:
        return dbc.Alert(f"Erreur: {str(e)}", color="danger"), dash.no_update
    finally:
        session.close()
