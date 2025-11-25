import pandas as pd
import dash
from dash import html, dcc
import dash_mantine_components as dmc
import os
import sys
import logging
from db.ods import connectionOds
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user, login_required
from core.data_loader import DataLoader
from core.risk_compute import RiskComputer

from layout import top_bar
import globals as app_globals
import dash_bootstrap_components as dbc

from models import User
from globals import Session

# Configuration du logging pour l'application
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Pour afficher dans la console
    ],
)

# Ajouter le dossier src au path
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))


# Initialisation des connexions aux bases de données
oracle_engine = connectionOds()
loader = DataLoader(oracle_engine)

risk_analyzer = RiskComputer(data_loader=loader)

# Stocker l'analyseur dans le module global pour partage avec les pages
app_globals.set_risk_analyzer(risk_analyzer)
style_sheet = "./src/style.css"
# Initialisation de l'application Dash avec DMC
app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME, style_sheet],
)
server = app.server
server.secret_key = os.urandom(24)

# Flask-Login setup
login_manager = LoginManager()
login_manager.init_app(server)
login_manager.login_view = '/login'


@login_manager.user_loader
def load_user(user_id):
    if not Session:
        return None
    session = Session()
    user = session.query(User).get(int(user_id))
    session.close()
    return user
app.title = "DGI - Analyse des Risques Fiscaux"


def serve_layout():
    return html.Div(
        [
            dcc.Location(id='url', refresh=True), # Added for routing/redirects
            # Store pour partager risk_analyzer
            top_bar(),
         
        ],
        className="vh-100 ",
    )

app.layout = serve_layout


@server.before_request
def protect_views():
    if request.path.startswith('/assets') or request.path.startswith('/_dash'):
        return None
    
    if request.path == '/login' or request.path == '/logout':
        return None
        
    if not current_user.is_authenticated:
        return redirect('/login')

if __name__ == "__main__":
    print("Starting Dash application...")
    print("Navigate to http://127.0.0.1:8050 to view the application")
    app.run_server(debug=True, host="127.0.0.1", port=8050)
