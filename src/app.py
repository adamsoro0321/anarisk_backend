import pandas as pd
import dash
from dash import html, dcc
import dash_mantine_components as dmc
import os
import sys
import logging
from db.ods import connectionOds

from core.data_loader import DataLoader
from core.risk_compute import RiskComputer

from layout import top_bar
import globals as app_globals
import dash_bootstrap_components as dbc


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
app.title = "DGI - Analyse des Risques Fiscaux"


app.layout = html.Div(
    [
        # Store pour partager risk_analyzer
        top_bar(),
     
    ],
    className="vh-100 ",
)


if __name__ == "__main__":
    print("Starting Dash application...")
    print("Navigate to http://127.0.0.1:8050 to view the application")
    app.run_server(debug=True, host="127.0.0.1", port=8050)
