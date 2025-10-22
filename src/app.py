import pandas as pd
import dash
from dash import Output, Input, State, html, dcc
import dash_mantine_components as dmc
import os
import sys
import logging
from db.ods import connectionOds
from db.pg_connection import connect_pg
from core.data_loader import DataLoader
from core.risk_compute import RiskComputer

from layout import top_bar
from src import globals as app_globals
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


# Configuration des variables d'environnement
POSTGRES_DB_HOST = os.getenv("ODS_PG_HOST", "10.3.1.129")
POSTGRES_DB_PORT = os.getenv("ODS_PG_PORT", "5432")
POSTGRES_DB_USER = os.getenv("ODS_PG_USER", "postgres")
POSTGRES_DB_PASSWORD = os.getenv("ODS_PG_PASSWORD", "cpf2022")
POSTGRES_DB_NAME = os.getenv("ODS_PG_DB", "postgres")


# Initialisation des connexions aux bases de données
oracle_engine = connectionOds()

# Tentative de connexion PostgreSQL (optionnelle)
try:
    postgres_engine = connect_pg(
        POSTGRES_DB_USER,
        POSTGRES_DB_PASSWORD,
        POSTGRES_DB_HOST,
        POSTGRES_DB_PORT,
        POSTGRES_DB_NAME,
    )
    logging.info("PostgreSQL connection successful")
except Exception as e:
    postgres_engine = None
    logging.warning(f"PostgreSQL connection failed (proceeding without it): {e}")


# Stocker l'analyseur dans le module global pour partage avec les pages
app_globals.set_risk_analyzer()
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
        # top_bar(),
        # create_professional_layout(),
    ],
    className="vh-100 ",
)


if __name__ == "__main__":
    print("Starting Dash application...")
    print("Navigate to http://127.0.0.1:8050 to view the application")
    app.run_server(debug=True, host="127.0.0.1", port=8050)
