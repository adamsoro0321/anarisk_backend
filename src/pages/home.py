import dash
from dash import html, Input, Output, State, callback, dcc
import pandas as pd
import os
from src.layout import render_table, create_professional_layout
from src import globals as app_globals
import logging
import dash_ag_grid as dag
from dash_iconify import DashIconify
from src.utils.util import COLS_DAG
from dash import ctx
import dash_bootstrap_components as dbc

from core.data_loader import DataLoader
from core.risk_compute import RiskComputer

dash.register_page(__name__, path="/")

day_file = "./data/df_model.csv"
df_risk_day = pd.read_csv(day_file)


grid = dag.AgGrid(
    id="get-started-example-basic",
    rowData=df_risk_day.to_dict("records"),
    columnDefs=COLS_DAG,  # Initialiser avec toutes les colonnes, le callback s'occupera de la visibilité
    defaultColDef={
        "editable": True,
        "resizable": True,
        "sortable": True,
        "filter": True,
    },
    # Configuration de l'export CSV
    csvExportParams={
        "fileName": f"donnees_contribuables_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
        "columnSeparator": ",",
        "suppressQuotes": False,
    },
    # Configuration de la pagination et options personnalisées
    dashGridOptions={
        "pagination": True,
        "paginationPageSize": 20,
        "paginationPageSizeSelector": [10, 20, 50, 100],
        "paginationAutoPageSize": False,
        "suppressPaginationPanel": False,
        "getRowId": {"function": "params => params.data.NUM_IFU"},
        "rowSelection": {
            "mode": "multiRow",
        },
    },
    # Options de base supportées
    style={"height": "700px", "width": "100%"},
    className="ag-theme-alpine",
)


header = html.Div(
    [
        html.Div(
            [
                dbc.Button(
                    "Executer",
                    color="success",
                    id="calculate-indicators-button",
                    className="me-1",
                ),
                # Sélecteur de colonnes avec icône
                html.Div(
                    [
                        html.Div(
                            [
                                DashIconify(
                                    icon="material-symbols:view-column",
                                    width=32,
                                    height=32,
                                    style={
                                        "color": "#2e7d32",
                                        "transition": "all 0.3s ease",
                                    },
                                )
                            ],
                            id="toggle-column-selector",
                            style={
                                "cursor": "pointer",
                                "padding": "8px",
                                "borderRadius": "50%",
                                "backgroundColor": "transparent",
                                "display": "flex",
                                "alignItems": "center",
                                "justifyContent": "center",
                                "transition": "all 0.3s ease",
                                "width": "48px",
                                "height": "48px",
                            },
                            className="column-selector-icon",
                        ),
                        # Overlay de fond (pour fermer en cliquant à l'extérieur)
                        html.Div(
                            id="column-panel-overlay",
                            style={
                                "display": "none",
                                "position": "fixed",
                                "top": "0",
                                "left": "0",
                                "width": "100%",
                                "height": "100%",
                                "backgroundColor": "rgba(0,0,0,0.3)",
                                "zIndex": "999",
                            },
                        ),
                        # Panel de sélection des colonnes (masqué par défaut)
                        html.Div(
                            [
                                # Header du panel avec titre et bouton fermer
                                html.Div(
                                    [
                                        html.H4(
                                            "Sélectionner :",
                                            style={
                                                "color": "#2e7d32",
                                                "margin": "0",
                                                "fontSize": "1.1rem",
                                                "fontWeight": "600",
                                            },
                                        ),
                                        html.Button(
                                            html.I(className="fas fa-times"),
                                            id="close-column-panel-x",
                                            style={
                                                "backgroundColor": "transparent",
                                                "border": "none",
                                                "color": "#666",
                                                "cursor": "pointer",
                                                "fontSize": "1.2rem",
                                                "padding": "4px",
                                                "borderRadius": "4px",
                                                "transition": "color 0.3s ease",
                                            },
                                        ),
                                    ],
                                    style={
                                        "display": "flex",
                                        "justifyContent": "space-between",
                                        "alignItems": "center",
                                        "marginBottom": "15px",
                                        "paddingBottom": "10px",
                                        "borderBottom": "1px solid #e0e0e0",
                                    },
                                ),
                                dcc.Checklist(
                                    id="column-checklist",
                                    options=[
                                        {
                                            "label": col.get("headerName", col["field"])
                                            + (
                                                " (obligatoire)"
                                                if col["field"]
                                                in ["NUM_IFU", "Actions"]
                                                else ""
                                            ),
                                            "value": col["field"],
                                            "disabled": col["field"]
                                            in [
                                                "NUM_IFU",
                                                "Actions",
                                            ],  # Désactiver les colonnes obligatoires
                                        }
                                        for col in COLS_DAG
                                    ],
                                    value=[
                                        col["field"]
                                        for col in COLS_DAG
                                        if col.get("showDefault", False)
                                    ],  # Colonnes avec showDefault=True par défaut
                                    style={"marginBottom": "15px"},
                                    inputStyle={"marginRight": "8px"},
                                    labelStyle={
                                        "display": "block",
                                        "marginBottom": "8px",
                                        "fontSize": "0.9rem",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Button(
                                            "Tout sélectionner",
                                            id="select-all-columns",
                                            style={
                                                "backgroundColor": "#4caf50",
                                                "color": "white",
                                                "border": "none",
                                                "padding": "8px 16px",
                                                "borderRadius": "6px",
                                                "cursor": "pointer",
                                                "fontSize": "0.85rem",
                                                "marginRight": "10px",
                                            },
                                        ),
                                        html.Button(
                                            "Tout désélectionner",
                                            id="deselect-all-columns",
                                            style={
                                                "backgroundColor": "#ff5722",
                                                "color": "white",
                                                "border": "none",
                                                "padding": "8px 16px",
                                                "borderRadius": "6px",
                                                "cursor": "pointer",
                                                "fontSize": "0.85rem",
                                                "marginRight": "10px",
                                            },
                                        ),
                                        html.Button(
                                            [
                                                html.I(
                                                    className="fas fa-check",
                                                    style={"marginRight": "6px"},
                                                ),
                                                "Appliquer",
                                            ],
                                            id="close-column-panel",
                                            style={
                                                "backgroundColor": "#2e7d32",
                                                "color": "white",
                                                "border": "none",
                                                "padding": "8px 16px",
                                                "borderRadius": "6px",
                                                "cursor": "pointer",
                                                "fontSize": "0.85rem",
                                                "fontWeight": "500",
                                                "display": "flex",
                                                "alignItems": "center",
                                            },
                                        ),
                                    ]
                                ),
                            ],
                            id="column-selector-panel",
                            className="column-selector-panel",
                            style={
                                "display": "none",  # Masqué par défaut
                                "position": "absolute",
                                "top": "60px",
                                "left": "0",
                                "backgroundColor": "white",
                                "border": "2px solid #e0e0e0",
                                "borderRadius": "10px",
                                "padding": "20px",
                                "boxShadow": "0 8px 24px rgba(0,0,0,0.15)",
                                "zIndex": "1000",
                                "minWidth": "300px",
                                "maxHeight": "400px",
                                "overflowY": "auto",
                            },
                        ),
                    ],
                    style={"flex": "1", "position": "relative"},
                ),
                # Boutons d'export
                html.Div(
                    [
                        html.Button(
                            [
                                html.I(
                                    className="fas fa-file-csv",
                                    style={"marginRight": "8px"},
                                ),
                                "CSV",
                            ],
                            id="export-csv-btn",
                            className="export-btn",
                            style={
                                "backgroundColor": "#4caf50",
                                "color": "white",
                                "border": "none",
                                "padding": "10px 20px",
                                "marginRight": "10px",
                                "borderRadius": "6px",
                                "cursor": "pointer",
                                "fontSize": "0.9rem",
                                "fontWeight": "500",
                                "display": "flex",
                                "alignItems": "center",
                                "boxShadow": "0 2px 4px rgba(76, 175, 80, 0.3)",
                                "transition": "all 0.3s ease",
                            },
                        ),
                        html.Button(
                            [
                                html.I(
                                    className="fas fa-file-excel",
                                    style={"marginRight": "8px"},
                                ),
                                "Excel",
                            ],
                            id="export-excel-btn",
                            className="export-btn",
                            style={
                                "backgroundColor": "#2e7d32",
                                "color": "white",
                                "border": "none",
                                "padding": "10px 20px",
                                "marginRight": "10px",
                                "borderRadius": "6px",
                                "cursor": "pointer",
                                "fontSize": "0.9rem",
                                "fontWeight": "500",
                                "display": "flex",
                                "alignItems": "center",
                                "boxShadow": "0 2px 4px rgba(46, 125, 50, 0.3)",
                                "transition": "all 0.3s ease",
                            },
                        ),
                        # Séparateur
                        html.Div(
                            style={
                                "width": "2px",
                                "height": "30px",
                                "backgroundColor": "#e0e0e0",
                                "margin": "0 15px",
                            }
                        ),
                        # Contrôles de pagination
                        html.Div(
                            [
                                html.Label(
                                    "Lignes par page:",
                                    style={
                                        "fontSize": "0.9rem",
                                        "color": "#666",
                                        "marginRight": "8px",
                                        "fontWeight": "500",
                                    },
                                ),
                                dcc.Dropdown(
                                    id="pagination-size-selector",
                                    options=[
                                        {"label": "10", "value": 10},
                                        {"label": "20", "value": 20},
                                        {"label": "50", "value": 50},
                                        {"label": "100", "value": 100},
                                        {
                                            "label": "Tout",
                                            "value": len(df_risk_day),
                                        },
                                    ],
                                    value=20,
                                    clearable=False,
                                    style={
                                        "width": "100px",
                                        "fontSize": "0.9rem",
                                    },
                                ),
                            ],
                            style={
                                "display": "flex",
                                "alignItems": "center",
                            },
                        ),
                    ],
                    style={"display": "flex", "alignItems": "center"},
                ),
            ],
            style={
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
                "padding": "20px",
                "backgroundColor": "#f8f9fa",
                "borderRadius": "10px 10px 0 0",
                "borderBottom": "2px solid #e0e0e0",
                "marginBottom": "0",
            },
        ),
    ]
)
layout = html.Div(
    [
        html.Div(
            [
                # create_professional_layout(),
                # Header de la grille avec boutons d'export
                html.Div(
                    [
                        # Header section
                        header,
                        # Container de la grille
                        html.Div(
                            [grid],
                            style={
                                "backgroundColor": "white",
                                "borderRadius": "0 0 10px 10px",
                                "overflow": "hidden",
                                "boxShadow": "0 2px 8px rgba(0,0,0,0.1)",
                            },
                        ),
                    ],
                    style={
                        "marginBottom": "20px",
                        "boxShadow": "0 4px 12px rgba(0,0,0,0.05)",
                        "borderRadius": "10px",
                        "overflow": "hidden",
                    },
                ),
                # Zone de statut des exports
                html.Div(id="export-status", style={"marginBottom": "20px"}),
                # Composant caché pour gérer les actions de la grille
                html.Div(id="grid-actions-trigger", style={"display": "none"}),
                html.Div(id="grid-actions-output", style={"marginTop": "20px"}),
                # html.Div(id="results-container", children=html.Div()),
            ]
        ),
    ]
)


def check_if_data_risk_day():
    """
    Vérifie si le fichier df_risk_day existe et contient des données.
    Si le fichier n'existe pas, il est créé avec une structure vide.
    """
    global df_risk_day
    if not os.path.exists(day_file):
        return False
    else:
        df_risk_day = pd.read_csv(day_file)
        if df_risk_day.empty:
            return False
        else:
            return True


@callback(
    Output("results-container", "children"),
    Input("calculate-indicators-button", "n_clicks"),
    State("indicateur-dropdown", "value"),
    State("date-picker-range", "start_date"),
    State("date-picker-range", "end_date"),
    prevent_initial_call=True,
)
def calculate_indicators(n_clicks, selected_indicators, start_date, end_date):
    """
    Fonction pour calculer les indicateurs de risque
    verifie si les risque sont calculer au cour de la journée
    si oui affiche le tableau
    sinon lance l'analyseur de risque
    """
    # Récupérer risk_analyzer depuis le module global
    risk_analyzer = app_globals.get_risk_analyzer()

    if not risk_analyzer:
        return "Analyseur de risques non initialisé. Veuillez redémarrer l'application."

    if not selected_indicators:
        return "Veuillez sélectionner au moins un indicateur."

    # Filtrage par période si des dates sont sélectionnées
    if start_date and end_date:
        # Message informatif sur la période sélectionnée
        period_info = html.Div(
            [
                html.P(
                    f"Période sélectionnée : du {start_date} au {end_date}",
                    style={
                        "color": "#2e7d32",
                        "fontWeight": "500",
                        "marginBottom": "20px",
                        "textAlign": "center",
                        "fontSize": "1.1rem",
                    },
                )
            ]
        )
    else:
        period_info = html.Div()

    is_day_risk_exist = check_if_data_risk_day()
    if not is_day_risk_exist:
        # Appel à l'analyseur de risques avec les indicateurs sélectionnés
        results = risk_analyzer.analyze(df_risk_day, selected_indicators)
        if results is None:
            return "Patientez, l'analyse est en cours..."
        else:
            # Filtrer les résultats selon la période si spécifiée
            df_filtered = filter_data_by_period(results, start_date, end_date)
            df_display = df_filtered.head(20).copy()
            return html.Div([period_info, show_df(df_display, selected_indicators)])
    else:
        # Filtrer les données selon la période si spécifiée
        df_filtered = filter_data_by_period(df_risk_day, start_date, end_date)
        df_display = df_filtered.head(
            20
        ).copy()  # Limiter à 20 lignes pour la performance

        return html.Div([period_info, show_df(df_display, selected_indicators)])


def filter_data_by_period(data, start_date, end_date):
    """
    Filtre les données selon la période sélectionnée
    """
    if not start_date or not end_date:
        return data

    # Convertir les dates si nécessaire
    try:
        # Supposons que la colonne ANNEE contient l'année fiscale
        start_year = pd.to_datetime(start_date).year
        end_year = pd.to_datetime(end_date).year

        # Filtrer par année
        filtered_data = data[
            (data["ANNEE"] >= start_year) & (data["ANNEE"] <= end_year)
        ]

        return filtered_data
    except Exception as e:
        logging.warning(f"Erreur lors du filtrage par période: {e}")
        return data


def show_df(df_display, selected_indicators):
    # Rendu du tableau en filtrant dynamiquement les colonnes de risque selon la sélection
    return render_table(df_display, selected_indicators=selected_indicators)


# Callbacks pour les exports
@callback(
    Output("get-started-example-basic", "exportDataAsCsv"),
    Input("export-csv-btn", "n_clicks"),
    prevent_initial_call=True,
)
def export_csv(n_clicks):
    """Export des données en CSV"""
    if n_clicks:
        return True
    return False


@callback(
    Output("export-status", "children"),
    Input("export-excel-btn", "n_clicks"),
    prevent_initial_call=True,
)
def handle_excel_export(excel_clicks):
    """Gère l'export Excel"""
    if not excel_clicks:
        return ""

    try:
        # Export Excel
        export_filename = (
            f"donnees_contribuables_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

        # Créer le dossier output s'il n'existe pas
        os.makedirs("./output", exist_ok=True)

        df_risk_day.to_excel(f"./output/{export_filename}", index=False)
        return html.Div(
            [
                html.I(
                    className="fas fa-check-circle",
                    style={"color": "#4caf50", "marginRight": "8px"},
                ),
                f"Export Excel réussi: {export_filename}",
            ],
            style={"color": "#4caf50", "padding": "10px", "fontWeight": "500"},
        )

    except Exception as e:
        return html.Div(
            [
                html.I(
                    className="fas fa-exclamation-triangle",
                    style={"color": "#f44336", "marginRight": "8px"},
                ),
                f"Erreur lors de l'export: {str(e)}",
            ],
            style={"color": "#f44336", "padding": "10px", "fontWeight": "500"},
        )


# Callback pour la pagination
@callback(
    Output("get-started-example-basic", "dashGridOptions"),
    Input("pagination-size-selector", "value"),
    prevent_initial_call=True,
)
def update_pagination_size(page_size):
    """Met à jour la taille de page de la pagination"""
    return {
        "pagination": True,
        "paginationPageSize": page_size,
        "paginationPageSizeSelector": [10, 20, 50, 100],
        "paginationAutoPageSize": False,
        "suppressPaginationPanel": False,
    }


# Callbacks pour la sélection de colonnes
@callback(
    [Output("column-selector-panel", "style"), Output("column-panel-overlay", "style")],
    [
        Input("toggle-column-selector", "n_clicks"),
        Input("close-column-panel", "n_clicks"),
        Input("close-column-panel-x", "n_clicks"),
        Input("column-panel-overlay", "n_clicks"),
    ],
    [State("column-selector-panel", "style"), State("column-panel-overlay", "style")],
    prevent_initial_call=True,
)
def toggle_column_panel(
    toggle_clicks,
    close_clicks,
    close_x_clicks,
    overlay_clicks,
    panel_style,
    overlay_style,
):
    """Affiche/masque le panel de sélection des colonnes"""

    if not ctx.triggered:
        return panel_style, overlay_style

    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "toggle-column-selector":
        # Toggle le panel (ouvrir/fermer)
        if panel_style.get("display") == "none":
            # Ouvrir le panel et l'overlay
            return (
                {**panel_style, "display": "block"},
                {**overlay_style, "display": "block"},
            )
        else:
            # Fermer le panel et l'overlay
            return (
                {**panel_style, "display": "none"},
                {**overlay_style, "display": "none"},
            )

    elif button_id in [
        "close-column-panel",
        "close-column-panel-x",
        "column-panel-overlay",
    ]:
        # Fermer le panel et l'overlay
        return (
            {**panel_style, "display": "none"},
            {**overlay_style, "display": "none"},
        )

    return panel_style, overlay_style


@callback(
    Output("column-checklist", "value"),
    [
        Input("select-all-columns", "n_clicks"),
        Input("deselect-all-columns", "n_clicks"),
    ],
    prevent_initial_call=True,
)
def handle_column_selection_buttons(select_all_clicks, deselect_all_clicks):
    """Gère les boutons Tout sélectionner / Tout désélectionner"""
    from dash import ctx

    if not ctx.triggered:
        return [col["field"] for col in COLS_DAG if col.get("showDefault", False)]

    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "select-all-columns":
        return [col["field"] for col in COLS_DAG]
    elif button_id == "deselect-all-columns":
        return []

    return [col["field"] for col in COLS_DAG if col.get("showDefault", False)]


@callback(
    Output("get-started-example-basic", "columnDefs"),
    Input("column-checklist", "value"),
    prevent_initial_call=False,  # Permettre l'appel initial
)
def update_grid_columns(selected_columns):
    """Met à jour les colonnes visibles dans la grille selon la sélection"""
    # Colonnes qui doivent toujours rester visibles
    always_visible = ["NUM_IFU", "Actions"]

    # Si selected_columns est None ou vide, utiliser les colonnes par défaut
    if not selected_columns:
        selected_columns = [
            col["field"] for col in COLS_DAG if col.get("showDefault", False)
        ]

    # Ajouter les colonnes toujours visibles si elles ne sont pas déjà sélectionnées
    for always_col in always_visible:
        if always_col not in selected_columns:
            selected_columns.append(always_col)

    # Séparer Actions des autres colonnes sélectionnées pour forcer l'ordre
    selected_without_actions = [col for col in selected_columns if col != "Actions"]
    has_actions = "Actions" in selected_columns

    # Construire la liste des colonnes visibles en respectant l'ordre COLS_DAG
    # mais en excluant temporairement Actions
    visible_cols = []

    # Parcourir COLS_DAG dans l'ordre défini, en excluant Actions
    for col in COLS_DAG:
        if col["field"] in selected_without_actions:
            col_copy = col.copy()
            # Supprimer la propriété hide si elle existe
            col_copy.pop("hide", None)
            visible_cols.append(col_copy)

    # Ajouter Actions à la fin si elle était sélectionnée avec un rendu personnalisé
    if has_actions:
        for col in COLS_DAG:
            if col["field"] == "Actions":
                actions_col = col.copy()
                actions_col.pop("hide", None)

                # Ajouter le rendu personnalisé pour le menu d'actions
                actions_col["cellRenderer"] = {
                    "function": """
                    function(params) {
                        return `
                            <div class="actions-menu">
                                <button class="actions-menu-button" onclick="toggleActionsMenu(event, '${params.data.NUM_IFU}')">
                                    <i class="fas fa-ellipsis-v" style="color: #666;"></i>
                                </button>
                                <div id="menu-${params.data.NUM_IFU}" class="actions-menu-content">
                                    <div class="actions-menu-item" onclick="generateFiche('${params.data.NUM_IFU}')">
                                        <i class="fas fa-file-alt" style="margin-right: 8px; color: #2e7d32;"></i>
                                        Générer une fiche individuelle
                                    </div>
                                </div>
                            </div>
                        `;
                    }
                    """
                }

                # Désactiver l'édition et le tri pour cette colonne
                actions_col["editable"] = False
                actions_col["sortable"] = False
                actions_col["filter"] = False
                actions_col["width"] = 80

                visible_cols.append(actions_col)
                break

    return visible_cols


# Callback pour gérer les actions de la grille (comme générer une fiche individuelle)
@callback(
    Output("grid-actions-output", "children"),
    Input("get-started-example-basic", "cellClicked"),
    prevent_initial_call=True,
)
def handle_grid_actions(cell_clicked):
    """Gère les actions cliquées dans la grille"""
    if not cell_clicked:
        return ""

    # Vérifier si l'action vient de la colonne Actions
    if cell_clicked.get("colId") == "Actions":
        num_ifu = cell_clicked.get("rowData", {}).get("NUM_IFU")
        if num_ifu:
            return html.Div(
                [
                    html.Div(
                        [
                            html.I(
                                className="fas fa-check-circle",
                                style={"color": "#4caf50", "marginRight": "8px"},
                            ),
                            f"Action déclenchée pour l'IFU: {num_ifu}",
                        ],
                        style={
                            "color": "#4caf50",
                            "padding": "10px",
                            "backgroundColor": "#f8f9fa",
                            "border": "1px solid #e9ecef",
                            "borderRadius": "6px",
                            "fontWeight": "500",
                        },
                    )
                ]
            )

    return ""


def generate_individual_fiche(num_ifu):
    """Génère une fiche individuelle pour un contribuable donné"""
    # Cette fonction sera implémentée pour générer la fiche
    # Pour l'instant, on simule l'action
    return f"Fiche individuelle générée pour l'IFU: {num_ifu}"
