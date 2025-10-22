"""
Module d'interface utilisateur pour l'analyse des risques fiscaux
Utilise les composants Dash standard pour une compatibilité maximale
"""

from dash import html, dcc
from src.utils import INDICATEUR_LIST_DROPDOWN, LABEL_COLUMNS
import re
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_ag_grid as dag

def create_professional_layout():
    """Crée un layout professionnel avec les composants Dash standard"""
    return html.Div(
        [
            # Contenu principal
            html.Div(
                [
                    # Container avec disposition horizontale pour les deux sections
                    html.Div(
                        [
                            # Section sélecteur d'indicateurs
                            html.Div(
                                [
                                    html.Label(
                                        "Sélectionner les indicateurs",
                                        style={
                                            "fontSize": "1.2rem",
                                            "fontWeight": "500",
                                            "color": "#1a5d1a",
                                            "marginBottom": "12px",
                                            "display": "block",
                                            "letterSpacing": "0.5px",
                                        },
                                    ),
                                    dcc.Dropdown(
                                        options=INDICATEUR_LIST_DROPDOWN,
                                        value=["IND_1"],
                                        clearable=True,
                                        multi=True,
                                        id="indicateur-dropdown",
                                        style={
                                            "width": "100%",
                                            "border": "2px solid #43a047",
                                            "borderRadius": "12px",
                                            "backgroundColor": "#f1f8e9",
                                            "boxShadow": "0 2px 8px rgba(67, 160, 71, 0.1)",
                                        },
                                    ),
                                ],
                                style={
                                    "flex": "2",
                                    "marginRight": "20px",
                                    "padding": "25px",
                                    "backgroundColor": "#ffffff",
                                    "borderRadius": "15px",
                                    "boxShadow": "0 4px 12px rgba(0,0,0,0.05)",
                                    "border": "1px solid #e8f5e9",
                                },
                            ),
                            # Section sélecteur de période
                            html.Div(
                                [
                                    html.Label(
                                        "Sélectionner la période",
                                        style={
                                            "fontSize": "1.2rem",
                                            "fontWeight": "500",
                                            "color": "#1a5d1a",
                                            "marginBottom": "12px",
                                            "display": "block",
                                            "letterSpacing": "0.5px",
                                        },
                                    ),
                                    dcc.DatePickerRange(
                                        id="date-picker-range",
                                        start_date="2023-01-01",
                                        end_date="2025-12-31",
                                        display_format="MM/YYYY",
                                        style={
                                            "width": "100%",
                                            "border": "2px solid #43a047",
                                            "borderRadius": "12px",
                                            "backgroundColor": "#f1f8e9",
                                            "boxShadow": "0 2px 8px rgba(67, 160, 71, 0.1)",
                                        },
                                        calendar_orientation="horizontal",
                                        first_day_of_week=1,  # Lundi comme premier jour
                                    ),
                                ],
                                style={
                                    "flex": "1",
                                    "padding": "25px",
                                    "backgroundColor": "#ffffff",
                                    "borderRadius": "15px",
                                    "boxShadow": "0 4px 12px rgba(0,0,0,0.05)",
                                    "border": "1px solid #e8f5e9",
                                },
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        "Analyser",
                                        id="calculate-indicators-button",
                                        n_clicks=0,
                                        style={
                                            "width": "100%",
                                            "maxWidth": "250px",
                                            "maxHeight": "45px",
                                            "background": "linear-gradient(45deg, #2e7d32 30%, #43a047 90%)",
                                            "color": "white",
                                            "padding": "5px 5px",
                                            "borderRadius": "5px",
                                            "border": "none",
                                            "boxShadow": "0 4px 12px rgba(67, 160, 71, 0.3)",
                                            "fontSize": "1.1rem",
                                            "fontWeight": "500",
                                            "cursor": "pointer",
                                            "transition": "all 0.3s ease",
                                            ":hover": {
                                                "transform": "translateY(-2px)",
                                                "boxShadow": "0 6px 15px rgba(67, 160, 71, 0.4)",
                                            },
                                        },
                                    ),
                                ],
                                style={
                                    "display": "flex",
                                    "justifyContent": "center",
                                    "marginTop": "20px",
                                },
                            ),
                        ],
                        style={
                            "display": "flex",
                            "gap": "0px",
                            "marginBottom": "35px",
                            "alignItems": "stretch",  # Pour que les deux boxes aient la même hauteur
                        },
                    ),
                ],
                style={
                    "margin": "0 auto",
                    "padding": "0 20px",
                },
            ),
            # Section des résultats
            html.Div(
                id="results-container",
                style={
                    "padding": "5px",
                    # "maxWidth": "1200px",
                    "margin": "40px auto",
                },
            ),
        ]
    )


def render_table(data, selected_indicators=None):
    """Rend un tableau dynamique.
    Pour chaque indicateur sélectionné (IND_X, IND_X_A, etc.), inclut automatiquement:
      - RISQUE_IND_X[_A]
      - GAP_IND_X[_A] (si existe)
      - SCORE_IND_X[_A] (si existe)
    Fallback: si aucun indicateur sélectionné, toutes les colonnes RISQUE/GAP/SCORE disponibles sont affichées triées.
    """
    # Colonnes de base hors indicateurs
    base_columns_only = [
        "NUM_IFU",
        "ANNEE",
        "CODE_STRUCTURE",
        "LIBELLE_STRUCTURE",
        "RAISON_SOCIALE",
        "PERIODE_FISCALE",
        "ETAT",
        "REGIME_FISCALE",
        "DATE_DEBUT_ACTIVITE",
        "SECTEUR_ACTIVITE",
        "TYPE_CONTROLE",
        "FORME_JURIDIQUE",
        "HIERARCHIE_2",
        "HIERARCHIE_3",
    ]

    def normalize_selection(val: str):
        if not isinstance(val, str):
            return None
        s = val.strip().upper()
        # Déjà une colonne complète
        if s in data.columns:
            return s
        # Cherche motif IND_12, IND-12A, INDICATEUR 12 B, etc.
        m = re.search(r"IND(?:ICATEUR)?[\s_-]*(\d+)([A-Z])?", s)
        if m:
            num = int(m.group(1))
            suffix = f"_{m.group(2)}" if m.group(2) else ""
            return f"RISQUE_IND_{num}{suffix}"
        # Si motif RISQUE_IND directement
        m2 = re.search(r"RISQUE_IND_[0-9]+(?:_[A-Z])?", s)
        if m2:
            return m2.group(0)
        return None

    def indicator_sort_key(col: str):
        # Extrait numéro et suffixe pour un tri naturel
        m = re.search(r"RISQUE_IND_(\d+)(?:_([A-Z]))?", col)
        if m:
            num = int(m.group(1))
            suf = m.group(2) or ""
            # Assure A avant B dans tri
            return (num, suf)
        return (9999, "")

    if selected_indicators:
        ordered_risk_cols = []
        seen = set()
        for raw in selected_indicators:
            base_risk = normalize_selection(raw)
            if base_risk and base_risk in data.columns and base_risk not in seen:
                seen.add(base_risk)
                ordered_risk_cols.append(base_risk)
        # Tri selon num + suffixe mais en conservant ordre sélection si besoin ? Demandé: juste affichage => on applique tri naturel
        ordered_risk_cols = sorted(ordered_risk_cols, key=indicator_sort_key)
        # Pour chaque risk col, ajouter GAP / SCORE associés si présents
        indicator_cols = []
        for risk_col in ordered_risk_cols:
            indicator_cols.append(risk_col)
            gap_col = risk_col.replace("RISQUE_", "GAP_")
            score_col = risk_col.replace("RISQUE_", "SCORE_")
            # Ajout dans l'ordre RISQUE, GAP, SCORE si existent
            if gap_col in data.columns:
                indicator_cols.append(gap_col)
            if score_col in data.columns:
                indicator_cols.append(score_col)
            # Cas particulier: IND_3 (AGE_MOIS_IND_3) – si lié et existant on peut l'ajouter après GAP
            if risk_col == "RISQUE_IND_3" and "AGE_MOIS_IND_3" in data.columns:
                indicator_cols.append("AGE_MOIS_IND_3")
    else:
        # Fallback : récupérer tous les RISQUE puis leurs GAP / SCORE associés
        all_risques = [
            c for c in data.columns if re.match(r"RISQUE_IND_\d+(?:_[A-Z])?", c)
        ]
        all_risques = sorted(all_risques, key=indicator_sort_key)
        indicator_cols = []
        for risk_col in all_risques:
            indicator_cols.append(risk_col)
            gap_col = risk_col.replace("RISQUE_", "GAP_")
            score_col = risk_col.replace("RISQUE_", "SCORE_")
            if gap_col in data.columns:
                indicator_cols.append(gap_col)
            if score_col in data.columns:
                indicator_cols.append(score_col)
            if risk_col == "RISQUE_IND_3" and "AGE_MOIS_IND_3" in data.columns:
                indicator_cols.append("AGE_MOIS_IND_3")

    # Colonnes finales
    columns = base_columns_only + indicator_cols + ["Actions"]

    # Listes utilitaires
    risk_cols = [c for c in indicator_cols if c.startswith("RISQUE_IND_")]
    gap_cols = [c for c in indicator_cols if c.startswith("GAP_IND_")]
    score_cols = [c for c in indicator_cols if c.startswith("SCORE_IND_")]

    def badge(value: str, palette):
        value_str = (str(value) if value is not None else "Non disponible").strip()
        val_lower = value_str.lower()
        theme = palette.get(val_lower, palette["non disponible"])
        return html.Span(
            value_str.upper(),
            style={
                "display": "inline-block",
                "padding": "4px 10px",
                "borderRadius": "999px",
                "backgroundColor": theme["bg"],
                "color": theme["fg"],
                "border": f"1px solid {theme['bd']}",
                "fontSize": "0.7rem",
                "fontWeight": 600,
                "letterSpacing": "0.5px",
                "minWidth": "78px",
                "textAlign": "center",
            },
            title=f"{value_str}",
        )

    risk_palette = {
        "rouge": {"bg": "#ef9a9a", "fg": "#b71c1c", "bd": "#e57373"},
        "orange": {"bg": "#ffcc80", "fg": "#e65100", "bd": "#ffb74d"},
        "jaune": {"bg": "#fff59d", "fg": "#8d6e63", "bd": "#fff176"},
        "vert": {"bg": "#a5d6a7", "fg": "#1b5e20", "bd": "#81c784"},
        "non disponible": {"bg": "#e0e0e0", "fg": "#616161", "bd": "#bdbdbd"},
    }

    # Largeurs automatiques
    col_widths = {
        "NUM_IFU": "110px",
        "ANNEE": "80px",
        "CODE_STRUCTURE": "130px",
        "LIBELLE_STRUCTURE": "180px",
        "RAISON_SOCIALE": "200px",
        "PERIODE_FISCALE": "120px",
        "ETAT": "100px",
        "REGIME_FISCALE": "130px",
        "DATE_DEBUT_ACTIVITE": "140px",
        "SECTEUR_ACTIVITE": "160px",
        "TYPE_CONTROLE": "140px",
        "FORME_JURIDIQUE": "140px",
        "HIERARCHIE_2": "140px",
        "HIERARCHIE_3": "140px",
        "AGE_MOIS_IND_3": "110px",
        "Actions": "110px",
    }
    for rc in indicator_cols:
        if rc.startswith(("RISQUE_IND_", "GAP_IND_", "SCORE_IND_")):
            col_widths.setdefault(rc, "115px")

    header_cells = [
        html.Th(
            LABEL_COLUMNS.get(col, col),
            style={
                "position": "sticky",
                "top": 0,
                "zIndex": 2,
                "padding": "10px 12px",
                "background": "linear-gradient(0deg, #2e7d32 0%, #388e3c 100%)",
                "color": "white",
                "fontWeight": 700,
                "textAlign": "left" if col != "Actions" else "center",
                "fontSize": "0.75rem",
                "borderBottom": "2px solid #1b5e20",
                "textTransform": "uppercase",
                "letterSpacing": "0.5px",
                "whiteSpace": "nowrap",
                "width": col_widths.get(col, "120px"),
                "minWidth": col_widths.get(col, "120px"),
            },
        )
        for col in columns
    ]

    def cell_style(col: str, idx: int):
        base = {
            "padding": "8px 10px",
            "borderBottom": "1px solid #e8f5e9",
            "fontSize": "0.72rem",
            "color": "#2e7d32",
            "backgroundColor": "#ffffff" if idx % 2 == 0 else "#f9fbf9",
            "verticalAlign": "middle",
            "maxWidth": col_widths.get(col, "200px"),
            "minWidth": col_widths.get(col, "100px"),
            "overflow": "hidden",
            "textOverflow": "ellipsis",
            "whiteSpace": "nowrap",
            "textAlign": "center"
            if col
            in (risk_cols + gap_cols + score_cols + ["Actions", "AGE_MOIS_IND_3"])
            else "left",
        }
        if col == "NUM_IFU":
            base.update(
                {
                    "position": "sticky",
                    "left": 0,
                    "zIndex": 1,
                    "backgroundColor": base["backgroundColor"],
                    "boxShadow": "2px 0 0 rgba(0,0,0,0.04)",
                    "fontWeight": 600,
                }
            )
        return base

    body_rows = []
    for idx, row in data.iterrows():
        tds = []
        for col in columns:
            if col == "Actions":
                content = html.Button(
                    "Voir plus",
                    id={"type": "view-details-btn", "index": idx},
                    title="Voir les détails de l'entreprise",
                    style={
                        "background": "linear-gradient(45deg, #43a047 0%, #66bb6a 100%)",
                        "color": "white",
                        "border": "none",
                        "padding": "5px 10px",
                        "borderRadius": "18px",
                        "fontSize": "0.65rem",
                        "fontWeight": 600,
                        "cursor": "pointer",
                        "boxShadow": "0 2px 6px rgba(67,160,71,0.3)",
                    },
                )
            elif col in risk_cols:
                content = badge(row.get(col, "Non disponible"), risk_palette)
            elif col in gap_cols:
                content = html.Span(
                    str(row.get(col, "")), style={"fontWeight": 600, "color": "#5d4037"}
                )
            elif col in score_cols:
                content = html.Span(
                    str(row.get(col, "")), style={"fontWeight": 600, "color": "#1b5e20"}
                )
            else:
                content = html.Span(str(row.get(col, "")))
            tds.append(html.Td(content, style=cell_style(col, idx)))
        body_rows.append(html.Tr(tds))

    table = html.Table(
        [html.Thead([html.Tr(header_cells)]), html.Tbody(body_rows)],
        style={
            "width": "100%",
            "borderCollapse": "separate",
            "borderSpacing": 0,
            "backgroundColor": "#ffffff",
            "fontFamily": "'Helvetica Neue', Arial, sans-serif",
        },
    )

    return html.Div(
        table,
        style={
            "width": "95%",
            "margin": "20px auto",
            "borderRadius": "12px",
            "overflow": "auto",
            "maxHeight": "68vh",
            "boxShadow": "0 6px 18px rgba(0,0,0,0.12)",
            "border": "1px solid #e8f5e9",
        },
    )


