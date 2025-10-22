from dash import html, dcc
import dash


def get_page_icon(page_name):
    """Retourne l'icône FontAwesome appropriée selon le nom de la page"""
    page_name_lower = page_name.lower()

    if (
        "home" in page_name_lower
        or "accueil" in page_name_lower
        or "index" in page_name_lower
    ):
        return "fas fa-home"
    elif (
        "contribuable" in page_name_lower
        or "taxpayer" in page_name_lower
        or "client" in page_name_lower
    ):
        return "fas fa-users"
    elif (
        "indicateur" in page_name_lower
        or "indicator" in page_name_lower
        or "metric" in page_name_lower
    ):
        return "fas fa-chart-line"
    elif "dashboard" in page_name_lower or "tableau" in page_name_lower:
        return "fas fa-tachometer-alt"
    elif "report" in page_name_lower or "rapport" in page_name_lower:
        return "fas fa-file-alt"
    elif "setting" in page_name_lower or "config" in page_name_lower:
        return "fas fa-cog"
    elif "data" in page_name_lower or "donnee" in page_name_lower:
        return "fas fa-database"
    else:
        return "fas fa-circle"  # Icône par défaut


def get_clean_page_name(page_name):
    """Nettoie et formate le nom de la page pour l'affichage"""
    # Supprime les underscores et tire-bas, capitalise
    clean_name = page_name.replace("_", " ").replace("-", " ")

    # Capitalise chaque mot
    clean_name = " ".join(word.capitalize() for word in clean_name.split())

    # Remplacements spécifiques pour le français
    replacements = {
        "Home": "Accueil",
        "Index": "Accueil",
        "Dashboard": "Tableau de Bord",
        "Taxpayer": "Contribuable",
        "Indicator": "Indicateur",
        "Report": "Rapport",
        "Setting": "Paramètres",
        "Config": "Configuration",
        "Data": "Données",
    }

    for en, fr in replacements.items():
        clean_name = clean_name.replace(en, fr)

    return clean_name


def is_current_page(page):
    """Détermine si c'est la page courante (pour l'instant, retourne False)"""
    # Cette fonction pourrait être améliorée avec la logique de détection de page courante
    # Pour l'instant, on considère la première page comme active
    return False


def get_active_style():
    """Retourne le style pour la page active"""
    return {
        "style": {
            "backgroundColor": "#2e7d32",
            "color": "#ffffff",
            "border": "2px solid #1b5e20",
            "boxShadow": "0 3px 8px rgba(46, 125, 50, 0.4)",
            "transform": "translateY(-1px)",
        }
    }


def top_bar():
    return html.Div(
        [
            # Header principal avec logo et titre
            html.Div(
                [
                    # Section logo
                    html.Div(
                        [
                            html.Img(
                                src="../../assets/dgi-logo.png",
                                height="90px",
                                style={
                                    "filter": "drop-shadow(0 2px 4px rgba(0,0,0,0.1))",
                                    "borderRadius": "8px",
                                },
                            )
                        ],
                        style={
                            "flex": "0 0 auto",
                            "display": "flex",
                            "alignItems": "center",
                            "marginRight": "30px",
                        },
                    ),
                    # Section titre principale
                    html.Div(
                        [
                            html.H1(
                                "DGI:ANARISQ",
                                style={
                                    "color": "#1a5d1a",
                                    "margin": "0",
                                    "fontSize": "2.5rem",
                                    "fontFamily": "'Segoe UI', 'Helvetica Neue', Arial, sans-serif",
                                    "fontWeight": "700",
                                    "letterSpacing": "0.5px",
                                    "textShadow": "1px 1px 3px rgba(0,0,0,0.1)",
                                    "lineHeight": "1.2",
                                },
                            ),
                        ],
                        style={
                            "flex": "1",
                            "display": "flex",
                            "flexDirection": "column",
                            "justifyContent": "center",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "padding": "25px 40px",
                    "background": "linear-gradient(135deg, #f1f8e9 0%, #e8f5e9 50%, #c8e6c9 100%)",
                    "borderRadius": "0 0 20px 20px",
                    "boxShadow": "0 4px 20px rgba(0,0,0,0.08)",
                    "position": "relative",
                    "overflow": "hidden",
                },
            ),
            # Menu de navigation élégant avec pages dynamiques
            html.Div(
                [
                    html.Nav(
                        [
                            html.Ul(
                                [
                                    html.Li(
                                        dcc.Link(
                                            [
                                                # Icône dynamique selon le nom de la page
                                                html.I(
                                                    className=get_page_icon(
                                                        page["name"]
                                                    ),
                                                    style={
                                                        "marginRight": "10px",
                                                        "fontSize": "1.1rem",
                                                    },
                                                ),
                                                # Nom de la page nettoyé
                                                get_clean_page_name(page["name"]),
                                            ],
                                            href=page["relative_path"],
                                            style={
                                                "display": "flex",
                                                "alignItems": "left",
                                                "padding": "14px 28px",
                                                "color": "#2e7d32",
                                                "textDecoration": "none",
                                                "borderRadius": "10px",
                                                "transition": "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
                                                "fontWeight": "500",
                                                "fontSize": "1rem",
                                                "backgroundColor": "transparent",
                                                "border": "2px solid transparent",
                                                "position": "relative",
                                                "overflow": "hidden",
                                            },
                                            className="nav-page-link",
                                        ),
                                        style={
                                            "listStyle": "none",
                                            "margin": "0 8px",
                                            "position": "relative",
                                        },
                                    )
                                    for page in dash.page_registry.values()
                                ],
                                style={
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "flex-start",
                                    "flexWrap": "wrap",
                                    "margin": "0",
                                    "padding": "0",
                                    "gap": "5px",
                                },
                            ),
                        ],
                        style={
                            "width": "100%",
                            "maxWidth": "1200px",
                            "margin": "0",
                            "paddingLeft": "0",
                        },
                    ),
                ],
                style={
                    "backgroundColor": "#f8fffe",
                    "background": "linear-gradient(135deg, #f8fffe 0%, #f1f8f1 100%)",
                    "padding": "20px 40px",
                    "borderTop": "1px solid #e8f5e8",
                    "borderBottom": "1px solid #e0e0e0",
                    "boxShadow": "inset 0 1px 0 rgba(255,255,255,0.9), 0 1px 3px rgba(0,0,0,0.05)",
                    "position": "relative",
                },
            ),
            html.Div(
                style={
                    "height": "4px",
                    "background": "linear-gradient(90deg, #1b5e20 0%, #2e7d32 50%, #4caf50 100%)",
                    "boxShadow": "0 2px 4px rgba(46, 125, 50, 0.3)",
                }
            ),
            dash.page_container,
        ],
        style={
            "backgroundColor": "#ffffff",
            "marginBottom": "20px",
            "boxShadow": "0 2px 10px rgba(0,0,0,0.05)",
        },
    )
