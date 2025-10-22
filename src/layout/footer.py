from dash import html


def footer():
    return (
        html.Div(
            [
                html.Hr(
                    style={
                        "margin": "40px auto",
                        "width": "80%",
                        "border": "none",
                        "height": "1px",
                        "background": "linear-gradient(90deg, transparent, #4caf50, transparent)",
                    }
                ),
                html.P(
                    "© 2025 Direction Générale des Impôts - Système d'Analyse des Risques",
                    style={
                        "textAlign": "center",
                        "color": "#2e582e",
                        "fontSize": "1rem",
                        "fontWeight": "300",
                        "letterSpacing": "0.5px",
                    },
                ),
            ],
            style={
                "padding": "30px",
                "backgroundColor": "#f8faf8",
                "marginTop": "40px",
            },
        ),
    )
