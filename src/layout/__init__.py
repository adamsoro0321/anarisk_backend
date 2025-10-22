"""
Package des composants de layout/interface utilisateur
Modules organisés pour l'interface Dash
"""

from .ui_components import create_professional_layout, render_table
from .header import top_bar
from .footer import footer

__all__ = [
    "create_professional_layout",
    "top_bar",
    "footer",
]
