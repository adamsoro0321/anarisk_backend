#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Dash pour le Dashboard DGI
"""

# Configuration des couleurs et thèmes
DASH_CONFIG = {
    'theme': {
        'primary': '#1e3a8a',      # Bleu DGI
        'secondary': '#059669',     # Vert
        'accent': '#dc2626',        # Rouge
        'warning': '#d97706',       # Orange
        'success': '#16a34a',       # Vert succès
        'background': '#f8fafc',    # Gris clair
        'white': '#ffffff',
        'text': '#1f2937',
        'border': '#e5e7eb'
    },
    
    'layout': {
        'sidebar_width': '250px',
        'header_height': '80px',
        'card_padding': '20px',
        'border_radius': '10px'
    },
    
    'graphs': {
        'default_height': 400,
        'default_margin': {'t': 50, 'l': 50, 'r': 50, 'b': 50},
        'font_family': 'Arial, sans-serif',
        'font_size': 12
    },
    
    'data': {
        'page_size': 10,
        'refresh_interval': 300000,  # 5 minutes en millisecondes
        'cache_duration': 3600       # 1 heure en secondes
    }
}

# Mapping des types d'impôts avec des couleurs
IMPOT_COLORS = {
    'TVA': '#1e3a8a',
    'IUTS': '#059669',
    'IS': '#dc2626',
    'BIC': '#d97706',
    'Pénalités': '#ef4444',
    'RET/SALAIRES': '#8b5cf6',
    'CNSS': '#06b6d4',
    'Droits divers': '#84cc16'
}

# Configuration des métriques
METRICS_CONFIG = {
    'total_recettes': {
        'icon': 'fa-coins',
        'color': '#1e3a8a',
        'format': 'currency'
    },
    'nb_impots': {
        'icon': 'fa-list-alt',
        'color': '#059669',
        'format': 'number'
    },
    'moyenne_mensuelle': {
        'icon': 'fa-calendar-alt',
        'color': '#d97706',
        'format': 'currency'
    },
    'evolution': {
        'icon': 'fa-arrow-up',
        'color': '#16a34a',
        'format': 'percentage'
    }
}

# Messages et textes
MESSAGES = {
    'no_data': 'Aucune donnée disponible pour cette sélection',
    'loading': 'Chargement des données...',
    'error': 'Erreur lors du chargement des données',
    'db_connected': 'Base de données connectée',
    'demo_mode': 'Mode démonstration',
    'last_update': 'Dernière mise à jour',
    'refresh': 'Actualiser'
}

# Configuration des graphiques spécifiques
CHART_CONFIGS = {
    'evolution': {
        'type': 'line',
        'title': 'Évolution des Recettes',
        'x_axis': 'Mois',
        'y_axis': 'Montant (FCFA)',
        'height': 400
    },
    'repartition': {
        'type': 'pie',
        'title': 'Répartition par Type d\'Impôt',
        'height': 400
    },
    'comparison': {
        'type': 'bar',
        'title': 'Comparaison Annuelle',
        'x_axis': 'Mois',
        'y_axis': 'Montant (FCFA)',
        'height': 400
    },
    'top_impots': {
        'type': 'horizontal_bar',
        'title': 'Top 5 Types d\'Impôts',
        'x_axis': 'Montant (FCFA)',
        'y_axis': 'Type d\'Impôt',
        'height': 400
    }
}

# Export de la configuration
__all__ = [
    'DASH_CONFIG',
    'IMPOT_COLORS', 
    'METRICS_CONFIG',
    'MESSAGES',
    'CHART_CONFIGS'
]
