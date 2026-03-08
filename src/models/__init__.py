"""
Module des modèles de données pour ANARISK
Direction Générale des Impôts - Burkina Faso
"""

from .model import (
    User,
    Role,
    Permission,
    Ressource,
    Indicateur,
    user_roles,
    role_permissions,
    permission_ressources
)

__all__ = [
    'User',
    'Role',
    'Permission',
    'Ressource',
    'Indicateur',
    'user_roles',
    'role_permissions',
    'permission_ressources'
]
