from typing import List, Optional
from datetime import datetime, timezone
from sqlalchemy import ForeignKey, String, DateTime, Table, Column, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db

# Liste des brigades autorisées
BRIGADES_AUTORISEES = [
    'BV1-DME-C3',
    'BV2-DME-C4',
    'BV3-DME-C4',
    'BV4-DME-C4',
    'BV-OUAGA2',
    'BV-OUAGA3',
    'BV-OUAGA4',
    'BV-OUAGA8',
    'BV-OUAGA9',
    'BV-DRI-CS',
    'BV-DRI-CSC',
    'BV-BOBO2',
    'BV-DRI-SO',
    'BV2-DME-CI',
    'BV-OUAGAVII',
]

# Tables d'association pour les relations many-to-many
user_roles = Table(
    'user_roles',
    db.Model.metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id', ondelete='CASCADE'), primary_key=True)
)

role_permissions = Table(
    'role_permissions',
    db.Model.metadata,
    Column('role_id', Integer, ForeignKey('roles.id', ondelete='CASCADE'), primary_key=True),
    Column('permission_id', Integer, ForeignKey('permissions.id', ondelete='CASCADE'), primary_key=True)
)

permission_ressources = Table(
    'permission_ressources',
    db.Model.metadata,
    Column('permission_id', Integer, ForeignKey('permissions.id', ondelete='CASCADE'), primary_key=True),
    Column('ressource_id', Integer, ForeignKey('ressources.id', ondelete='CASCADE'), primary_key=True)
)


class User(db.Model):
    """Modèle utilisateur pour l'authentification et l'autorisation"""
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    nom: Mapped[str] = mapped_column(String(50), nullable=False)
    prenom: Mapped[str] = mapped_column(String(100), nullable=False)
    password: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), default='active', nullable=False)
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    date_modification: Mapped[Optional[datetime]] = mapped_column(DateTime, onupdate=lambda: datetime.now(timezone.utc))
    
    #role = Mapped[Optional[str]] = mapped_column(String(50))  # Rôle principal (ex: 'admin', 'analyste', etc.)
    # Relations
    roles: Mapped[List["Role"]] = relationship(
        secondary=user_roles,
        back_populates="users"
    )

    def set_password(self, password: str):
        """Hashage du mot de passe"""
        self.password = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        """Vérification du mot de passe"""
        return check_password_hash(self.password, password)

    def has_role(self, role_name: str) -> bool:
        """Vérifie si l'utilisateur a un rôle spécifique"""
        return any(role.intitule == role_name for role in self.roles)

    def has_permission(self, permission_name: str) -> bool:
        """Vérifie si l'utilisateur a une permission spécifique via ses rôles"""
        for role in self.roles:
            if any(perm.intitule == permission_name for perm in role.permissions):
                return True
        return False

    def __repr__(self):
        return f'<User {self.email}>'


class Role(db.Model):
    """Modèle des rôles (admin, analyste, contrôleur, etc.)"""
    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    intitule: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(String(255))
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relations
    users: Mapped[List["User"]] = relationship(
        secondary=user_roles,
        back_populates="roles"
    )
    permissions: Mapped[List["Permission"]] = relationship(
        secondary=role_permissions,
        back_populates="roles"
    )

    def __repr__(self):
        return f'<Role {self.intitule}>'


class Permission(db.Model):
    """Modèle des permissions (lecture, écriture, suppression, etc.)"""
    __tablename__ = "permissions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    intitule: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(String(255))
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relations
    roles: Mapped[List["Role"]] = relationship(
        secondary=role_permissions,
        back_populates="permissions"
    )
    ressources: Mapped[List["Ressource"]] = relationship(
        secondary=permission_ressources,
        back_populates="permissions"
    )

    def __repr__(self):
        return f'<Permission {self.intitule}>'


class Ressource(db.Model):
    """Modèle des ressources protégées (endpoints, données, etc.)"""
    __tablename__ = "ressources"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    intitule: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'api', 'data', 'page'
    description: Mapped[Optional[str]] = mapped_column(String(255))
    endpoint: Mapped[Optional[str]] = mapped_column(String(255))  # Pour les APIs
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    
    # Relations
    permissions: Mapped[List["Permission"]] = relationship(
        secondary=permission_ressources,
        back_populates="ressources"
    )

    def __repr__(self):
        return f'<Ressource {self.intitule}>'


class Indicateur(db.Model):
    """Modèle des indicateurs de risque"""
    __tablename__ = "indicateurs"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    intitule: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(500))
    categorie: Mapped[str] = mapped_column(String(50), nullable=False)  # 'TVA', 'CME', 'RSI', etc.
    formule: Mapped[Optional[str]] = mapped_column(String(500))  # Formule de calcul
    seuil_alerte: Mapped[Optional[float]] = mapped_column()
    poids: Mapped[float] = mapped_column(default=1.0, nullable=False)  # Poids dans le calcul du risque global
    actif: Mapped[bool] = mapped_column(default=True, nullable=False)
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    date_modification: Mapped[Optional[datetime]] = mapped_column(DateTime, onupdate=lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return f'<Indicateur {self.code}: {self.intitule}>' 

class Programme(db.Model):
    """Modèle des programmes d'analyse de risque"""
    __tablename__ = "programmes"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    IFU: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    actif: Mapped[bool] = mapped_column(default=True, nullable=False)
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    id_brigade: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('brigades.id', ondelete='SET NULL'))
    id_quantume: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('quantumes.id', ondelete='SET NULL'))

    # Relations
    brigade: Mapped[Optional["Brigade"]] = relationship("Brigade", lazy='joined')
    quantume: Mapped[Optional["Quantume"]] = relationship("Quantume", lazy='joined')

    def __repr__(self):
        return f'<Programme {self.IFU}>'

class Quantume(db.Model):
    """Modèle des quantumes d'analyse de risque"""
    __tablename__ = "quantumes"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    libelle: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    def __repr__(self):
        return f'<Quantume {self.libelle}>'

class Brigade(db.Model):
    """Modèle des brigades d'analyse de risque"""
    __tablename__ = "brigades"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    libelle: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    def __repr__(self):
        return f'<Brigade {self.libelle}>'