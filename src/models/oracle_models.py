
from typing import List, Optional
from datetime import datetime, timezone
from sqlalchemy import ForeignKey, String, DateTime, Table, Column, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db

class Programme(db.Model):
    """Modèle des programmes d'analyse de risque"""
    __tablename__ = "programmes"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    IFU: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    brigade: Mapped[str] = mapped_column(String(200), nullable=False)
    quantume: Mapped[Optional[str]] = mapped_column(String(500))
    actif: Mapped[bool] = mapped_column(default=True, nullable=False)
    date_creation: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    def __repr__(self):
        return f'<Programme {self.IFU}: {self.brigade}>'

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