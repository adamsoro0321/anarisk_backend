from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

Base = declarative_base()

class User(UserMixin, Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    nom = Column(String(100))
    prenom = Column(String(100))
    mail = Column(String(120), unique=True, nullable=False)
    role = Column(String(50), default='user') # e.g., 'admin', 'user'
    status = Column(String(50), default='active') # e.g., 'active', 'inactive'
    password_hash = Column(String(256))

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.mail}>'
