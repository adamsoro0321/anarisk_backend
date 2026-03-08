
from models.model import User, Role, Permission, Ressource, Indicateur, Brigade, BRIGADES_AUTORISEES,Quantume
from extensions import db
from app import app
from api_utils.utils import hash_password
ressources_data = [
        # APIs
        {"intitule": "api_contribuables", "type": "api", "endpoint": "/api/contribuables", "description": "API de gestion des contribuables"},
        {"intitule": "api_risques", "type": "api", "endpoint": "/api/risques", "description": "API d'analyse des risques"},
        {"intitule": "api_programmes", "type": "api", "endpoint": "/api/programmes", "description": "API de génération des programmes"},
        {"intitule": "api_indicateurs", "type": "api", "endpoint": "/api/indicateurs", "description": "API de gestion des indicateurs"},
        {"intitule": "api_users", "type": "api", "endpoint": "/api/users", "description": "API de gestion des utilisateurs"},

        # Pages
        {"intitule": "page_dashboard", "type": "page", "endpoint": "/dashboard", "description": "Page tableau de bord"},
        {"intitule": "page_analyse", "type": "page", "endpoint": "/analyse", "description": "Page d'analyse des risques"},
        {"intitule": "page_programmes", "type": "page", "endpoint": "/programmes", "description": "Page des programmes de contrôle"},
        {"intitule": "page_admin", "type": "page", "endpoint": "/admin", "description": "Page d'administration"},

        # Données
        {"intitule": "data_tva", "type": "data", "description": "Données TVA"},
        {"intitule": "data_dgd", "type": "data", "description": "Données douanières (DGD)"},
        {"intitule": "data_benefices", "type": "data", "description": "Données bénéfices"},
    ]
permissions_data = [
        # Permissions CRUD de base
        {"intitule": "read_data", "description": "Lecture des données"},
        {"intitule": "write_data", "description": "Écriture des données"},
        {"intitule": "delete_data", "description": "Suppression des données"},
        {"intitule": "export_data", "description": "Export des données"},

        # Permissions spécifiques
        {"intitule": "manage_users", "description": "Gestion des utilisateurs"},
        {"intitule": "manage_roles", "description": "Gestion des rôles"},
        {"intitule": "view_dashboard", "description": "Voir le tableau de bord"},
        {"intitule": "analyze_risk", "description": "Analyser les risques"},
        {"intitule": "generate_programme", "description": "Générer des programmes de contrôle"},
        {"intitule": "validate_control", "description": "Valider un contrôle"},
        {"intitule": "access_admin", "description": "Accès à l'administration"},
    ]
roles_dict = {
        "admin": ["read_data", "write_data", "delete_data", "export_data", "manage_users", "manage_roles", "view_dashboard", "analyze_risk", "generate_programme", "validate_control", "access_admin"],
        "dcf": ["read_data", "analyze_risk", "view_dashboard"],
        "ur": ["read_data", "validate_control"],
        "agent": ["read_data", "view_dashboard", "validate_control"],
        "bv": ["read_data"]
        
    }
users_data = [
    {
            "nom": "SORO",
            "prenom": "Adama",
            "email": "adama.soro@dgi.bf",
            "password": "Admin@2024",
            "roles":"admin",
            "status": "active"
        },
        {
            "nom": "TRAORE",
            "prenom": "Amadou",
            "email": "amadou.traore@dgi.bf",
            "password": "Admin@2024",
            "roles":"admin",
            "status": "active"
        },
        {
            "nom": "KABORE",
            "prenom": "Marie",
            "email": "marie.kabore@dgi.bf",
            "password": "Analyste@2024",
            "roles":"dcf",
            "status": "active"
        },
        {
            "nom": "OUEDRAOGO",
            "prenom": "Jean",
            "email": "jean.ouedraogo@dgi.bf",
            "password": "Control@2024",
            "roles": "ur",
            "status": "active"
        },
        {
            "nom": "SAWADOGO",
            "prenom": "Fatou",
            "email": "fatou.sawadogo@dgi.bf",
            "password": "Superv@2024",
            "roles": "agent",
            "status": "active"
        },
        {
            "nom": "COMPAORE",
            "prenom": "Alain",
            "email": "alain.compaore@dgi.bf",
            "password": "Consult@2024",
            "roles": "bv",
            "status": "active"
        },
    ]

quantum_data = [
    "Q1_2026","Q2_2026", "Q3_2026", "Q4_2026",
    "Q1_27","Q3_2027"
]




if __name__ == "__main__":
    """
    Exécuter ce script directement pour seed la base de données
    
    Usage:
        python seed.py                 # Créer les données
        python seed.py --clear         # Supprimer toutes les données
        python seed.py --reset         # Supprimer puis recréer toutes les données
    """
    import sys
    with app.app_context():
        print("\n🔧 Insertion des ressources...")
        
        # 1. Insérer les ressources
        ressources_inserted = 0
        for data in ressources_data:
            ressource = Ressource.query.filter_by(intitule=data["intitule"]).first()
            if not ressource:
                ressource = Ressource(**data)
                db.session.add(ressource)
                ressources_inserted += 1
                print(f"  ✓ Ressource créée: {data['intitule']}")
            else:
                print(f"  ⚠ Ressource existe déjà: {data['intitule']}")
        
        # Commit les 
        db.session.commit()
        print(f"\n✅ {ressources_inserted} ressources insérées sur {len(ressources_data)}")
        
        # 1b. Insérer les brigades autorisées
        print("\n🔧 Insertion des brigades...")
        brigades_inserted = 0
        for brigade_code in BRIGADES_AUTORISEES:
            brigade = Brigade.query.filter_by(libelle=brigade_code).first()
            if not brigade:
                brigade = Brigade(libelle=brigade_code)
                db.session.add(brigade)
                brigades_inserted += 1
                print(f"  ✓ Brigade créée: {brigade_code}")
            else:
                print(f"  ⚠ Brigade existe déjà: {brigade_code}")
        
        # Commit les brigades
        db.session.commit()
        print(f"\n✅ {brigades_inserted} brigades insérées sur {len(BRIGADES_AUTORISEES)}")
        
        # 2. Insérer les permissions
        permissions_inserted = 0
        for data in permissions_data:
            permission = Permission.query.filter_by(intitule=data["intitule"]).first()
            if not permission:
                permission = Permission(**data)
                db.session.add(permission)
                permissions_inserted += 1
                print(f"  ✓ Permission créée: {data['intitule']}")
            else:
                print(f"  ⚠ Permission existe déjà: {data['intitule']}")
        
        # Commit les permissions
        db.session.commit()
        print(f"\n✅ {permissions_inserted} permissions insérées sur {len(permissions_data)}")
        
        #3. Insérer les rôles
        roles_inserted = 0
        for role_name, perms in roles_dict.items():
            role = Role.query.filter_by(intitule=role_name).first()
            if not role:
                role = Role(intitule=role_name, description=f"Rôle {role_name}")
                db.session.add(role)
                db.session.flush()  # Flush pour obtenir l'ID du rôle

                # Associer les permissions au rôle
                for perm_intitule in perms:
                    permission = Permission.query.filter_by(intitule=perm_intitule).first()
                    if permission:
                        role.permissions.append(permission)
                roles_inserted += 1
                print(f"  ✓ Rôle créé: {role_name}")
            else:
                print(f"  ⚠ Rôle existe déjà: {role_name}")
        
        # Commit les rôles
        db.session.commit()
        print(f"\n✅ {roles_inserted} rôles insérés sur {len(roles_dict)}")

        # 4. Insérer les utilisateurs
        users_inserted = 0
        for data in users_data:
            user = User.query.filter_by(email=data["email"]).first()
            if not user:
                # Extraire password et roles du dictionnaire
                password = data.pop("password")
                role_names = data.pop("roles").split(",")  # Peut être "admin" ou "admin,dcf"
                
                # Créer l'utilisateur sans password et roles
                user = User(**data)
                
                # Définir le mot de passe avec hachage
                user.set_password(password)
                
                # Associer les rôles
                for role_name in role_names:
                    role = Role.query.filter_by(intitule=role_name.strip()).first()
                    if role:
                        user.roles.append(role)
                    else:
                        print(f"  ⚠ Rôle introuvable: {role_name}")
                
                db.session.add(user)
                users_inserted += 1
                print(f"  ✓ Utilisateur créé: {user.email} avec rôles: {role_names}")
            else:
                print(f"  ⚠ Utilisateur existe déjà: {data['email']}")
        
        # Commit les utilisateurs
        db.session.commit()
        print(f"\n✅ {users_inserted} utilisateurs insérés sur {len(users_data)}")
        
        #4. inser quantume 
        for q in quantum_data:
            quant = Quantume.query.filter_by(libelle=q).first()
            if not quant:
                quant = Quantume(libelle=q)
                db.session.add(quant)
                print(f"  ✓ Quantume créé: {q}")
            else:
                print(f"  ⚠ Quantume existe déjà: {q}")

        # Commit les quantumes
        db.session.commit()
        print(f"\n✅ {len(quantum_data)} quantumes insérés sur {len(quantum_data)}")