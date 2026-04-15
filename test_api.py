"""
Script de test pour l'API ANARISK
"""
import requests
import json

BASE_URL = "http://127.0.0.1:5000"

def test_health_check():
    """Test de la route health check"""
    print("\n=== Test Health Check ===")
    response = requests.get(f"{BASE_URL}/api/v1/health")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

def test_login():
    """Test de l'endpoint login"""
    print("\n=== Test Login ===")
    data = {
        "email": "test@example.com",
        "password": "test123"
    }
    response = requests.post(
        f"{BASE_URL}/api/v1/login",
        json=data,
        headers={"Content-Type": "application/json"}
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    return response.json()

def test_register():
    """Test de l'endpoint register"""
    print("\n=== Test Register ===")
    data = {
        "email": "newuser@dgi.bf",
        "password": "password123",
        "nom": "Ouedraogo",
        "prenom": "Moussa",
        "role": "bv",
        "ur": "DGE",
        "brigade": "BV1_DGE"
    }
    response = requests.post(
        f"{BASE_URL}/api/v1/register",
        json=data,
        headers={"Content-Type": "application/json"}
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

def test_protected_route(token):
    """Test d'une route protégée"""
    print("\n=== Test Route Protégée (/api/v1/me) ===")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.get(f"{BASE_URL}/api/v1/me", headers=headers)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

if __name__ == "__main__":
    print("=" * 60)
    print("Tests de l'API ANARISK - DGI Burkina Faso")
    print("=" * 60)
    
    try:
        # Test health check
        test_health_check()
        
        # Test register (créer un utilisateur de test)
        test_register()
        
        # Test login
        login_response = test_login()
        
        # Si le login réussit, tester une route protégée
        if login_response.get("token"):
            token = login_response["token"]
            test_protected_route(token)
        
        print("\n" + "=" * 60)
        print("Tests terminés avec succès !")
        print("=" * 60)
        
    except requests.exceptions.ConnectionError:
        print("\n❌ ERREUR: Impossible de se connecter au serveur.")
        print("Assurez-vous que le serveur Flask est en cours d'exécution.")
    except Exception as e:
        print(f"\n❌ ERREUR: {str(e)}")
