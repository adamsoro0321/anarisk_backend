from app import app
import os
from waitress import serve
host = os.getenv('FLASK_HOST', '127.0.0.1')


port = int(os.getenv('FLASK_PORT', 5000))
serve(app, host=host, port=port, threads=1) #WAITRESS!