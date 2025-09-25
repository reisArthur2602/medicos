from flask import Flask
from flask_cors import CORS
from atestado import atestado_bp
from declaracao import declaracao_bp
from receita import receita_bp
from pedido_medicos import exames_bp  # ✅ arquivo certo

app = Flask(__name__)
CORS(app)

# Registra os blueprints
app.register_blueprint(atestado_bp)
app.register_blueprint(declaracao_bp)
app.register_blueprint(receita_bp)
app.register_blueprint(exames_bp)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=6969, debug=True)  
