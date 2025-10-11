from flask import Flask
from flask_cors import CORS

from backend.routes.animations import bp as animations_bp
from backend.routes.images import bp as images_bp
from backend.routes.uploads import bp as uploads_bp

app = Flask(__name__)
CORS(app)

# register routes
app.register_blueprint(uploads_bp)
app.register_blueprint(images_bp)
app.register_blueprint(animations_bp)

if __name__ == "__main__":
    # Default local dev port
    app.run(host="0.0.0.0", port=8080, debug=True)
