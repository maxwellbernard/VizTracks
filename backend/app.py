import logging
import os

from flask import Flask
from flask_cors import CORS

from backend.routes.animations import bp as animations_bp
from backend.routes.images import bp as images_bp
from backend.routes.uploads import bp as uploads_bp


def _configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    gunicorn_error = logging.getLogger("gunicorn.error")
    root = logging.getLogger()
    if gunicorn_error.handlers:
        root.handlers = gunicorn_error.handlers
        root.setLevel(level)
    else:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


_configure_logging()

app = Flask(__name__)
CORS(app)

# register routes
app.register_blueprint(uploads_bp)
app.register_blueprint(images_bp)
app.register_blueprint(animations_bp)
