import os

os.environ['PYGEOAPI_CONFIG'] = '/opt/.../pygeoapi/pygeoapi-config.yml'
os.environ['PYGEOAPI_OPENAPI'] = '/opt/.../pygeoapi/pygeoapi-openapi.yml'
os.environ['PYGEOAPI_DATA_DIR'] = '/opt/.../Input/'
os.environ['R_SCRIPT_DIR'] = '/opt/.../R_scripts/'

from logging.config import dictConfig

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(levelname)s in %(filename)s: %(message)s",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "default",
            }
        },
        "root": {"level": "DEBUG", "handlers": ["console"]},
    }
)

from pygeoapi.flask_app import APP as application
