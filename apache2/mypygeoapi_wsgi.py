import os

os.environ['PYGEOAPI_CONFIG'] = '/opt/.../pygeoapi/pygeoapi-config.yml'
os.environ['PYGEOAPI_OPENAPI'] = '/opt/.../pygeoapi/pygeoapi-openapi.yml'
os.environ['PYGEOAPI_DATA_DIR'] = '/opt/.../Input/'
os.environ['R_SCRIPT_DIR'] = '/opt/.../R_scripts/'

from pygeoapi.flask_app import APP as application
