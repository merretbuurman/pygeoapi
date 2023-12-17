
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import geojson

from pygeoapi.process.aquainfra.calling_r_scripts import PYGEOAPI_DATA_DIR as DATA_DIR
from pygeoapi.process.aquainfra.aquainfra import _get_basin_polygon

'''
Curl to test:
curl -X POST "http://localhost:5000/processes/get-drainage-basin/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"basin_id\": \"481051\"}}"
'''


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'BLA',
    'title': {'en': 'Get drainage basin polygon'},
    'description': {
        'en': 'Get drainage basin as a vector polygon (GeoJSON)'
              'based on drainage basin id.'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['polygon', 'drainage-basin'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://BLAAAAAA',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'basin_id': {
            'title': 'Basin ID',
            'description': 'ID of the drainage basin that you would like to get as polygon.',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,    # TODO several possible?
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['BLA']
        }
    },
    'outputs': {
        'basin_geojson': {
            'title': 'Drainage Basin Polygon',
            'description': 'Drainage Basin Polygon as GeoJSON',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'basin_id': "481051"
        }
    }
}

class DrainageBasinProcessor(BaseProcessor):
    """Get Drainage Basin Processor"""

    def __init__(self, processor_def):
         super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        LOGGER.info('Starting DrainageBasinProcessor as ogc_service!"')

        # Get input:
        basin_id = data.get('basin_id')
        LOGGER.debug('User requested this drainage basin: %s' % basin_id)

        # Retrieve polygon:
        polygon_geodataframe = _get_basin_polygon(DATA_DIR, basin_id=basin_id)
        LOGGER.debug('Retrieved polygon: %s' % type(polygon_geodataframe))

        # Convert to geojson:
        output_as_geodataframe = polygon_geodataframe
        LOGGER.debug('Converting result to GeoJSON...')
        output_as_geojson_string = output_as_geodataframe.to_json()
        output_as_geojson_pretty = geojson.loads(output_as_geojson_string)
        LOGGER.debug('Converting done. Result as geojson: %s ... ... ...' % output_as_geojson_string[0:200])

        # Return outputs:
        outputs = {
            'id': 'basin_geojson',
            'value': output_as_geojson_pretty
        }

        mimetype = 'application/json'
        return mimetype, outputs

    def __repr__(self):
        return f'<DrainageBasinProcessor> {self.name}'


