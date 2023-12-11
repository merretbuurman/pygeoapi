
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import json
import geojson
import geopandas


from pygeoapi.process.aquainfra.aquainfra import PYGEOAPI_DATA_DIR as DATA_DIR
from pygeoapi.process.aquainfra.aquainfra import _get_basin_polygon
from pygeoapi.process.aquainfra.aquainfra import _read_dam_data_GRanD_dams_v1_3
from pygeoapi.process.aquainfra.aquainfra import _spatial_filter

'''
Written by Merret on 2023-11
Just for testing purposes - licenses not checked yet!

Curl to test:
curl -X POST "http://localhost:5000/processes/get-dams-grand/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"basin_id\": \"481051\"}}"

# With FeatureCollection
curl -X POST "http://localhost:5000/processes/get-dams-grand/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"basin_geojson\": {\"type\": \"FeatureCollection\", \"features\": [{\"type\": \"Feature\", \"properties\": {}, \"geometry\": {\"type\": \"Polygon\", \"coordinates\": [[[-43.8798, -16.701], [ -46.5404,-16.701], [-46.5404, -18.608], [-43.87982, -18.608], [-43.879819, -16.701]]]}}]}}}"

# With Feature
curl -X POST "http://localhost:5000/processes/get-dams-grand/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"basin_geojson\": {\"type\": \"Feature\", \"properties\": {}, \"geometry\": {\"type\": \"Polygon\", \"coordinates\": [[[-43.8798, -16.701], [ -46.5404,-16.701], [-46.5404, -18.608], [-43.87982, -18.608], [-43.879819, -16.701]]]}}}}"

# With Polygon
curl -X POST "http://localhost:5000/processes/get-dams-grand/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"basin_geojson\": {\"type\": \"Polygon\", \"coordinates\": [[[-43.8798, -16.701], [ -46.5404,-16.701], [-46.5404, -18.608], [-43.87982, -18.608], [-43.879819, -16.701]]]}}}"

'''

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'BLA',
    'title': {'en': 'BLAAAAAAAA'},
    'description': {
        'en': 'BLAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
              'BLAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['BLAAAAAA', 'example'],
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
            'minOccurs': 0,
            'maxOccurs': 1,    # TODO several possible?
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['BLA']
        },
        'basin_geojson': {
            'title': 'Drainage Basin Polygon',
            'description': 'Drainage Basin Polygon as GeoJSON',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            },
            'minOccurs': 0,
            'maxOccurs': 1,    # TODO several possible?
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['BLA']
        }
    },
    'outputs': {
        'dams_grand': {
            'title': 'Dams',
            'description': 'Dams as GeoJSON',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'basin_id': '481051',
            'basin_geojson': {} 
        }
    }
}

class DamGrandProcessor(BaseProcessor):

    def __init__(self, processor_def):
         super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        LOGGER.info('Starting DamGrandProcessor as ogc_service!"')


        # Get inputs:
        basin_id = data.get('basin_id')
        LOGGER.debug('User provided this drainage basin id: "%s"' % basin_id)
        basin_polygon = data.get('basin_geojson')
        LOGGER.debug('User provided this drainage basin polygon: %s ... ...' % geojson.dumps(basin_polygon)[0:150])

        # Decision about inputs:
        if basin_id is not None and basin_polygon is not None:
            LOGGER.error('User provided both basin_id and basin_polygon! Unsure which one to use!')
            # TODO: What if both are provided?
            # TODO Return error here!
        elif basin_id is None and basin_polygon is None:
            LOGGER.error('Have to provide one at least!')
            # TODO Return error here!
 
        # Retrieve polygon:
        if basin_polygon is not None:
            LOGGER.info('Using the basin polygon that the user provided.')
            LOGGER.debug('Converting the polygon to GeoDataFrame...')
            basin_polygon = geojson.loads(json.dumps(basin_polygon)) # maybe more elegant way?
            if isinstance(basin_polygon, geojson.geometry.Geometry):
                basin_polygon = geojson.Feature(geometry=basin_polygon) # This does not verify if the input was already a Feature!
                LOGGER.debug('Feature: %s' % basin_polygon)
            if isinstance(basin_polygon, geojson.feature.Feature):
                basin_polygon = geojson.FeatureCollection([basin_polygon])
                LOGGER.debug('Feature Collection: %s' % basin_polygon)
            if isinstance(basin_polygon, geojson.feature.FeatureCollection):
                polygon_geodataframe = geopandas.GeoDataFrame.from_features(basin_polygon)
                LOGGER.debug('Feature Collection GeoDataFrame: %s' % polygon_geodataframe)
            LOGGER.debug('Converting the polygon to GeoDataFrame... done.')

        elif basin_id is not None:
            LOGGER.info('Using basin "%s" for which we will retrieve the polygon.' % basin_id)
            LOGGER.debug('Retrieving polygon for basin "%s"...' % basin_id)
            polygon_geodataframe = _get_basin_polygon(DATA_DIR, basin_id=basin_id)
            LOGGER.debug('Retrieving polygon... done.')

        # Get dam data
        LOGGER.info('Retrieving data about dams from GRanD v 1.3...')
        dams_grand = _read_dam_data_GRanD_dams_v1_3(DATA_DIR)
        LOGGER.info('Retrieving data about dams from GRanD v 1.3... done.')

        # Filter by basin polygon:
        LOGGER.info('Filtering dams by drainage basin "%s...' % basin_id)
        dams_grand = _spatial_filter(dams_grand, polygon_geodataframe)
        LOGGER.info('Filtering dams by drainage basin... done. %s objects left...' % len(dams_grand))
        #print(dams_grand.head())

        # Convert to geojson:
        output_as_geodataframe = dams_grand
        LOGGER.debug('Converting result to GeoJSON...')
        output_as_geojson_string = output_as_geodataframe.to_json()
        output_as_geojson_pretty = geojson.loads(output_as_geojson_string)
        LOGGER.debug('Converting done. Result as geojson: %s ... ... ...' % output_as_geojson_string[0:200])

        # Return outputs:
        # TODO: Should we just return the GeoJSON and leave out this id stuff? Read specs!
        outputs = {
            'id': 'dams_grand',
            'value': output_as_geojson_pretty
        }

        mimetype = 'application/json'
        return mimetype, outputs

    def __repr__(self):
        return f'<DamGrandProcessor> {self.name}'


