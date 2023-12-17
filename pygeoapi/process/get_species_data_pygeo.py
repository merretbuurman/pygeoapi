
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)
# TODO Improve logging!

import argparse
import subprocess
import geojson
import os
import sys
import tempfile
import random
import string

from pygeoapi.process.aquainfra.calling_r_scripts import PYGEOAPI_DATA_DIR as DATA_DIR
from pygeoapi.process.aquainfra.calling_r_scripts import get_species_data
from pygeoapi.process.aquainfra.calling_r_scripts import csv_coordinates_to_geodataframe




'''
Written by Merret on 2023-10-16
Just for testing purposes - licenses not checked yet!

Curl to test:
curl -X POST "http://localhost:5000/processes/get-species-data/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"species_name\": \"Conorhynchos conirostris\", \"basin_id\": \"481051\"}}"
'''



#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'get-species-data',
    'title': {'en': 'Get species data from GBIF!'},
    'description': {
        'en': 'Trying to expose stuff from hydrographr package as ogc process: '
              'https://glowabio.github.io/hydrographr/'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['hydrographr', 'example', 'rgbif', 'GBIF'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'species_name': {
            'title': 'Species name',
            'description': 'The Name of the Species',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['freshwater species']
        },
        'basin_id': {
            'title': 'Basin Id',
            'description': 'TODO',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['river basin']
        }
    },
    'outputs': {
        'species_occurrences': {
            'title': 'Coordinates of species occurrences',
            'description': 'TODO.',
            'schema': {
                "type": "string",
                "contentMediaType": "application/json"
            }
        }
    },
    'example': {
        'inputs': {
            'species_name': 'Conorhynchos conirostris',
            'basin_id': '481051'
        }
    }
}

class GetSpeciesData(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        LOGGER.info('Starting "get_species_data as ogc_service!"')

        # Get inputs:
        species_name = data.get("species_name")
        basin_id = data.get("basin_id")
        LOGGER.debug('User provided species name:  %s' % species_name)
        LOGGER.debug('User provided basin id:  %s' % basin_id)

        # Call R script
        # TODO: Do these two as one step?
        # TODO: Getting stuff from R always via temp file? - How else?
        # Writes comma-separated:
        #mbuurman@IN0142:~$ cat /tmp/__output_getspeciesdatatool_8had3.csv
        #X,Y,occurence_id,longitude,latitude,species,occurrenceStatus,country,year
        #-44.885825,-17.25355,"1",-44.885825,-17.25355,Conorhynchos conirostris,PRESENT,Brazil,"2021"
        #-43.595833,-13.763611,"2",-43.595833,-13.763611,Conorhynchos conirostris,PRESENT,Brazil,"2020"
        result_file_path = get_species_data(species_name, basin_id, DATA_DIR)
        remove_temp_file = True
        sep = ',' # as get_species_data writes comma-separated
        col_name_lon = 'longitude'
        col_name_lat = 'latitude'
        output_as_geodataframe = csv_coordinates_to_geodataframe(result_file_path, col_name_lon, col_name_lat, sep, remove_temp_file)

        # Convert to geojson:
        LOGGER.debug('Converting result to GeoJSON...')
        output_as_geojson_string = output_as_geodataframe.to_json()
        output_as_geojson_pretty = geojson.loads(output_as_geojson_string)
        LOGGER.debug('Converting done. Result as geojson: %s ... ... ...' % output_as_geojson_string[0:200])

        # Return output
        outputs = {
            'id': 'species_occurrences',
            'value': output_as_geojson_pretty
        }

        mimetype = 'application/json'
        return mimetype, outputs


    def __repr__(self):
        return f'<GetSpeciesData> {self.name}'
