
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


'''

Written by Merret on 2023-10-16
Just for testing purposes - licenses not checked yet!

curl -X POST "http://localhost:5000/processes/get-species-data/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"species_name\": \"Conorhynchos conirostris\"}}" 



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
            'metadata': None,  # TODO how to use?
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
            'species_name': 'Conorhynchos conirostris'
        }
    }
}

class GetSpeciesData(BaseProcessor):
    """Get Get-Species-Data Processor example"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.get_tide_id.GetFilteredVector
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        print('Starting "get_species_data as ogc_service!"')

        # Get PYGEOAPI_DATA_DIR from environment:
        #if not 'PYGEOAPI_DATA_DIR' in os.environ:
        #    print('ERROR: Missing environment variable PYGEOAPI_DATA_DIR. We cannot find the input data!\nPlease run:\nexport PYGEOAPI_DATA_DIR="/.../"')
        #    print('Exiting...')
        #    sys.exit(1) # This leads to curl error: (52) Empty reply from server. TODO: Send error message back!!!

        species_name = data.get("species_name")
        print('Py: Species name:  %s' % species_name)

        ### Call R Script:
        ### Attention: Path to data is hard-coded in R-Script!
        # #/usr/bin/Rscript --vanilla /home/mbuurman/work/trying_out_hydrographr/get_species_data.R "/tmp/species.csv" "Conorhynchos conirostris" "/home/mbuurman/work/hydro_casestudy_saofra/data/basin_481051/basin_481051.gpkg"
        path_command = "/home/mbuurman/work/instance_pygeoapi/pygeoapi/pygeoapi/pygeoapi/process/get_species_data.r"
        #path_command = "get_species_data.r"
        temp_dir_path = "/tmp/get_species_data.csv"
        polygon_inputfile = "/home/mbuurman/work/hydro_casestudy_saofra/data/basin_481051/basin_481051.gpkg"
        cmd = ["/usr/bin/Rscript", "--vanilla", path_command, temp_dir_path, species_name, polygon_inputfile]
        print('Py: Bash command:')
        print(cmd)
        print('Py: Run command...')
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        stdoutdata, stderrdata = p.communicate()
        print("Py: Done running command!")


        ### Get return code and output
        print('Py: Bash process exit code: %s' % p.returncode)
        stdouttext = stdoutdata.decode()
        stderrtext = stderrdata.decode()
        if len(stderrdata) > 0:
            err_and_out = '___PROCESS OUTPUT___\n___stdout___\n%s\n___stderr___\n%s\n___END___' % (stdouttext, stderrtext)
        else:
            err_and_out = '___PROCESS OUTPUT___\n___stdout___\n%s\n___(Nothing written to stderr)___\n___END___' % stdouttext
        print(err_and_out)

        '''
        X,Y,occurence_id,longitude,latitude,species,occurrenceStatus,country,year
        -44.885825,-17.25355,"1",-44.885825,-17.25355,Conorhynchos conirostris,PRESENT,Brazil,"2021"
        -43.595833,-13.763611,"2",-43.595833,-13.763611,Conorhynchos conirostris,PRESENT,Brazil,"2020"
        -40.492972,-12.948389,"3",-40.492972,-12.948389,Conorhynchos conirostris,PRESENT,Brazil,"2020"
        -45.780827,-17.178306,"4",-45.780827,-17.178306,Conorhynchos conirostris,PRESENT,Brazil,"2018"
        -45.904444,-17.074722,"5",-45.904444,-17.074722,Conorhynchos conirostris,PRESENT,Brazil,"2014"
        -46.893511,-17.397917,"6",-46.893511,-17.397917,Conorhynchos conirostris,PRESENT,Brazil,"2012"
        -45.245,-18.136,"7",-45.245,-18.136,Conorhynchos conirostris,PRESENT,Brazil,"2009"
        -45.24,-18.14,"8",-45.24,-18.14,Conorhynchos conirostris,PRESENT,Brazil,"2009"
        -43.138889,-11.092222,"9",-43.138889,-11.092222,Conorhynchos conirostris,PRESENT,Brazil,"2001"
        -44.954661,-17.356778,"10",-44.954661,-17.356778,Conorhynchos conirostris,PRESENT,Brazil,"1998"
        -43.966667,-14.916667,"11",-43.966667,-14.916667,Conorhynchos conirostris,PRESENT,Brazil,"1998"
        -44.954963,-17.336154,"12",-44.954963,-17.336154,Conorhynchos conirostris,PRESENT,Brazil,"1942"
        -44.357,-15.49277,"13",-44.357,-15.49277,Conorhynchos conirostris,PRESENT,Brazil,"1932"
        -44.357,-15.492768,"14",-44.357,-15.492768,Conorhynchos conirostris,PRESENT,Brazil,"1907"
        '''
        
        print('Py: Read result coordinates from file: %s' % temp_dir_path)
        first_line = True
        output_list = []
        with open(temp_dir_path, mode='r') as myfile:  # b is important -> binary
            for line in myfile:
                if first_line:
                    first_line = False
                    continue
                line = line.strip().split(',')
                west, south = line[0], line[1]
                print('  West: %s, South: %s' % (west, south))
                output_list.append([float(west), float(south)])

        output_as_geojson = {"type": "MultiPoint", "coordinates": output_list}
        import json
        print('Py: OUTPUT GEOJSON: %s' % json.dumps(output_as_geojson))

        outputs = {
            'id': 'species_occurrences',
            'result': output_as_geojson
        }

        mimetype = 'application/json'
        return mimetype, outputs


    def __repr__(self):
        return f'<GetSpeciesData> {self.name}'
