
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import subprocess
import geojson
import os
import sys
import tempfile
import random
import string
import traceback

'''
curl -X POST "http://130.225.37.27:5000/processes/assessment-indicator/execution" -H "Content-Type: application/json" 
-d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\"}}"


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
    'keywords': ['HELCOM', 'HEAT'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://BLAAAAAA',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'assessmentPeriod': {
            'title': 'Assessment Period',
            'description': 'Ask HELCOM. Example: 2011-2016',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['BLA']
        }
    },
    'outputs': {
        'verbal_result': {
            'title': 'Assessment Indicator CSV',
            'description': 'Assessment Indicator CSV, ask HELCOM.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'text/csv'
            }
        }
    },
    'example': {
        'inputs': {
            'assessmentPeriod': "2011-2016"
        }
    }
}

class HELCOMAssessmentIndicatorProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<HELCOMAssessmentIndicatorProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting HEAT_subparts4_wk4.R as ogc_service!"')
        try:
            return self._execute(data)
        except Exception as e:
            LOGGER.error(e)
            print(traceback.format_exc())

    def _execute(self, data):

        # Defaut directories, if environment var not set:
        PYGEOAPI_DATA_DIR = '/home/ubuntu/Input' # TODO in production remove this!

        # Get PYGEOAPI_DATA_DIR from environment:
        #if not 'PYGEOAPI_DATA_DIR' in os.environ:
        #    err_msg = 'ERROR: Missing environment variable PYGEOAPI_DATA_DIR. We cannot find the input data!\nPlease run:\nexport PYGEOAPI_DATA_DIR="/.../"'
        #    LOGGER.error(err_msg)
        #    raise ValueError(err_msg)

        path_data = os.environ.get('PYGEOAPI_DATA_DIR', PYGEOAPI_DATA_DIR)

        # Get input:
        assessmentPeriod = data.get('assessmentPeriod')

        # Check validity of argument:
        validAssessmentPeriods = ["1877-9999", "2011-2016", "2016-2021"]
        if not assessmentPeriod in validAssessmentPeriods:
            raise ValueError('assessmentPeriod is "%s", must be one of: %s' % (assessmentPeriod, type(assessmentPeriod), validAssessmentPeriods))

        # Define output path for this run:
        randomstring = (''.join(random.sample(string.ascii_lowercase+string.digits, 6)))
        output_temp_dir = tempfile.gettempdir()+os.sep+assessmentPeriod+'_'+randomstring

        ########################
        ### Call R Script 1: ###
        ########################
        # I think it takes the map of the assessment units and makes grid units. What for?
    
        # Define input file paths: Unitsfile, Configuration file.
        if (assessmentPeriod == "1877-9999"):
            configurationFileName = "1877-9999/Configuration1877-9999.xlsx"
        elif (assessmentPeriod == "2011-2016"):
            configurationFileName = "2011-2016/Configuration2011-2016.xlsx"
        elif assessmentPeriod == "2016-2021":
            configurationFileName = "2016-2021/Configuration2016-2021.xlsx"
        else:
            pass # TODO error

        r_file_name = 'HEAT_subpart4_wk4_wk5.R'
        r_file_name = 'HEAT_subpart4_wk5.R'
        LOGGER.info('Now calling bash which calls R: %s' % r_file_name)
        LOGGER.debug('Current directory: %s' % os.getcwd())
        r_file = '/home/ubuntu/'+r_file_name
        cmd = ["/usr/bin/Rscript", "--vanilla", r_file, configurationFileName, output_temp_dir]
        LOGGER.debug('Bash command:')
        LOGGER.info(cmd)
        LOGGER.debug('Run command... (Output will be shown once the command has finished)')
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        stdoutdata, stderrdata = p.communicate()
        LOGGER.debug("Done running command!")

        ### Get return code and output
        LOGGER.info('Bash process exit code: %s' % p.returncode)
        stdouttext = stdoutdata.decode()
        stderrtext = stderrdata.decode()
        if len(stderrdata) > 0:
            err_and_out = '___PROCESS OUTPUT___\n___stdout___\n%s\n___stderr___\n%s\n___END___' % (stdouttext, stderrtext)
        else:
            err_and_out = '___PROCESS OUTPUT___\n___stdout___\n%s\n___(Nothing written to stderr)___\n___END___' % stdouttext
        LOGGER.info(err_and_out)

        # There are no results, except for the one file that R stores for further use:
        # /home/ubuntu/intermediate_files/my_wk5.rds
        # and one CSV of the Assessment Indicator:
        # /tmp/.../Assessment_Indicator.csv


        ################
        ### Results: ###
        ################

        if p.returncode == 0:
            res = 'Finished Ok'
            resultfilepath = output_temp_dir+os.sep+'Assessment_Indicator.csv'
            LOGGER.info('Reading result from R process from file "%s"' % resultfilepath)
            with open(resultfilepath, 'r') as mycsv:
                resultfile = mycsv.read()
            mimetype = 'text/csv'
            return mimetype, resultfile

        else:
            LOGGER.warning('The R process did not return 0, so it went wrong...')
            outputs = {
                'id': 'verbal_result',
                'value': 'went_wrong' # TODO Better return (error msg) here!
            }
            mimetype = 'application/json'
            return mimetype, outputs





