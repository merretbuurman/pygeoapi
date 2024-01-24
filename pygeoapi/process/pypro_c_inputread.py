
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import subprocess
import geojson
import os
import sys
import traceback
from pygeoapi.process.utils import get_output_temp_dir
from pygeoapi.process.utils import call_r_script


'''
curl -X POST "http://130.225.37.27:5000/processes/assessment-read/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\"}}"

# Save result to file:
curl -X POST -o Assessment.csv "http://130.225.37.27:5000/processes/assessment-read/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\"}}"

'''


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'assessment-read',
    'title': {'en': 'HELCOM Assessment'},
    'description': {
        'en': 'Process to compute the HELCOM Assessment for the HEAT assessment tool.'
              ' This process represents subpart 3 of the original HEAT assessment computation script.'
              ' For more info, please go to: https://github.com/ices-tools-prod/HEAT/tree/master'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['HELCOM', 'HEAT'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'GitHub repo for the original HEAT analysis',
        'href': 'https://github.com/ices-tools-prod/HEAT/tree/master',
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
            'title': 'Assessment CSV',
            'description': 'Assessment CSV (Assessment.csv), ask HELCOM.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'text/csv'
            }
        }
    },
    'example': {
        'inputs': {
            'annual_indicators_csv': "2011-2016-TODO" # TODO
        }
    }
}

class HELCOMAssessmentReadProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<HELCOMAssessmentReadProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting HEAT_subparts5...R as ogc_service!"')
        try:
            return self._execute(data)
        except Exception as e:
            LOGGER.error(e)
            print(traceback.format_exc())

    def _execute(self, data):

        # Get PYGEOAPI_DATA_DIR from environment:
        if not 'PYGEOAPI_DATA_DIR' in os.environ:
            err_msg = 'ERROR: Missing environment variable PYGEOAPI_DATA_DIR. We cannot find the input data!\nPlease run:\nexport PYGEOAPI_DATA_DIR="/.../"'
            LOGGER.error(err_msg)
            raise ValueError(err_msg)

        # Get R_SCRIPT_DIR from environment:
        if not 'R_SCRIPT_DIR' in os.environ:
            err_msg = 'ERROR: Missing environment variable R_SCRIPT_DIR. We cannot find the R scripts!\nPlease run:\nexport R_SCRIPT_DIR="/.../"'
            LOGGER.error(err_msg)
            raise ValueError(err_msg)

        path_data = os.environ.get('PYGEOAPI_DATA_DIR').rstrip('/')
        path_rscripts = os.environ.get('R_SCRIPT_DIR').rstrip('/')
        path_intermediate = path_data+os.sep+'intermediate' # Not writeable by Apache2: Need better solution in production!
        path_intermediate = '/tmp/intermediate' # TODO Better solution in production!

        # Get input:
        assessmentPeriod = data.get('assessmentPeriod')

        # Define output path for this run:
        output_temp_dir = get_output_temp_dir(assessmentPeriod)
        if not os.path.exists(output_temp_dir):
            os.makedirs(output_temp_dir)


        ########################
        ### Call R Script 1: ###
        ########################
        # I think it takes the map of the assessment units and makes grid units. What for?
    
        # Define input file path: Configuration file.
        if (assessmentPeriod == "1877-9999"):
            configurationFileName = path_data+os.sep+"1877-9999/Configuration1877-9999.xlsx"
        elif (assessmentPeriod == "2011-2016"):
            configurationFileName = path_data+os.sep+"2011-2016/Configuration2011-2016.xlsx"
        elif assessmentPeriod == "2016-2021":
            configurationFileName = path_data+os.sep+"2016-2021/Configuration2016-2021.xlsx"
        else:
            pass # TODO error

        # Output:
        resultfilepath = output_temp_dir+os.sep+'Assessment.csv'

        r_file_name = 'HEAT_subpart5_inputread.R'
        r_args = [configurationFileName, path_intermediate, resultfilepath]
        returncode = call_r_script('1', LOGGER, r_file_name, path_rscripts, r_args)

        # There are no results, except for the one file that R stores for further use:
        # /.../intermediate/my_wk5.rds
        # and one CSV of the Assessment Indicator:
        # /tmp/.../Assessment_Indicator.csv


        ################
        ### Results: ###
        ################

        if returncode == 0:
            LOGGER.info('Reading result from R process from file "%s"' % resultfilepath)
            with open(resultfilepath, 'r') as mycsv:
                resultfile = mycsv.read()
            mimetype = 'text/csv'
            LOGGER.info('Returning CSV content as mimetype "%s"' % mimetype)
            return mimetype, resultfile

        else:
            outputs = {'id': 'error_message', 'value': 'R script "%s" failed.' % r_file_name}
            LOGGER.warning('Script "%s" failed. Returning error message.' % r_file_name)
            return 'application/json', outputs

