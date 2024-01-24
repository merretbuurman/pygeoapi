
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
curl -X POST "http://130.225.37.27:5000/processes/annual-indicator/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"combined_Chlorophylla_IsWeighted\": true}}"

# Save result to file:
curl -X POST -o /tmp/Annual_Indicator.csv "http://130.225.37.27:5000/processes/annual-indicator/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"combined_Chlorophylla_IsWeighted\": true}}"



'''


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'annual-indicator',
    'title': {'en': 'HELCOM Annual Indicator'},
    'description': {
        'en': 'Process to compute the HELCOM Annual Indicator for the HEAT assessment tool.'
              ' This process represents subpart 1 of the original HEAT assessment computation script.'
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
        },
        'combined_Chlorophylla_IsWeighted': {
            'title': 'combined_Chlorophylla_IsWeighted',
            'description': 'Ask HELCOM. Example: true', # TODO make boolean
            'schema': {'type': 'bool'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None, 
            'keywords': ['BLA']
        }
    },
    'outputs': {
        'verbal_result': {
            'title': 'Annual Indicator CSV',
            'description': 'Annual Indicator CSV (Annual_Indicator.csv), ask HELCOM.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/csv'
            }
        }
    },
    'example': {
        'inputs': {
            'assessmentPeriod': "2011-2016",
            'combined_Chlorophylla_IsWeighted': 'true'
        }
    }
}

class HELCOMAnnualIndicatorProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<HELCOMAnnualIndicatorProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting process "Annual Indicator" (HEAT_subpart1_gridunits - HEAT_subpart2_stations - HEAT_subpart3_wk3) as ogc_service!"')
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
        combined_Chlorophylla_IsWeighted = data.get('combined_Chlorophylla_IsWeighted')

        # Check validity of argument:
        validAssessmentPeriods = ["1877-9999", "2011-2016", "2016-2021"]
        if not assessmentPeriod in validAssessmentPeriods:
            raise ValueError('assessmentPeriod is "%s", must be one of: %s' % (assessmentPeriod, type(assessmentPeriod), validAssessmentPeriods))

        # Define output path for this run:
        randomstring = (''.join(random.sample(string.ascii_lowercase+string.digits, 6)))
        output_temp_dir = tempfile.gettempdir()+os.sep+assessmentPeriod+'_'+randomstring
        if not os.path.exists(output_temp_dir):
            os.makedirs(output_temp_dir)

        # End result:
        resultfilepath = output_temp_dir+os.sep+'Annual_Indicator.csv'

        # Start running the various scripts:

        r_file_name = 'HEAT_subpart1_gridunits.R'
        returncode = self.first_r_script(r_file_name, path_rscripts, assessmentPeriod, path_data, output_temp_dir, path_intermediate)
        if not returncode == 0:
            outputs = {'id': 'error_message', 'value': 'R script "%s" failed.' % r_file_name}
            LOGGER.info('Script "%s" failed. Returning error message.' % r_file_name)
            return 'application/json', outputs

        r_file_name = 'HEAT_subpart2_stations.R'
        returncode = self.second_r_script(r_file_name, path_rscripts, assessmentPeriod, path_data, output_temp_dir, path_intermediate)
        if not returncode == 0:
            outputs = {'id': 'error_message', 'value': 'R script "%s" failed.' % r_file_name}
            LOGGER.info('Script "%s" failed. Returning error message.' % r_file_name)
            return 'application/json', outputs

        r_file_name = 'HEAT_subpart3_wk3.R'
        returncode = self.third_r_script(r_file_name, path_rscripts, assessmentPeriod, combined_Chlorophylla_IsWeighted, path_data, resultfilepath, path_intermediate)
        if not returncode == 0:
            outputs = {'id': 'error_message', 'value': 'R script "%s" failed.' % r_file_name}
            LOGGER.info('Script "%s" failed. Returning error message.' % r_file_name)
            return 'application/json', outputs


        ################
        ### Results: ###
        ################

        LOGGER.info('Reading result from R process from file "%s"' % resultfilepath)
        with open(resultfilepath, 'r') as mycsv:
            resultfile = mycsv.read()
        mimetype = 'text/csv'
        LOGGER.info('Returning CSV content as mimetype "%s"' % mimetype)
        return mimetype, resultfile


    def call_r_script(self, num, r_file_name, path_rscripts, r_args):

        LOGGER.debug('Now calling bash which calls R: %s' % r_file_name)
        r_file = path_rscripts.rstrip('/')+os.sep+r_file_name
        cmd = ["/usr/bin/Rscript", "--vanilla", r_file] + r_args
        LOGGER.info(cmd)
        LOGGER.debug('Running command... (Output will be shown once finished)')
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        stdoutdata, stderrdata = p.communicate()
        LOGGER.debug("Done running command! Exit code from bash: %s" % p.returncode)

        ### Print stdout and stderr
        stdouttext = stdoutdata.decode()
        stderrtext = stderrdata.decode()
        if len(stderrdata) > 0:
            err_and_out = 'R stdout and stderr:\n___PROCESS OUTPUT {n}___\n___stdout___\n{stdout}\n___stderr___\n{stderr}   (END PROCESS OUTPUT {n})\n___________'.format(
                stdout= stdouttext, stderr=stderrtext, n=num)
        else:
            err_and_out = 'R stdour:\n___PROCESS OUTPUT {n}___\n___stdout___\n{stdout}\n___stderr___\n___(Nothing written to stderr)___\n   (END PROCESS OUTPUT {n})\n___________'.format(
                stdout = stdouttext, n = num)
        LOGGER.info(err_and_out)
        return p.returncode


    def first_r_script(self, r_file_name, path_rscripts, assessmentPeriod, path_data, output_temp_dir, path_intermediate):
        # I think it takes the map of the assessment units and makes grid units. What for?
    
        # Define input file paths: Unitsfile, Configuration file.
        if (assessmentPeriod == "1877-9999"):
            unitsFileName = path_data+os.sep+"1877-9999/HELCOM_subbasin_with_coastal_WFD_waterbodies_or_watertypes_2022_eutro.shp"
            configurationFileName = path_data+os.sep+"1877-9999/Configuration1877-9999.xlsx"
        elif (assessmentPeriod == "2011-2016"):
            unitsFileName = path_data+os.sep+"2011-2016/AssessmentUnits.shp"
            configurationFileName = path_data+os.sep+"2011-2016/Configuration2011-2016.xlsx"
        elif assessmentPeriod == "2016-2021":
            unitsFileName = path_data+os.sep+"2016-2021/HELCOM_subbasin_with_coastal_WFD_waterbodies_or_watertypes_2022_eutro.shp"
            configurationFileName = path_data+os.sep+"2016-2021/Configuration2016-2021.xlsx"
        else:
            pass # TODO error

        r_args = [assessmentPeriod, unitsFileName, configurationFileName, output_temp_dir, path_intermediate]
        returncode = self.call_r_script('1', r_file_name, path_rscripts, r_args)
        return returncode

        # There are no results, except for the two files that R stores for further use:
        # /.../intermediate/my_gridunits.rds
        # /.../intermediate/my_units.rds
        # and five maps of the Assessment Units:
        # /tmp/.../Assessment_Units.png
        # /tmp/.../Assessment_GridUnits10.png
        # /tmp/.../Assessment_GridUnits30.png
        # /tmp/.../Assessment_GridUnits60.png
        # /tmp/.../Assessment_GridUnits.png
        # TODO Discuss: Not leave as single module? Return the R things as some other format?
        # TODO Discuss: Return maps as GeoJSON? Those are static, could just be downloaded. Unless we allow other assessment units one day.


    def second_r_script(self, r_file_name, path_rscripts, assessmentPeriod, path_data, output_temp_dir, path_intermediate):

        # TODO DISCUSS: Adding user's own data, the user passes a file name?

        # Define input file paths: Samples file
        # TODO DISCUSS: Let user decide which of the three?
        if (assessmentPeriod == "1877-9999"):
            stationSamplesBOTFile = path_data+os.sep+"1877-9999/StationSamples1877-9999BOT_2022-12-09.txt.gz"
            stationSamplesCTDFile = path_data+os.sep+"1877-9999/StationSamples1877-9999CTD_2022-12-09.txt.gz"
            stationSamplesPMPFile = path_data+os.sep+"1877-9999/StationSamples1877-9999PMP_2022-12-09.txt.gz"
        elif (assessmentPeriod == "2011-2016"):
            stationSamplesBOTFile = path_data+os.sep+"2011-2016/StationSamples2011-2016BOT_2022-12-09.txt.gz"
            stationSamplesCTDFile = path_data+os.sep+"2011-2016/StationSamples2011-2016CTD_2022-12-09.txt.gz"
            stationSamplesPMPFile = path_data+os.sep+"2011-2016/StationSamples2011-2016PMP_2022-12-09.txt.gz"
        elif assessmentPeriod == "2016-2021":
            stationSamplesBOTFile = path_data+os.sep+"2016-2021/StationSamples2016-2021BOT_2022-12-09.txt.gz"
            stationSamplesCTDFile = path_data+os.sep+"2016-2021/StationSamples2016-2021CTD_2022-12-09.txt.gz"
            stationSamplesPMPFile = path_data+os.sep+"2016-2021/StationSamples2016-2021PMP_2022-12-09.txt.gz"
        else:
            pass # TODO error

        r_args = [stationSamplesBOTFile, stationSamplesCTDFile, stationSamplesPMPFile, output_temp_dir, path_intermediate]
        returncode = self.call_r_script('2', r_file_name, path_rscripts, r_args)
        return returncode

        # Results:
        # /.../intermediate/my_stationSamples.rds
        # /tmp/.../StationSamplesBOT.csv
        # /tmp/.../StationSamplesCTD.csv
        # /tmp/.../StationSamplesPMP.csv


    def third_r_script(self, r_file_name, path_rscripts, assessmentPeriod, combined_Chlorophylla_IsWeighted, path_data, resultfilepath, path_intermediate):

        # Define input file paths: Config file (indicators)
        if assessmentPeriod == "1877-9999":
            configurationFileName = path_data+os.sep+"1877-9999/Configuration1877-9999.xlsx"
        elif assessmentPeriod == "2011-2016":
            configurationFileName = path_data+os.sep+"2011-2016/Configuration2011-2016.xlsx"
        elif assessmentPeriod == "2016-2021":
            configurationFileName = path_data+os.sep+"2016-2021/Configuration2016-2021.xlsx"
        else:
            pass # TODO error

        r_args = [configurationFileName, str(combined_Chlorophylla_IsWeighted).lower(), resultfilepath, path_intermediate]
        returncode = self.call_r_script('3', r_file_name, path_rscripts, r_args)
        return returncode

        # Results:
        # /.../intermediate/my_wk3.rds
        # /tmp/.../Annual_Indicator.csv






