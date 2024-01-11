
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
# Post dummy data, does not work:
curl -X POST "http://130.225.37.27:5000/processes/assessment-indicator-posted/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"annual_indicators_csv\": \"abc;def;ghi\"}}"

# Save result to file:
curl -X POST -o /tmp/Assessment_Indicator.csv "http://130.225.37.27:5000/processes/assessment-indicator-posted/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"annual_indicators_csv\": \"abc;def;ghi\"}}"

# Read the data to be posted from file (DOES NOT WORK YET):
curl -X POST "http://130.225.37.27:5000/processes/assessment-indicator-posted/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"annual_indicators_csv\": \"$(cat /tmp/Annual_Indicator.csv)\"}}"

# Pass the data directly (NOT TESTED YET):
curl -X POST "http://130.225.37.27:5000/processes/assessment-indicator-posted/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"annual_indicators_csv\": \"UnitID,IndicatorID,CriteriaID,Name,Code,Parameters,Units,YearMin,YearMax,MonthMin,MonthMax,DepthMin,DepthMax,Metric,Response,Applied,Comments,GTC_HM,GTC_ML,STC_HM,STC_ML,SSC_HM,SSC_ML,ET,ACDEV,EQR_HG,EQR_GM,EQR_MP,EQR_PB,IW,Period,ES,SD,N,NM,GridArea,EQR,EQRS,GTC,STC,SSC,NMP,UnitArea,SE,CI,ER,BEST,W,EQRS_Class
1,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2011,5.05982456140351,1.91564617523428,38,3,14618863581.4494,0.658784369474013,0.588872051022136,100,100,100,3,15671688012.3213,0.310758844930215,0.60907614394049,1.0119649122807,3.33333333333333,,Moderate
1,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2012,5.61457142857143,2.90757429678358,35,3,14618863581.4494,0.593693281088325,0.496978749771753,100,100,100,3,15671688012.3213,0.491469757573183,0.963263024334071,1.12291428571429,3.33333333333333,,Moderate
1,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2013,6.06353741496599,1.57517274304946,49,3,14618863581.4494,0.549734108196648,0.434918740983503,100,100,100,3,15671688012.3213,0.225024677578494,0.441040263686586,1.2127074829932,3.33333333333333,,Moderate
1,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2014,5.62387596899225,1.62113062809589,43,3,14618863581.4494,0.592711032695595,0.495592046158487,100,100,100,3,15671688012.3213,0.24722010310702,0.484542498344038,1.12477519379845,3.33333333333333,,Moderate
1,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2015,6.83073333333333,1.94519225801331,50,3,15172550637.862,0.487990552502904,0.347751368239393,100,100,100,3,15671688012.3213,0.275091727270557,0.539169877895206,1.36614666666667,3.33333333333333,,Poor
1,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2016,6.23609195402299,2.35986619850086,58,3,14618863581.4494,0.5345228001622,0.413443953170165,100,100,100,3,15671688012.3213,0.309865612551382,0.607325440648152,1.2472183908046,3.33333333333333,,Moderate
2,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2011,5.84285714285714,1.2918138677004,7,3,648133374.519156,0.570497147514263,0.464231267078959,50,100,0,3,1944163057.42455,0.488259747731394,0.956971520654143,1.16857142857143,3.33333333333333,,Moderate
2,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2012,6.93888888888889,1.14298180537024,6,2,895184070.997962,0.480384307445957,0.337013139923704,50,50,0,3,1944163057.42455,0.466620368073699,0.914559115877274,1.38777777777778,3.33333333333333,,Poor
2,1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,50,0.808333333333333,0.666666666666667,0.525,0.383333333333333,25,2013,5.97619047619048,1.58367999546699,7,3,895184070.997962,0.557768924302789,0.446262010780408,50,100,0,3,1944163057.42455,0.598574774901937,1.17318500086197,1.1952380952381,3.33333333333333,,Moderate\"}}"

'''


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'assessment-indicator-posted',
    'title': {'en': 'HELCOM Assessment Indicator'},
    'description': {
        'en': 'Process to compute the HELCOM Assessment Indicator for the HEAT assessment tool.'
              ' This process represents subpart 2 of the original HEAT assessment computation script.'
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
        'annual_indicators_csv': {
            'title': 'Annual Indicators',
            'description': 'From last process.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'text/csv'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['BLA']
        }
    },
    'outputs': {
        'verbal_result': {
            'title': 'Assessment Indicator CSV',
            'description': 'Assessment Indicator CSV (Assessment_Indicator.csv), ask HELCOM.',
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

class HELCOMAssessmentIndicatorPostedProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<HELCOMAssessmentIndicatorPostedProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting HEAT_subparts4_wk4_b.R as ogc_service!"')
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
        annual_indicators_csv = data.get('annual_indicators_csv')

        # Define output path for this run:
        randomstring = (''.join(random.sample(string.ascii_lowercase+string.digits, 6)))
        output_temp_dir = tempfile.gettempdir()+os.sep+randomstring
        if not os.path.exists(output_temp_dir):
            os.makedirs(output_temp_dir)

        # Add the received input to that dir:
        input_temp_path = output_temp_dir+os.sep+'AnnualIndicators.csv'
        with open(input_temp_path, 'w') as inputfile:
            inputfile.write(annual_indicators_csv)
            # TODO Clean up csv files!

        ########################
        ### Call R Script 1: ###
        ########################
        # I think it takes the map of the assessment units and makes grid units. What for?
    
        # Define input file path: Configuration file.
        if (assessmentPeriod == "1877-9999"):
            configurationFileName = "1877-9999/Configuration1877-9999.xlsx"
        elif (assessmentPeriod == "2011-2016"):
            configurationFileName = "2011-2016/Configuration2011-2016.xlsx"
        elif assessmentPeriod == "2016-2021":
            configurationFileName = "2016-2021/Configuration2016-2021.xlsx"
        else:
            pass # TODO error

        #r_file_name = 'HEAT_subpart4_wk4_wk5.R'
        r_file_name = 'HEAT_subpart4_wk5_inputposted.R'
        LOGGER.info('Now calling bash which calls R: %s' % r_file_name)
        LOGGER.debug('Current directory: %s' % os.getcwd())
        r_file = '/home/ubuntu/'+r_file_name
        cmd = ["/usr/bin/Rscript", "--vanilla", r_file, configurationFileName, input_temp_path, output_temp_dir]
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





