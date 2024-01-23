
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
The whole process does not work yet, as the read input is not the same as the original R-created input!

# Post dummy data, does not work:
curl -X POST "http://130.225.37.27:5000/processes/assessment-posted/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"assessment_indicators_csv\": \"abc;def;ghi\"}}"

# Save result to file:
curl -X POST -o /tmp/Assessment.csv "http://130.225.37.27:5000/processes/assessment-posted/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"assessment_indicators_csv\": \"abc;def;ghi\"}}"

# Read the data to be posted from file (works):
curl -X POST "http://130.225.37.27:5000/processes/assessment-posted/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"assessment_indicators_csv\": \"$(awk '{printf "%s\\n", $0}' /tmp/Assessment_Indicator.csv)\"}}"

# Pass the data directly (works), comma-separated and with \n as line break:
curl -X POST "http://130.225.37.27:5000/processes/assessment-posted/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\", \"assessment_indicators_csv\": \"IndicatorID,CriteriaID,Name,Code,Parameters,Units,YearMin,YearMax,MonthMin,MonthMax,DepthMin,DepthMax,Metric,Response,Applied,Comments,GTC_HM,GTC_ML,STC_HM,STC_ML,SSC_HM,SSC_ML,UnitID,ET,ACDEV,EQR_HG,EQR_GM,EQR_MP,EQR_PB,IW,NSTC100,Period,ES,SD,ER,EQR,EQRS,N,N_OBS,GTC,STC,SSC,EQRS_Class,TC,TC_Class,SC,SC_Class,AC_SE,AC_NPA,AC_PA,AC,ACC,ACC_Class,C,C_Class
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,1,5,50,,,,,25,6,20112016,5.90477244354825,0.611463635163127,1.18095448870965,0.569572690686614,0.462926151557573,6,273,100,100,100,Moderate,100,High,100,High,0.0370074627226476,2.61814578460048e-132,1,1,100,High,100,High
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,2,5,50,,,,,25,5,20112016,6.38332506613757,0.557018546000908,1.27666501322751,0.525382383482524,0.40053983550474,6,45,50,100,0,Moderate,75,High,0,Low,0.0830354222390749,1.2917917372263e-62,1,1,100,High,58.3333333333333,Moderate
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,3,3.3,50,,,,,25,6,20112016,6.02226080246914,0.578965864897596,1.82492751589974,0.368042634110132,0.197288457599592,6,50,50,100,0,Bad,75,High,0,Low,0.0818781378289249,1.10138193566272e-242,1,1,100,High,58.3333333333333,Moderate
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,4,5.5,50,,,,,25,4,20112016,5.97503191785974,0.923422157651498,1.08636943961086,0.625333635184626,0.541647484966531,6,93,83.3333333333333,100,91.6666666666667,Moderate,91.6666666666667,High,91.6666666666667,High,0.0957544171328347,3.5076430806531e-07,0.999999649235692,0.999999649235692,100,High,94.4444444444444,High
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,5,4.3,50,,,,,25,3,20112016,6.40546909834899,0.432734583585453,1.48964397636023,0.449199102627297,0.292986968415008,6,157,100,100,91.6666666666667,Poor,100,High,91.6666666666667,High,0.0345359795615108,0,1,1,100,High,97.2222222222222,High
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,6,2.9,50,,,,,25,6,20112016,3.92083611838073,0.317232747557995,1.35201245461405,0.495858885333909,0.358859602824342,6,221,100,100,100,Poor,100,High,100,High,0.0213393838646964,0,1,1,100,High,100,High
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,7,1.8,50,,,,,25,6,20112016,8.99441479084224,6.89111960139595,4.99689710602347,0.179230631655511,0.0935116339072229,6,187,100,100,58.3333333333333,Bad,100,High,58.3333333333333,Moderate,0.50392857358628,1.52967025814995e-46,1,1,100,High,86.1111111111111,High
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,8,5.53,50,,,,,25,,20112016,4.59605324074074,2.46755764314724,0.831112701761436,0.752471645805541,0.697607029372529,6,32,41.6666666666667,0,41.6666666666667,Good,20.8333333333333,Low,41.6666666666667,Low,0.436206685609527,0.983865591674703,0.0161344083252966,0.983865591674703,100,High,54.1666666666667,Moderate
1,1,Dissolved Inorganic Nitrogen,DIN,NTRA+NTRI+AMON,umol/l,2011,2016,12,2,0,10,Mean,1,1,,15,5,0,2,70,50,9,4.2,50,,,,,20,6,20112016,3.54567445620223,0.86305625361227,0.844208203857675,0.806587476654272,0.774203446708665,6,139,83.3333333333333,100,33.3333333333333,Good,91.6666666666667,High,33.3333333333333,Low,0.0732034760004996,1,0,1,100,High,75,High\"}}"

'''


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'assessment-posted',
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
        },
        'assessment_indicators_csv': {
            'title': 'Assessment Indicators',
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

class HELCOMAssessmentPostedProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<HELCOMAssessmentPostedProcessor> {self.name}'


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

        # Get input:
        assessmentPeriod = data.get('assessmentPeriod')
        assessment_indicators_csv = data.get('assessment_indicators_csv')

        # Define output path for this run:
        randomstring = (''.join(random.sample(string.ascii_lowercase+string.digits, 6)))
        output_temp_dir = tempfile.gettempdir()+os.sep+randomstring
        if not os.path.exists(output_temp_dir):
            os.makedirs(output_temp_dir)

        # Add the received input to that dir:
        input_temp_path = output_temp_dir+os.sep+'AssessmentIndicators.csv'
        with open(input_temp_path, 'w') as inputfile:
            inputfile.write(assessment_indicators_csv)
            # TODO Clean up csv files!

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

        r_file_name = 'HEAT_subpart5_inputposted.R'
        LOGGER.info('Now calling bash which calls R: %s' % r_file_name)
        r_file = path_rscripts.rstrip('/')+os.sep+r_file_name
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
            resultfilepath = output_temp_dir+os.sep+'Assessment.csv'
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





