import time
import subprocess
import datetime
import os
import logging
import sys

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)



assessmentPeriod = "2011-2016"
combined_Chlorophylla_IsWeighted = True
path_data = "/home/mbuurman/work/pyg_helcom/inputs_from_helcom"
path_rscripts = '/home/mbuurman/work/pyg_helcom/pygeoapi/pygeoapi/process/rscripts/'
output_temp_dir = "/tmp/testing/{today}/outputs/".format(today=datetime.datetime.today().strftime('%Y%m%d'))
output_temp_dir = output_temp_dir.rstrip('/')
path_intermediate = "/tmp/testing/{today}/intermediate/".format(today=datetime.datetime.today().strftime('%Y%m%d'))
path_intermediate = path_intermediate.rstrip('/')
if not os.path.exists(path_intermediate):
    os.makedirs(path_intermediate)


'''
In R, need to run:
install.packages('tidyverse')
'''


def call_r_script(num, r_file_name, path_rscripts, r_args, verbose):

    print('')

    LOGGER.debug('Now calling bash which calls R: %s' % r_file_name)
    r_file = path_rscripts.rstrip('/')+os.sep+r_file_name
    cmd = ["/usr/bin/Rscript", "--vanilla", r_file] + r_args
    LOGGER.info(cmd)
    LOGGER.debug('Running command... (Output will be shown once finished)')
    #print(datetime.datetime.now().isoformat())
    #t1 = time.process_time() #does not count the waited time
    t2 = time.perf_counter()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = p.communicate()
    LOGGER.debug("Done running command! Exit code from bash: %s" % p.returncode)
    #elapsed_time1 = time.process_time() - t1
    elapsed_time2 = time.perf_counter() - t2
    #print('ELAPSED TIME: %s sec' % (elapsed_time1))
    print('ELAPSED TIME: %s sec' % (elapsed_time2))

    ### Print stdout and stderr
    stdouttext = stdoutdata.decode()
    stderrtext = stderrdata.decode()
    if len(stderrdata) > 0:
        err_and_out = 'R stdout and stderr:\n___PROCESS OUTPUT {n}___\n___stdout___\n{stdout}\n___stderr___\n{stderr}   (END PROCESS OUTPUT {n})\n___________'.format(
            stdout= stdouttext, stderr=stderrtext, n=num)
    else:
        err_and_out = 'R stdour:\n___PROCESS OUTPUT {n}___\n___stdout___\n{stdout}\n___stderr___\n___(Nothing written to stderr)___\n   (END PROCESS OUTPUT {n})\n___________'.format(
            stdout = stdouttext, n = num)
    if verbose:
        LOGGER.info(err_and_out)
    return p.returncode


def run_one_test(num, r_file_name, r_args, verbose=False):
    code = call_r_script(num, r_file_name, path_rscripts, r_args, verbose)
    if code == 0:
        print("OK: %s" % r_file_name)
        return True
    else:
        print("ERROR: %s" % r_file_name)
        return False


if (assessmentPeriod == "1877-9999"):
    unitsFileName = path_data+os.sep+"1877-9999/HELCOM_subbasin_with_coastal_WFD_waterbodies_or_watertypes_2022_eutro.shp"
    configurationFileName = path_data+os.sep+"1877-9999/Configuration1877-9999.xlsx"
    stationSamplesBOTFile = path_data+os.sep+"1877-9999/StationSamples1877-9999BOT_2022-12-09.txt.gz"
    stationSamplesCTDFile = path_data+os.sep+"1877-9999/StationSamples1877-9999CTD_2022-12-09.txt.gz"
    stationSamplesPMPFile = path_data+os.sep+"1877-9999/StationSamples1877-9999PMP_2022-12-09.txt.gz"
elif (assessmentPeriod == "2011-2016"):
    unitsFileName = path_data+os.sep+"2011-2016/AssessmentUnits.shp"
    configurationFileName = path_data+os.sep+"2011-2016/Configuration2011-2016.xlsx"
    stationSamplesBOTFile = path_data+os.sep+"2011-2016/StationSamples2011-2016BOT_2022-12-09.txt.gz"
    stationSamplesCTDFile = path_data+os.sep+"2011-2016/StationSamples2011-2016CTD_2022-12-09.txt.gz"
    stationSamplesPMPFile = path_data+os.sep+"2011-2016/StationSamples2011-2016PMP_2022-12-09.txt.gz"
elif assessmentPeriod == "2016-2021":
    unitsFileName = path_data+os.sep+"2016-2021/HELCOM_subbasin_with_coastal_WFD_waterbodies_or_watertypes_2022_eutro.shp"
    configurationFileName = path_data+os.sep+"2016-2021/Configuration2016-2021.xlsx"
    stationSamplesBOTFile = path_data+os.sep+"2016-2021/StationSamples2016-2021BOT_2022-12-09.txt.gz"
    stationSamplesCTDFile = path_data+os.sep+"2016-2021/StationSamples2016-2021CTD_2022-12-09.txt.gz"
    stationSamplesPMPFile = path_data+os.sep+"2016-2021/StationSamples2016-2021PMP_2022-12-09.txt.gz"


if __name__ == '__main__':

    print('START')
    print('Reading R scripts from %s' % path_rscripts)
    print('Reading inputs from %s' % path_data)
    print('Writing outputs to %s' % output_temp_dir)
    print('Writing intermediates to %s' % path_intermediate)

    verbose = True
    if verbose:
    	logging.basicConfig(level=logging.DEBUG)

    r_file_name = 'HEAT_subpart1_gridunits.R'
    r_args = [assessmentPeriod, unitsFileName, configurationFileName, output_temp_dir, path_intermediate]
    success = run_one_test('1', r_file_name, r_args, verbose)
    if not success:
        sys.exit()

    r_file_name = 'HEAT_subpart2_stations.R'
    r_args = [stationSamplesBOTFile, stationSamplesCTDFile, stationSamplesPMPFile, output_temp_dir, path_intermediate]
    success = run_one_test('2', r_file_name, r_args, verbose)
    if not success:
        sys.exit()

    r_file_name = 'HEAT_subpart3_wk3.R'
    resultfilepath = output_temp_dir+os.sep+'Annual_Indicator.csv'
    r_args = [configurationFileName, str(combined_Chlorophylla_IsWeighted).lower(), resultfilepath, path_intermediate]
    success = run_one_test('3', r_file_name, r_args, verbose)
    if not success:
        sys.exit()

    r_file_name = 'HEAT_subpart4_wk5_inputread.R'
    resultfilepath = output_temp_dir+os.sep+'Assessment_Indicator.csv'
    r_args = [configurationFileName, path_intermediate, resultfilepath]
    success = run_one_test('4-read', r_file_name, r_args, verbose)
    if not success:
        sys.exit()

    r_file_name = 'HEAT_subpart5_inputread.R'
    resultfilepath = output_temp_dir+os.sep+'Assessment.csv'
    r_args = [configurationFileName, path_intermediate, resultfilepath]
    success = run_one_test('5-read', r_file_name, r_args, verbose)
    if not success:
        sys.exit()

    print(datetime.datetime.now().isoformat())
    print('\nFINISH')


'''
ELAPSED TIME: 4.933181487984257 sec
OK: HEAT_subpart1_gridunits.R

ELAPSED TIME: 7.124233530979836 sec
OK: HEAT_subpart2_stations.R

ELAPSED TIME: 17.1377600459964 sec
OK: HEAT_subpart3_wk3.R

4.93 + 7.12 + 17.13 # 29.18

ELAPSED TIME: 0.8228717499878258 sec
OK: HEAT_subpart4_wk5_inputread.R

ELAPSED TIME: 0.5271131019981112 sec
OK: HEAT_subpart5_inputread.R


'''