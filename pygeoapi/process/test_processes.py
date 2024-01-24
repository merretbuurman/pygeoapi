import time
import subprocess
import datetime
import os
import logging
import sys

try:
    # To be able to run it from the first pygeoapi directory:
    from pygeoapi.process.base import BaseProcessor
except ModuleNotFoundError:
    # To be able to run it inside the process directory:
    sys.path.insert(0, '/home/mbuurman/work/pyg_helcom/pygeoapi/')
    from pygeoapi.process.base import BaseProcessor

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


if __name__ == '__main__':

    print('START')

    os.environ['PYGEOAPI_DATA_DIR'] = '/home/mbuurman/work/pyg_helcom/inputs_from_helcom'
    os.environ['R_SCRIPT_DIR'] = '/home/mbuurman/work/pyg_helcom/pygeoapi/pygeoapi/process/rscripts'

    print('Reading R scripts from %s' % os.environ['R_SCRIPT_DIR'])
    print('Reading inputs from %s' % os.environ['PYGEOAPI_DATA_DIR'])
    #print('Writing outputs to %s' % output_temp_dir)
    print('Writing outputs to %s' % 'some random directory in /tmp...')
    #print('Writing intermediates to %s' % path_intermediate)
    print('Writing intermediates to %s (hard-coded...)' % '/tmp/intermediate')


    ### A
    if True:
        #print('\nNow creating my custom process object:')
        import pygeoapi.process.pypro_a
        process_a = pygeoapi.process.pypro_a.HELCOMAnnualIndicatorProcessor({'name': 'bla'})

        print('\nNow executing my custom process: HELCOMAnnualIndicatorProcessor')
        t0 = time.perf_counter()
        returned = process_a.execute({'assessmentPeriod' : '2011-2016', 'combined_Chlorophylla_IsWeighted': 'True'})
        print('ELAPSED TIME: %s sec' % (time.perf_counter() - t0))
        if type(returned) == type((2,2)) and returned[0] == 'text/csv':
            print('OK!')
            #print('CSV was returned: %s...' % returned[1][:120])
        else:
            print('ERROR! Return: %s' % returned)

    ### B
    if True:
        #print('\nNow creating my custom process object:')
        import pygeoapi.process.pypro_b_inputread
        process_c = pygeoapi.process.pypro_b_inputread.HELCOMAssessmentIndicatorReadProcessor({'name': 'bla'})

        print('\nNow executing my custom process: HELCOMAssessmentIndicatorReadProcessor')
        t0 = time.perf_counter()
        returned = process_c.execute({'assessmentPeriod' : '2011-2016'})
        print('ELAPSED TIME: %s sec' % (time.perf_counter() - t0))
        if type(returned) == type((2,2)) and returned[0] == 'text/csv':
            print('OK!')
            #print('CSV was returned: %s...' % returned[1][:120])
        else:
            print('ERROR! Return: %s' % returned)

    ### C
    if True:
        #print('\nNow creating my custom process object:')
        import pygeoapi.process.pypro_c_inputread
        process_c = pygeoapi.process.pypro_c_inputread.HELCOMAssessmentReadProcessor({'name': 'bla'})

        print('\nNow executing my custom process: HELCOMAssessmentReadProcessor')
        t0 = time.perf_counter()
        returned = process_c.execute({'assessmentPeriod' : '2011-2016'})
        print('ELAPSED TIME: %s sec' % (time.perf_counter() - t0))
        if type(returned) == type((2,2)) and returned[0] == 'text/csv':
            print('OK!')
            #print('CSV was returned: %s...' % returned[1][:120])
        else:
            print('ERROR! Return: %s' % returned)

    print('\nFINISH')

'''
Now creating my custom process object:
Now executing my custom process:
ELAPSED TIME: 29.94480521499645 sec

Now creating my custom process object:
Now executing my custom process:
ELAPSED TIME: 0.6529775310191326 sec

Now creating my custom process object:
Now executing my custom process:
ELAPSED TIME: 0.5230650909943506 sec

'''