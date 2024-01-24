import requests
import time
import logging
import sys


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


# If the user passed a username and password, use prod settings!
dev = True
if len(sys.argv) > 1:
    dev = False


# Access to server:
if dev:
    url = 'http://130.225.37.27:5000/'
    user = None
    pw = None
else:
    url = 'https://130.225.37.27/pygeoapi/'
    user = sys.argv[1]
    pw = sys.argv[2]


# Function to call process:
def run_one(url, process_id, args, username=None, password=None, verify=False):
    #curl -X POST "http://130.225.37.27:5000/processes/assessment-read/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"assessmentPeriod\": \"2011-2016\"}}"

    base_url = url.rstrip('/')

    h = {'accept': 'application/json', 'Content-Type': 'application/json'}
    '''
    body = {
        "inputs":{
            "assessmentPeriod" : args.assessment_period,
            "combined_Chlorophylla_IsWeighted": args.combined_chlorophylla_is_weighted
        }
    }
    '''
    body = {"inputs":args}
    LOGGER.debug('This payload will be sent: %s' % body)

    url = '{url}/processes/{process_id}/execution'.format(url=url, process_id=process_id)
    LOGGER.debug('This URL will be queried: %s' % url)

    print('\nNow executing process "{process}" on server {url}'.format(process = process_id, url = base_url))
    t0 = time.perf_counter()
    if username is not None and password is not None:
        LOGGER.debug('With username and password!')
        resp = requests.post(url, headers=h, json=body, verify=verify, auth=(username, password))
    else:
        LOGGER.debug('Without username and password!')
        resp = requests.post(url, headers=h, json=body, verify=verify)

    print('ELAPSED TIME: %s sec' % (time.perf_counter() - t0))
    if resp.status_code == 200:
        print('OK! HTTP 200!')
    else:
        print('ERROR! HTTP {http}, RESPONSE: {content}'.format(http=resp.status_code, content=resp.content))




if __name__ == '__main__':

    print('START')

    ### A
    process_id = 'annual-indicator'
    args = {'assessmentPeriod' : '2011-2016', 'combined_Chlorophylla_IsWeighted': 'True'}
    run_one(url, process_id, args, user, pw)

    ### B
    process_id = 'assessment-indicator-read'
    args = {'assessmentPeriod' : '2011-2016'}
    run_one(url, process_id, args, user, pw)

    ### C
    process_id = 'assessment-read'
    args = {'assessmentPeriod' : '2011-2016'}
    run_one(url, process_id, args, user, pw)

print('\nFINISH')