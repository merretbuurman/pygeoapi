#!/usr/bin/env python3

import json
import os
import pandas
import geopandas
import logging
LOGGER = logging.getLogger(__name__)
LOGGER.info('Pandas version: ', pandas.__version__)
LOGGER.info('Geopandas version: ', geopandas.__version__)


# Define data # TODO Read from env var or so? What is the best way... Hard coding is not!
PYGEOAPI_DATA_DIR = '/home/mbuurman/work/hydro_casestudy_saofra/data/'

# Get PYGEOAPI_DATA_DIR from environment:
#if not 'PYGEOAPI_DATA_DIR' in os.environ:
#    err_msg = 'ERROR: Missing environment variable PYGEOAPI_DATA_DIR. We cannot find the input data!\nPlease run:\nexport PYGEOAPI_DATA_DIR="/.../"'
#    LOGGER.error(err_msg)
#    LOGGER.error('Exiting...')
#    sys.exit(1) # This leads to curl error: (52) Empty reply from server. TODO: Send error message back!!!
#path_data = os.environ.get('PYGEOAPI_DATA_DIR')
path_data = os.environ.get('PYGEOAPI_DATA_DIR', PYGEOAPI_DATA_DIR)


def _get_basin_polygon(data_dir, basin_id='481051'):
    datapath = data_dir.rstrip('/')+'/basin_%s/basin_%s.gpkg' % (basin_id, basin_id)
    #gdf_basin = geopandas.read_file(datapath_basin, layer='somelayername') # TODO Which layer?
    gdf = geopandas.read_file(datapath)
    return gdf
