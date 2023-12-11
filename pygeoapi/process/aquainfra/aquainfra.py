#!/usr/bin/env python3

import json
import os
import pandas
import geopandas
import logging
LOGGER = logging.getLogger(__name__)
LOGGER.info('Pandas version: %s' % pandas.__version__)
LOGGER.info('Geopandas version: %s' % geopandas.__version__)


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


def _read_dam_data_FHReD(data_dir):
    # https://geopandas.org/en/stable/gallery/create_geopandas_from_pandas.html
    datapath = data_dir.rstrip('/')+'/downloaded_dam_data/FHReD_2015_future_dams_Zarfl_et_al_beta_version/FHReD_2015_future_dams_Zarfl_et_al_beta_version.csv'
    df = pandas.read_csv(datapath, sep=';')
    geom = geopandas.points_from_xy(df.Lon_Cleaned, df.LAT_cleaned)
    gdf = geopandas.GeoDataFrame(df, geometry=geom, crs="EPSG:4326") # TODO CRS WGS84 is my assumption!
    return gdf


def _read_dam_data_GRanD_dams_v1_3(data_dir):
    gdf = geopandas.read_file(data_dir.rstrip('/')+"/downloaded_dam_data/GRanD_Version_1_3/GRanD_dams_v1_3.shp")
    #print(gdf) # This object has lots of useful GIS functions!
    return gdf


def _spatial_filter(gdf_points, gdf_polygon):
    point_in_polygon = gdf_points.within(gdf_polygon.loc[0, 'geometry'])
    #print('point_in_polygon: %s' % point_in_polygon)
    gdf_filtered = gdf_points.loc[point_in_polygon].copy()
    return gdf_filtered
    # TODO Filter by bbox --> Can be done by this function, but we'd need a pygeoapi process that allows to specify BBox!


def _filter_by_value(gdf, filter_column, filter_value, reverse_filter):

    # Filter by string:
    if filter_column is not None and filter_value is not None:
        if reverse_filter:
            print('(Reverse-filtering...)')
            # Keep all values that are NOT like the filter_value
            gdf = gdf[shapefile[filter_column] != filter_value]
        else:
            # Keep all values that are LIKE the filter_value
            print('(Positive filtering...)')
            gdf = gdf[gdf[filter_column] == filter_value]

    # TODO Filter by several strings (merge shapefiles?)
    # TODO Filter by string, with NOT?
    # TODO Filter by math operator

    return gdf




###
### Run the stuff for testing!
###


if __name__ == '__main__':

    print('PYGEOAPI_DATA_DIR: %s' % PYGEOAPI_DATA_DIR)


    # Get polygon
    print('\nSTEP 1')
    print('Loading geopackage with drainage basin...')
    gdf_basin_polygon = _get_basin_polygon(PYGEOAPI_DATA_DIR, basin_id='481051')
    print('Loading geopackage with drainage basin... done')


    # Dam shapefile
    print('\nSTEP 2')
    print('Loading shapefile with dams...')
    # Define parameters # TODO argparse
    FILTER_COLUMN = 'QUALITY'
    FILTER_VALUE = '4: Poor'
    REVERSE_FILTER = False
    #FILTER_VALUES = ['4: Poor', '2: Good']
    gdf_grand = _read_dam_data_GRanD_dams_v1_3(PYGEOAPI_DATA_DIR)
    print('Loading shapefile with dams... done. Found %s shapes...' % len(gdf_grand))
    print('Filtering shapefile with dams by value: %s=%s...' % (FILTER_COLUMN, FILTER_VALUE))
    gdf_grand = _filter_by_value(gdf_grand, FILTER_COLUMN, FILTER_VALUE, REVERSE_FILTER)
    print('Filtering shapefile with dams... done.')
    print('After filtering, %s objects left...' % len(gdf_grand))
    print('Filtering dams by drainage basin...')
    gdf_grand = _spatial_filter(gdf_grand, gdf_basin_polygon)
    print('Filtering dams by drainage basin... done')
    print('After filtering, %s shapes left...' % len(gdf_grand))


    # The second dam thingy:
    print('\nSTEP 3')
    gdf_fhred = _read_dam_data_FHReD(PYGEOAPI_DATA_DIR)
    print('Filtering dams by drainage basin...')
    gdf_fhred = _spatial_filter(gdf_fhred, gdf_basin_polygon)
    print('Filtering dams by drainage basin... done')
    print('After filtering, %s objects left...' % len(gdf_fhred))
    print(gdf_fhred.head())
    print('\nDONE.')


    #print(json.loads(gdf_grand.to_json()))
    #print(json.loads(gdf_fhred.to_json()))
    #print(json.loads(gdf_basin_polygon.to_json()))




'''

7320 Dams
59 Properties

[7320 rows x 59 columns]

Column names:
 'GRAND_ID', 'RES_NAME', 'DAM_NAME', 'ALT_NAME', 'RIVER', 'ALT_RIVER',
 'MAIN_BASIN', 'SUB_BASIN', 'NEAR_CITY', 'ALT_CITY', 'ADMIN_UNIT',
 'SEC_ADMIN', 'COUNTRY', 'SEC_CNTRY', 'YEAR', 'ALT_YEAR', 'REM_YEAR',
 'DAM_HGT_M', 'ALT_HGT_M', 'DAM_LEN_M', 'ALT_LEN_M', 'AREA_SKM',
 'AREA_POLY', 'AREA_REP', 'AREA_MAX', 'AREA_MIN', 'CAP_MCM', 'CAP_MAX',
 'CAP_REP', 'CAP_MIN', 'DEPTH_M', 'DIS_AVG_LS', 'DOR_PC', 'ELEV_MASL',
 'CATCH_SKM', 'CATCH_REP', 'DATA_INFO', 'USE_IRRI', 'USE_ELEC',
 'USE_SUPP', 'USE_FCON', 'USE_RECR', 'USE_NAVI', 'USE_FISH', 'USE_PCON',
 'USE_LIVE', 'USE_OTHR', 'MAIN_USE', 'LAKE_CTRL', 'MULTI_DAMS',
 'TIMELINE', 'COMMENTS', 'URL', 'QUALITY', 'EDITOR', 'LONG_DD', 'LAT_DD',
 'POLY_SRC', 'geometry'

>>> print(shapefile)
      GRAND_ID     RES_NAME    DAM_NAME     ALT_NAME       RIVER    ALT_RIVER  ...      QUALITY     EDITOR     LONG_DD     LAT_DD POLY_SRC   geometry
0      1   None Terror Lake   None      Terror River  Marmont Bay  ...      3: Fair  UNH -153.026649  57.651485      NHD  POINT (-153.02665 57.65149)
1      2   None  Mayo   None  Mayo   None  ...  1: Verified  McGill-BL -135.367048  63.774184   CanVec  POINT (-135.36705 63.77418)
2      3   None   Blue Lake   None     Sawmill Creek   None  ...      2: Good  UNH -135.200305  57.062136      NHD  POINT (-135.20030 57.06214)
3      4   None  Green Lake   None     Vodopad River   None  ...      2: Good  UNH -135.112812  56.986785      NHD  POINT (-135.11281 56.98679)
4      5    Long Lake     Long Lake Dam     Snettisham Dam  Long River   None  ...      4: Poor  UNH -133.728413  58.168750      NHD  POINT (-133.72841 58.16875)
...  ...    ...   ...    ...   ...    ...  ...    ...  ...   ...  ...      ...        ...
7315      7316  Sardis Lake Sardis Lake  Sardis Lake    Jackfork Creek   None  ...      2: Good  McGill-PB  -95.350711  34.629812      JRC   POINT (-95.35071 34.62981)
7316      7317     Tims Ford Lake   Tims Ford     Tims Ford Lake   Elk River   None  ...      2: Good  McGill-PB  -86.276254  35.197234      JRC   POINT (-86.27625 35.19723)
7317      7318  Cordell Hull Lake  Cordell Hull Dam  Cordell Hull Lake  Cumberland River   None  ...      2: Good  McGill-PB  -85.944131  36.292220      JRC   POINT (-85.94413 36.29222)
7318      7319   None      Merwin    Ariel Dam Lewis River   None  ...      2: Good  McGill-PB -122.555376  45.957492      JRC  POINT (-122.55538 45.95749)
7319      7320   None  Twin Lakes   None  Twin   None  ...      3: Fair  McGill-PB  -89.168587  46.023717     SWBD   POINT (-89.16859 46.02372)

[7320 rows x 59 columns]
'''