import random
import string
import subprocess
import os
import tempfile
import pandas
import geopandas
import logging
LOGGER = logging.getLogger(__name__)


# Define data # TODO Read from env var or so? What is the best way... Hard coding is not!
# TODO: Same for R-Scripts!
PYGEOAPI_DATA_DIR = '/home/mbuurman/work/hydro_casestudy_saofra/data/'

# Get PYGEOAPI_DATA_DIR from environment:
#if not 'PYGEOAPI_DATA_DIR' in os.environ:
#    err_msg = 'ERROR: Missing environment variable PYGEOAPI_DATA_DIR. We cannot find the input data!\nPlease run:\nexport PYGEOAPI_DATA_DIR="/.../"'
#    LOGGER.error(err_msg)
#    LOGGER.error('Exiting...')
#    sys.exit(1) # This leads to curl error: (52) Empty reply from server. TODO: Send error message back!!!
#path_data = os.environ.get('PYGEOAPI_DATA_DIR')
path_data = os.environ.get('PYGEOAPI_DATA_DIR', PYGEOAPI_DATA_DIR)


def csv_coordinates_to_geodataframe(csv_file_path, col_name_lon, col_name_lat, sep, remove_temp_file=True):
    '''
    The CSV file must contain the columns "longitude" and "latitude".

    Written for get-species-data.
    get_species_data.r writes its output into a CSV file, which needs to be converted to geodatagrame
    for further usage by pygeoapi.

    Example input file:
    X,Y,occurence_id,longitude,latitude,species,occurrenceStatus,country,year
    -44.885825,-17.25355,"1",-44.885825,-17.25355,Conorhynchos conirostris,PRESENT,Brazil,"2021"
    -43.595833,-13.763611,"2",-43.595833,-13.763611,Conorhynchos conirostris,PRESENT,Brazil,"2020"
    -45.780827,-17.178306,"4",-45.780827,-17.178306,Conorhynchos conirostris,PRESENT,Brazil,"2018"
    -45.904444,-17.074722,"5",-45.904444,-17.074722,Conorhynchos conirostris,PRESENT,Brazil,"2014"
    -46.893511,-17.397917,"6",-46.893511,-17.397917,Conorhynchos conirostris,PRESENT,Brazil,"2012"
    -45.245,-18.136,"7",-45.245,-18.136,Conorhynchos conirostris,PRESENT,Brazil,"2009"
    -45.24,-18.14,"8",-45.24,-18.14,Conorhynchos conirostris,PRESENT,Brazil,"2009"
    -43.138889,-11.092222,"9",-43.138889,-11.092222,Conorhynchos conirostris,PRESENT,Brazil,"2001"
    -44.954661,-17.356778,"10",-44.954661,-17.356778,Conorhynchos conirostris,PRESENT,Brazil,"1998"
    -43.966667,-14.916667,"11",-43.966667,-14.916667,Conorhynchos conirostris,PRESENT,Brazil,"1998"
    -44.954963,-17.336154,"12",-44.954963,-17.336154,Conorhynchos conirostris,PRESENT,Brazil,"1942"
    -44.357,-15.49277,"13",-44.357,-15.49277,Conorhynchos conirostris,PRESENT,Brazil,"1932"
    -44.357,-15.492768,"14",-44.357,-15.492768,Conorhynchos conirostris,PRESENT,Brazil,"1907"
    '''

    LOGGER.debug('Reading result coordinates from file "%s"...' % csv_file_path)

    # Load csv into pandas dataframe:
    df = pandas.read_csv(csv_file_path, sep=sep)

    # Load pandas dataframe geopandas geodataframe:
    #gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.longitude, df.latitude))
    #gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df['longitude'], df['latitude']))
    gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df[col_name_lon], df[col_name_lat]))

    # Remove file - not during debugging, but in production we should probably
    if remove_temp_file and os.path.exists(csv_file_path):
        os.remove(csv_file_path)
        LOGGER.debug('Intermediate file "%s" has been removed.' % csv_file_path)
    
    LOGGER.debug('Reading result coordinates from file "%s"... done.' % csv_file_path)
    return gdf


def file_coordinates_to_geojson_old(csv_file_path, remove_temp_file=True):
    # TODO Improve reading csv from file! And writing to GeoJSON

    LOGGER.debug('Reading result coordinates from file "%s"...' % csv_file_path)
    first_line = True
    output_list = []
    with open(csv_file_path, mode='r') as myfile:
        for line in myfile:
            if first_line:
                first_line = False
                continue
            line = line.strip().split(',')
            west, south = line[0], line[1]
            LOGGER.debug('  West: %s, South: %s' % (west, south))
            output_list.append([float(west), float(south)])

    # Remove file - not during debugging, but in production we should probably
    if remove_temp_file and os.path.exists(temp_dir_path):
        os.remove(temp_dir_path)
        LOGGER.debug('Intermediate file "%s" has been removed.' % temp_dir_path)
    
    LOGGER.debug('Reading result coordinates from file "%s"... done.' % csv_file_path)

    output_as_geojson = {"type": "MultiPoint", "coordinates": output_list}
    return output_as_geojson



def _get_output_temp_file(name):
    randomstring = (''.join(random.sample(string.ascii_lowercase+string.digits, 5)))
    temp_dir_path =  tempfile.gettempdir()+os.sep+'__output_'+name+'tool_'+randomstring+'.csv' # intermediate result storage used by R!
    return temp_dir_path


def _get_input_temp_file(name):
    randomstring = (''.join(random.sample(string.ascii_lowercase+string.digits, 5)))
    temp_dir_path =  tempfile.gettempdir()+os.sep+'__input_'+name+'tool_'+randomstring+'.csv' # intermediate result storage used by R!
    return temp_dir_path


def _get_dir_scripts():
    LOGGER.debug('Current directory: %s' % os.getcwd())
    if __name__ == '__main__':
        return os.getcwd().rstrip('/')

    return os.getcwd()+'/pygeoapi/process/aquainfra/'.rstrip('/')


def get_species_data(species_name, basin_id, data_dir):
    # TODO: Return CSV or geodataframe?

    # Call R script, it writes the results to CSV file:
    csv_file_path = _get_species_data_to_csv(species_name, basin_id, data_dir)
    return csv_file_path

    # Read them from CSV file into geodatagrame, which we return:
    #result_gdf = _csv_coordinates_to_geodataframe(csv_file_path, remove_temp_file=True)
    #return result_gdf


def _get_species_data_to_csv(species_name, basin_id, data_dir):

    # Call R Script:
    #How is it called from bash?
    #/usr/bin/Rscript --vanilla /home/mbuurman/work/trying_out_hydrographr/get_species_data.R "/tmp/species.csv" "Conorhynchos conirostris" "/home/mbuurman/work/hydro_casestudy_saofra/data/basin_481051/basin_481051.gpkg"
    
    csv_file_path = _get_output_temp_file('getspeciesdata')

    # input file:
    polygon_inputfile = "%s/basin_%s/basin_%s.gpkg" % (data_dir.rstrip('/'), basin_id, basin_id)

    # call r script:
    r_file = '/get_species_data.r'
    r_path = _get_dir_scripts()+r_file
    cmd = ["/usr/bin/Rscript", "--vanilla", r_path, csv_file_path, species_name, polygon_inputfile]
    LOGGER.info('Running R script "%s"...' % r_file)
    LOGGER.debug(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = p.communicate()
    LOGGER.debug('Running R script "%s"... done.' % r_file)

    # Get return code and output
    LOGGER.info('R process exit code: %s' % p.returncode)
    stdouttext = stdoutdata.decode()
    stderrtext = stderrdata.decode()
    if len(stderrdata) > 0:
        err_and_out = '___PROCESS OUTPUT___\n___stdout___\n%s\n___stderr___\n%s\n___END___' % (stdouttext, stderrtext)
    else:
        err_and_out = '___PROCESS OUTPUT___\n___stdout___\n%s\n___(Nothing written to stderr)___\n___END___' % stdouttext
    LOGGER.info(err_and_out)

    return csv_file_path

    '''
    X,Y,occurence_id,longitude,latitude,species,occurrenceStatus,country,year
    -44.885825,-17.25355,"1",-44.885825,-17.25355,Conorhynchos conirostris,PRESENT,Brazil,"2021"
    -43.595833,-13.763611,"2",-43.595833,-13.763611,Conorhynchos conirostris,PRESENT,Brazil,"2020"
    -40.492972,-12.948389,"3",-40.492972,-12.948389,Conorhynchos conirostris,PRESENT,Brazil,"2020"
    -45.780827,-17.178306,"4",-45.780827,-17.178306,Conorhynchos conirostris,PRESENT,Brazil,"2018"
    -45.904444,-17.074722,"5",-45.904444,-17.074722,Conorhynchos conirostris,PRESENT,Brazil,"2014"
    -46.893511,-17.397917,"6",-46.893511,-17.397917,Conorhynchos conirostris,PRESENT,Brazil,"2012"
    -45.245,-18.136,"7",-45.245,-18.136,Conorhynchos conirostris,PRESENT,Brazil,"2009"
    -45.24,-18.14,"8",-45.24,-18.14,Conorhynchos conirostris,PRESENT,Brazil,"2009"
    -43.138889,-11.092222,"9",-43.138889,-11.092222,Conorhynchos conirostris,PRESENT,Brazil,"2001"
    -44.954661,-17.356778,"10",-44.954661,-17.356778,Conorhynchos conirostris,PRESENT,Brazil,"1998"
    -43.966667,-14.916667,"11",-43.966667,-14.916667,Conorhynchos conirostris,PRESENT,Brazil,"1998"
    -44.954963,-17.336154,"12",-44.954963,-17.336154,Conorhynchos conirostris,PRESENT,Brazil,"1942"
    -44.357,-15.49277,"13",-44.357,-15.49277,Conorhynchos conirostris,PRESENT,Brazil,"1932"
    -44.357,-15.492768,"14",-44.357,-15.492768,Conorhynchos conirostris,PRESENT,Brazil,"1907"
    '''


###
### Snap to network
###

def geojson_to_csv(multipoint):

    # Step 1: GeoJSON to GeoDataFrame
    gdf = multipoint_to_geodataframe(multipoint)

    input_coord_file_path = _get_input_temp_file('snaptonetwork')
    geodataframe_to_csv(gdf, input_coord_file_path)


def multipoint_to_geodataframe(multipoint):

    # Multipoint: Make FeatureCollection, then import:
    # https://stackoverflow.com/questions/37728540/create-a-geodataframe-from-a-geojson-object
    multipoint = {"type": "MultiPoint", "coordinates": [[-17.25355, -44.885825], [-13.763611, -43.595833]]}
    print('Input:\n'+geojson.dumps(multipoint))

    print('Try to make FeatureCollection...')
    feature = geojson.Feature(geometry=multipoint, properties={}) #  also add properties??
    collection = geojson.FeatureCollection([feature])
    print('Result:\n'+geojson.dumps(collection))

    print('Try to make GeoDataFrame')
    gdf = geopandas.GeoDataFrame.from_features(collection)
    print('Result (type %s):\n%s' % (type(gdf), gdf.head()))


def geodataframe_to_csv(gdf, csv_file_path, sep=','):
    pass # WIP TODO how is geometry encoded?
    df = pd.DataFrame(gdf)
    df.to_csv(csv_file_path, sep, encoding='utf-8')


def multipoint_to_csv(multipoint, col_name_lon, col_name_lat, sep):
    # TODO Use geopandas or something for this! Geojson to 
    # What file is desired?

    # Make a file out of the input geojson, as the bash file wants them as a file
    input_coord_file_path = _get_input_temp_file('snaptonetwork')
    LOGGER.debug('Writing input coordinates from geojson into "%s"...' % input_coord_file_path)
    with open(input_coord_file_path, 'w') as inputfile: # Overwrite previous input file!
        inputfile.write('%s%s%s%s%s\n' % ("foo", sep, col_name_lon, sep, col_name_lat))
        for coord_pair in multipoint['coordinates']:
            lon = coord_pair[0]
            lat = coord_pair[1]
            coord_pair_str = '%s%s%s%s%s\n' % (99999, sep, lat, sep, lon) # comma separated
            inputfile.write(coord_pair_str)

    # DEBUG TODO FIXME This is a dirty little fix
    # This is what happens in R in this line:
    # https://github.com/glowabio/hydrographr/blob/HEAD/R/snap_to_network.R#L193C3-L193C17
    # They just export the coordinates, no other columns!
    # But the input line in grass seems to expect more lines!
    # Line 73 in snap_to_network.sh:
    # https://github.com/glowabio/hydrographr/blob/24e350f1606e60a02594b4d655501ca68bc3e846/inst/sh/snap_to_network.sh#L73
    # So I now added another dummy foo column in front.

    return input_coord_file_path

def call_snap_to_network_sh(path_coord_file, id_col_name, lon_col_name, lat_col_name, path_stream_tif, path_accumul_tif, method, distance, accumulation, snap_tmp_path, tmp_dir):
    
    with open(snap_tmp_path, 'w') as myfile:
        pass # To empty the file! TODO can be done more elegantly - truncate!

    LOGGER.debug('Now calling bash which calls grass/gdal...')
    bash_file = 'snap_to_network.sh'
    bash_path = _get_dir_scripts()+'/'+bash_file

    cmd =[bash_path, path_coord_file, id_col_name, lon_col_name, lat_col_name,
          path_stream_tif, path_accumul_tif, method, str(distance), str(accumulation), snap_tmp_path, tmp_dir]
    LOGGER.debug('GRASS command: %s' % cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = p.communicate()
    LOGGER.debug('Process exit code: %s' % p.returncode)
    LOGGER.debug('Process output:\n____________\n%s\n_____(end of process stdout)_______' % stdoutdata.decode())
    if len(stderrdata.decode()) > 0:
        LOGGER.debug('Process errors:\n____________\n%s\n_____(end of process stderr)_______' % stderrdata.decode())
    else:
        LOGGER.debug('(Nothing written to stderr: "%s")' % stderrdata.decode())
    return snap_tmp_path


def snap_to_network(input_coord_file_path, basin_id, method, distance, accumulation, data_dir):
    # Hardcoded paths on server:
    # TODO: Do we have to cut them smaller for processing?
    #path_accumul_tif = '/home/mbuurman/work/testing_hydrographr/data/basin_481051/accumulation_481051.tif'
    #path_stream_tif  = '/home/mbuurman/work/testing_hydrographr/data/basin_481051/segment_481051.tif'
    # data dir: /home/mbuurman/work/hydro_casestudy_saofra/data/
    #path_accumul_tif = path_data+os.sep+'basin_481051/accumulation_481051.tif'
    #path_stream_tif  = path_data+os.sep+'basin_481051/segment_481051.tif'
    path_accumul_tif = "%s/basin_%s/accumulation_%s.tif" % (data_dir.rstrip('/'), basin_id, basin_id)
    path_stream_tif = "%s/basin_%s/segment_%s.tif" % (data_dir.rstrip('/'), basin_id, basin_id)
    tmp_dir =  tempfile.gettempdir()
    #randomstring = (''.join(random.sample(string.ascii_lowercase+string.digits, 5)))
    #snap_tmp_path =  tempfile.gettempdir()+os.sep+'__output_snappingtool_'+randomstring+'.txt' # intermediate result storage used by GRASS!
    snap_tmp_path = _get_output_temp_file('snappingtool')
    col_name_id = 'dummy_unused' # or so! I don't think is is really used, is it? TODO check this third column business
    col_name_lon = 'lon' # TODO BETTER defined where?
    col_name_lat = 'lat' # TODO BETTER defined where?

    # Now call the tool:
    snap_tmp_path = call_snap_to_network_sh(input_coord_file_path,
        col_name_id, col_name_lon, col_name_lat,
        path_stream_tif, path_accumul_tif,
        method, distance, accumulation,
        snap_tmp_path, tmp_dir)

    return snap_tmp_path

    # Now make geojson from the tool:
    #result_multipoint = self.csv_to_geojson(snap_tmp_path)
    #LOGGER.debug('________________________________')
    #LOGGER.info('Result multipoint: %s' % result_multipoint)
    #LOGGER.info('Result multipoint: %s (dumped)' % geosjon.dumps(result_multipoint))



if __name__ == '__main__':

    #import sys
    #logfilename = sys.argv[0].rstrip('py')+'log'
    # TODO add date
    #logging.basicConfig(filename=logfilename, encoding='utf-8', level=logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG)

    if True:

        # Step 1: Get species data filtered by drainage basin:
        print('GET SPECIES DATA')
        species_name = 'Conorhynchos conirostris'
        basin_id = '481051'
        csv_file_path = get_species_data(species_name, basin_id, PYGEOAPI_DATA_DIR)
        LOGGER.debug('Result stored to: "%s"' % csv_file_path)
        # Writes comma-separated:
        #mbuurman@IN0142:~$ cat /tmp/__output_getspeciesdatatool_8had3.csv
        #X,Y,occurence_id,longitude,latitude,species,occurrenceStatus,country,year
        #-44.885825,-17.25355,"1",-44.885825,-17.25355,Conorhynchos conirostris,PRESENT,Brazil,"2021"
        #-43.595833,-13.763611,"2",-43.595833,-13.763611,Conorhynchos conirostris,PRESENT,Brazil,"2020"

        # Step 2: Convert to geodataframe:
        remove_temp_file = False # for testing
        import geojson
        #output_as_geojson = file_coordinates_to_geojson_old(csv_file_path, remove_temp_file)
        #LOGGER.info('Output geojson %s' % geojson.dumps(output_as_geojson))
        # Convert to geojson:
        col_name_lon = 'longitude'
        col_name_lat = 'latitude'
        sep = ',' # as get_species_data writes comma-separated
        output_as_geodataframe = csv_coordinates_to_geodataframe(csv_file_path, col_name_lon, col_name_lat, sep, remove_temp_file)
        LOGGER.debug('Converting result to GeoJSON...')
        output_as_geojson_string = output_as_geodataframe.to_json()
        output_as_geojson_pretty = geojson.loads(output_as_geojson_string)
        LOGGER.debug('Converting done. Result as geojson: %s ... ... ...' % output_as_geojson_string[0:200])
        print('\nGET SPECIES DATA DONE\n\n')


    ### Now test snap to network:
    if True:
        print('\nSNAPPING:')
        basin_id = '481051'
        method = 'distance'
        distance = '500'
        accumulation = '0.5'
        multipoint = {"type": "MultiPoint", "coordinates": [
            [-40.492972, -12.948389], [-45.780827, -17.178306], [-45.904444, -17.074722],
            [-46.893511, -17.397917], [-45.245, -18.136], [-45.24, -18.14], 
            [-43.138889, -11.092222], [-44.954661, -17.356778], [-43.966667, -14.916667],
            [-44.954963, -17.336154], [-44.357, -15.49277], [-44.357, -15.492768]
        ]}
        col_name_lon = 'lon'
        col_name_lat = 'lat'
        sep=',' # snapping tool expects comma-sep
        input_coord_file_path = multipoint_to_csv(multipoint, col_name_lon, col_name_lat, sep)
        output_path = snap_to_network(input_coord_file_path, basin_id, method, distance, accumulation, PYGEOAPI_DATA_DIR)
        print('SNAPPING OUTPUT: %s' % output_path)

        #df = pandas.read_csv(output_path, sep=' ')
        #print(df.head())
        #gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.lon, df.lat))
        #gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df['lon'], df['lat']))
        col_name_lon = 'lon'
        col_name_lat = 'lat'
        remove_temp_file = False
        sep=' ' # snapping tool writes space-sep
        gdf = csv_coordinates_to_geodataframe(output_path, col_name_lon, col_name_lat, sep, remove_temp_file)
        print(gdf.head())
