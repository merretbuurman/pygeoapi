
# Run this in R
# From bash:
#/usr/bin/Rscript --vanilla /home/mbuurman/work/trying_out_hydrographr/get_species_data.R "/tmp/species.csv" "Conorhynchos conirostris" "/home/mbuurman/work/hydro_casestudy_saofra/data/basin_481051/basin_481051.gpkg"


# These have to be passed as args from python:
# outputfilename = paste0(tempdir(), "/", "species_unfiltered.csv")
# species_name = "Conorhynchos conirostris"
# basinpolygonfilename = "/home/mbuurman/work/hydro_casestudy_saofra/data/basin_481051/basin_481051.gpkg"

# Get command line arguments, passed by python
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
species_name = args[2]
outputfilename = args[1]
basinpolygonfilename = args[3]

# Imports
library(dplyr) # for select
library(rgbif) # for occ_data
library(sf)    # for st_as_sf

# Download occurrence data with coordinates from GBIF
print('R: Requesting data from GBIF using rgbif (please cite!)')
gbif_data <- occ_data(scientificName = species_name, hasCoordinate = TRUE)
print('R: Finished requesting data!')
# To cite the data use:
# gbif_citation(gbif_data)        

# Clean the data
print('R: Starting to clean the data...')
spdata <- gbif_data$data %>% 
  select(decimalLongitude, decimalLatitude, species, occurrenceStatus, country, year) %>% 
  filter(!is.na(year)) %>% 
  distinct() %>% 
  mutate(occurence_id = 1:nrow(.)) %>% 
  rename("longitude" = "decimalLongitude", "latitude" = "decimalLatitude") %>% 
  select(7, 1:6)
print('R: Finished to clean the data...')

# How many occurences? 14!
#length(spdata$occurence_id)

# Create simple features from the data tables 
spdata_pts <- st_as_sf(spdata, coords = c("longitude","latitude"), remove = FALSE, crs="WGS84")
print('R: Created simple features from the data...')

# Write them to file for Python to pick up!
###st_write(spdata_pts, outputfilename, layer_options = "GEOMETRY=AS_XY")


 ####
 #### GETTING SPECIES IS DONE!
 ####


# Load the polygon of the drainage basin
#basin_poly <- read_sf(paste0(saofra_dir,"/basin_481051.gpkg"))
print('R: Reading basin polygon')
basin_poly <- read_sf(basinpolygonfilename)

# Only keep species occurrences and dams within the drainage basin
print('R: Filtering species occurences by polygon')
spdata_pts_basin <- st_filter(spdata_pts, basin_poly)
# How many?
#length(spdata_pts_481051$occurence_id)

# Write Polygon
#polygonfilename = paste0(tempdir(), "/", "polygon.csv")
#st_write(basin_poly, polygonfilename, layer_options = "GEOMETRY=AS_WKT")

print(paste0('R: Writing filtered data to file: ', outputfilename))
st_write(spdata_pts_basin, outputfilename, layer_options = "GEOMETRY=AS_XY")
print('R: Done writing to file')
