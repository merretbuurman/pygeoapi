
# Import packages
#packages <- c("sf", "data.table", "tidyverse", "readxl", "ggplot2", "ggmap", "mapview", "httr", "R.utils")
packages <- c("sf", "data.table", "tidyverse", "readxl")
lapply(packages, require, character.only = TRUE)

# User params
#assessmentPeriod <- "2016-2021" # HOLAS III
#assessmentPeriod <- "1877-9999"
#assessmentPeriod <- "2011-2016"
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
assessmentPeriod = args[1]
unitsFileName = args[2]
configurationFileName = args[3]
outputPath = args[4]

# Define paths for input data
#inputPath <- file.path("Input", assessmentPeriod)
#inputPath <- file.path("home", "ubuntu", "Input", assessmentPeriod) # TODO Fix workding dirs!
inputPath <- paste0("/home/ubuntu/Input") # TODO Fix workding dirs!
#outputPath <- file.path(paste0("Output", format(Sys.time(), "%Y%m%d_%H%M%S")), assessmentPeriod)
#outputPath <- file.path(paste0("Output", format(Sys.time(), "%Y%m%d")), assessmentPeriod)
dir.create(outputPath, showWarnings = FALSE, recursive = TRUE)

unitsFile <- file.path(inputPath, unitsFileName)
configurationFile <- file.path(inputPath, configurationFileName)


# Read indicator configs
# This needs "readxl"
#print(paste('Current working directory is:', getwd()))
#print(paste('Reading indicators from', configurationFile))
#indicators <- as.data.table(read_excel(configurationFile, sheet = "Indicators", col_types = c("numeric", "numeric", "text", "text", "text", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "text", "numeric", "numeric", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"))) %>% setkey(IndicatorID)
#indicatorUnits <- as.data.table(read_excel(configurationFile, sheet = "IndicatorUnits", col_types = "numeric")) %>% setkey(IndicatorID, UnitID)
#indicatorUnitResults <- as.data.table(read_excel(configurationFile, sheet = "IndicatorUnitResults", col_types = "numeric")) %>% setkey(IndicatorID, UnitID, Period)

# Assessment Units + Grid Units-------------------------------------------------

if (assessmentPeriod == "2011-2016") {
  # Read assessment unit from shape file, requires sf
  units <- st_read(unitsFile)
  
  # Filter for open sea assessment units, requires data.table
  units <- units[units$Code %like% 'SEA',]
  
  # Correct Description column name - temporary solution!
  colnames(units)[2] <- "Description"
  
  # Correct Åland Sea ascii character - temporary solution!
  units[14,2] <- 'Åland Sea'
  
  # Include stations from position 55.86667+-0.01667 12.75+-0.01667 which will include the Danish station KBH/DMU 431 and the Swedish station Wlandskrona into assessment unit 3/SEA-003
  units[3,] <- st_union(units[3,],st_as_sfc("POLYGON((12.73333 55.85,12.73333 55.88334,12.76667 55.88334,12.76667 55.85,12.73333 55.85))", crs = 4326))
  
  # Assign IDs
  units$UnitID = 1:nrow(units)
  
  # Transform projection into ETRS_1989_LAEA
  units <- st_transform(units, crs = 3035)
  
  # Calculate area
  units$UnitArea <- st_area(units)
} else {
  # Read assessment unit from shape file
  units <- st_read(unitsFile) %>% st_zm()
  
  # Filter for open sea assessment units
  units <- units[units$HELCOM_ID %like% 'SEA',]
  
  # Include stations from position 55.86667+-0.01667 12.75+-0.01667 which will include the Danish station KBH/DMU 431 and the Swedish station Wlandskrona into assessment unit 3/SEA-003
  units[3,] <- st_union(units[3,],st_transform(st_as_sfc("POLYGON((12.73333 55.85,12.73333 55.88334,12.76667 55.88334,12.76667 55.85,12.73333 55.85))", crs = 4326), crs = 3035))
  
  # Order, Rename and Remove columns
  units <- as.data.table(units)[order(HELCOM_ID), .(Code = HELCOM_ID, Description = Name, GEOM = geometry)] %>%
    st_sf()
  
  # Assign IDs
  units$UnitID = 1:nrow(units)

  # Identify invalid geometries
  st_is_valid(units)
  
  # Calculate area
  units$UnitArea <- st_area(units)
}

# Identify invalid geometries
st_is_valid(units) # doppelt?

# Make geometries valid by doing the buffer of nothing trick
#units <- sf::st_buffer(units, 0.0)

# Identify overlapping assessment units
#st_overlaps(units)

# Make grid units
make.gridunits <- function(units, gridSize) {
  units <- st_transform(units, crs = 3035)
  
  bbox <- st_bbox(units)
  
  xmin <- floor(bbox$xmin / gridSize) * gridSize
  ymin <- floor(bbox$ymin / gridSize) * gridSize
  xmax <- ceiling(bbox$xmax / gridSize) * gridSize
  ymax <- ceiling(bbox$ymax / gridSize) * gridSize
  
  xn <- (xmax - xmin) / gridSize
  yn <- (ymax - ymin) / gridSize
  
  grid <- st_make_grid(units, cellsize = gridSize, c(xmin, ymin), n = c(xn, yn), crs = 3035) %>%
    st_sf()
  
  grid$GridID = 1:nrow(grid)
  
  gridunits <- st_intersection(grid, units)
  
  gridunits$Area <- st_area(gridunits)
  
  return(gridunits)
}

gridunits10 <- make.gridunits(units, 10000)
gridunits30 <- make.gridunits(units, 30000)
gridunits60 <- make.gridunits(units, 60000)

# This needs "readxl"
unitGridSize <- as.data.table(read_excel(configurationFile, sheet = "UnitGridSize")) %>% setkey(UnitID)

a <- merge(unitGridSize[GridSize == 10000], gridunits10 %>% select(UnitID, GridID, GridArea = Area))
b <- merge(unitGridSize[GridSize == 30000], gridunits30 %>% select(UnitID, GridID, GridArea = Area))
c <- merge(unitGridSize[GridSize == 60000], gridunits60 %>% select(UnitID, GridID, GridArea = Area))
gridunits <- st_as_sf(rbindlist(list(a,b,c)))
rm(a,b,c)

gridunits <- st_cast(gridunits)

#st_write(gridunits, file.path(outputPath, "gridunits.shp"), delete_layer = TRUE)

# Plot the assessment units
ggplot() + geom_sf(data = units) + coord_sf()
ggsave(file.path(outputPath, "Assessment_Units.png"), width = 12, height = 9, dpi = 300)
ggplot() + geom_sf(data = gridunits10) + coord_sf()
ggsave(file.path(outputPath, "Assessment_GridUnits10.png"), width = 12, height = 9, dpi = 300)
ggplot() + geom_sf(data = gridunits30) + coord_sf()
ggsave(file.path(outputPath, "Assessment_GridUnits30.png"), width = 12, height = 9, dpi = 300)
ggplot() + geom_sf(data = gridunits60) + coord_sf()
ggsave(file.path(outputPath, "Assessment_GridUnits60.png"), width = 12, height = 9, dpi = 300)
ggplot() + geom_sf(data = st_cast(gridunits)) + coord_sf()
ggsave(file.path(outputPath, "Assessment_GridUnits.png"), width = 12, height = 9, dpi = 300)

# Done!

print('R script finished running.')

print('Now writing intermediate files to /home/ubuntu/intermediate_files/')
intermediateFileName1 = "/home/ubuntu/intermediate_files/my_gridunits.rds"
intermediateFileName2 = "/home/ubuntu/intermediate_files/my_units.rds"
saveRDS(gridunits, file = intermediateFileName1)
print(paste('Stored intermediate result:', intermediateFileName1))
saveRDS(units, file = intermediateFileName2)
print(paste('Stored intermediate result:', intermediateFileName2))

#print(paste('Now writing outputs to', outputPath))
# No outputs
#print(paste('R script wrote outputs to', outputPath))

