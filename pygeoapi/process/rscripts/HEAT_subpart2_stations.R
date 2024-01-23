




# Import packages
packages <- c("sf", "data.table")
lapply(packages, require, character.only = TRUE)

# User params
#assessmentPeriod <- "2016-2021" # HOLAS III
#assessmentPeriod <- "1877-9999"
#assessmentPeriod <- "2011-2016"
# TODO: Can we have named ones?
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
stationSamplesBOTFilePath = args[1]
stationSamplesCTDFilePath = args[2]
stationSamplesPMPFilePath = args[3]
outputPath = args[4]
intermediatePath = args[5]

# Create directory for outputs (in this case, three CSV files: StationSamplesBOT.csv, StationSamplesCTD.csv, StationSamplesPMP.csv)
dir.create(outputPath, showWarnings = FALSE, recursive = TRUE)

# Load required intermediate file:
intermediateFileName = paste0(intermediatePath,"/my_gridunits.rds")
print(paste('Now reading intermediate file from:', intermediateFileName))
gridunits = readRDS(file = intermediateFileName)


# In this script, all files are treated equally, so we just let the client pass the file name.
print(paste('stationSamplesBOTFile::::', stationSamplesBOTFilePath))
print(paste('stationSamplesCTDFile::::', stationSamplesCTDFilePath))
print(paste('stationSamplesPMPFile::::', stationSamplesPMPFilePath))


# Ocean hydro chemistry - Bottle and low resolution CTD data, needs "data.table"
stationSamplesBOT <- fread(input = stationSamplesBOTFilePath, sep = "\t", na.strings = "NULL", stringsAsFactors = FALSE, header = TRUE, check.names = TRUE)
stationSamplesBOT[, Type := "B"]

# Ocean hydro chemistry - High resolution CTD data
stationSamplesCTD <- fread(input = stationSamplesCTDFilePath, sep = "\t", na.strings = "NULL", stringsAsFactors = FALSE, header = TRUE, check.names = TRUE)
stationSamplesCTD[, Type := "C"]

# Ocean hydro chemistry - Pump data
stationSamplesPMP <- fread(input = stationSamplesPMPFilePath, sep = "\t", na.strings = "NULL", stringsAsFactors = FALSE, header = TRUE, check.names = TRUE)
stationSamplesPMP[, Type := "P"]

# Combine station samples
stationSamples <- rbindlist(list(stationSamplesBOT, stationSamplesCTD, stationSamplesPMP), use.names = TRUE, fill = TRUE)

# Remove original data tables
rm(stationSamplesBOT, stationSamplesCTD, stationSamplesPMP)

# Unique stations by natural key
uniqueN(stationSamples, by = c("Cruise", "Station", "Type", "Year", "Month", "Day", "Hour", "Minute", "Longitude..degrees_east.", "Latitude..degrees_north."))

# Assign station ID by natural key
stationSamples[, StationID := .GRP, by = .(Cruise, Station, Type, Year, Month, Day, Hour, Minute, Longitude..degrees_east., Latitude..degrees_north.)]

# Classify station samples into grid units -------------------------------------

# Extract unique stations i.e. longitude/latitude pairs
stations <- unique(stationSamples[, .(Longitude..degrees_east., Latitude..degrees_north.)])

# Make stations spatial keeping original latitude/longitude. This needs "sf"
stations <- st_as_sf(stations, coords = c("Longitude..degrees_east.", "Latitude..degrees_north."), remove = FALSE, crs = 4326)

# Transform projection into ETRS_1989_LAEA
stations <- st_transform(stations, crs = 3035)

# Classify stations into grid units
# GRIDUNITS!!
#gridunits = readRDS(file = "my_gridunits.rds")
stations <- st_join(stations, gridunits, join = st_intersects)

# Delete stations not classified
stations <- na.omit(stations)

# Remove spatial column and nake into data table
stations <- st_set_geometry(stations, NULL) %>% as.data.table()

# Merge stations back into station samples - getting rid of station samples not classified into assessment units
stationSamples <- stations[stationSamples, on = .(Longitude..degrees_east., Latitude..degrees_north.), nomatch = 0]



print('R script finished running.')


intermediateFileName = paste0(intermediatePath,"/my_stationSamples.rds")
saveRDS(stationSamples, file = intermediateFileName)
print(paste('Now writing intermediate files to:', intermediateFileName))


print(paste('Now writing outputs to', outputPath))
# Output station samples mapped to assessment units for contracting parties to check i.e. acceptance level 1
fwrite(stationSamples[Type == 'B'], file.path(outputPath, "StationSamplesBOT.csv"))
fwrite(stationSamples[Type == 'C'], file.path(outputPath, "StationSamplesCTD.csv"))
fwrite(stationSamples[Type == 'P'], file.path(outputPath, "StationSamplesPMP.csv"))
print(paste('R script wrote outputs to', outputPath, ': StationSamplesBOT.csv, StationSamplesCTD.csv, StationSamplesPMP.csv'))

