# pygeoapi

[![DOI](https://zenodo.org/badge/121585259.svg)](https://zenodo.org/badge/latestdoi/121585259)
[![Build](https://github.com/geopython/pygeoapi/actions/workflows/main.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/main.yml)
[![Docker](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml)

[pygeoapi](https://pygeoapi.io) is a Python server implementation of the [OGC API](https://ogcapi.ogc.org) suite of standards. The project emerged as part of the next generation OGC API efforts in 2018 and provides the capability for organizations to deploy a RESTful OGC API endpoint using OpenAPI, GeoJSON, and HTML. pygeoapi is [open source](https://opensource.org/) and released under an [MIT license](https://github.com/geopython/pygeoapi/blob/master/LICENSE.md).

Please read the docs at [https://docs.pygeoapi.io](https://docs.pygeoapi.io) for more information.



## AquaInfra case

Please add these steps when installing pygeoapi for the AquaInfra case, as they are needed to run the `get_species_data` process!

* Please install the python module `geojson` in the virtual environment that runs the service (most likely running `cd /home/.../pygeoapi/pygeoapi && . ../bin/activate && pip install geojson` will do the job).
* Please create a directory for the input data, e.g. `/home/.../work/pygeo/data`
* Into that directory, add a directory `basin_481051` and into that, put the input vector file `basin_481051.gpkg`. The same for any other river basin you want to support (`481051` being the drainage basin id).
* Then, tell pygeoapi where to find the data, by executing: `export PYGEOAPI_DATA_DIR='/home/.../work/pygeo/data'`.
* Please install the R library `rgbif`.

(Merret, 2023-10-18)


### Creating basin_481051.gpkg

You can create this file using R and the package [hydrographr](https://github.com/glowabio/hydrographr/) with the following steps. The source of the data is [hydrography90m](https://hydrography.org/).

```
library(hydrographr)

### Merret, 2023-10-17
### To allow downloading and creating the vector layer needed for the pygeoapi
### test service "get_species_data": basin_481051.gpkg
### Instructions are a subset of:
### https://glowabio.github.io/hydrographr/articles/case_study_brazil.html

### Download three tiles that comprise basin 481051
wdir = "/home/.../work/"
tile_id = c("h12v08", "h12v10", "h14v08")
vars_gpkg <- c("basin", "order_vect_segment")
download_tiles(variable = vars_gpkg, tile_id = tile_id, file_format = "gpkg", download_dir =   paste0(wdir, "/data"))

### Filter basin 481051
# Define a directory for the São Francisco drainage basin
saofra_dir <-  paste0(wdir, "/data/basin_481051")
if(!dir.exists(saofra_dir)) dir.create(saofra_dir)
# Get the full paths of the basin  GeoPackage tiles
basin_dir <- list.files(wdir, pattern = "basin_h[v0-8]+.gpkg$", full.names = TRUE, recursive = TRUE)

# Filter basin ID from the GeoPackages of the basin tiles
# Save the filtered tiles
for(itile in basin_dir) {
  filtered_tile <- read_geopackage(itile, import_as = "sf", subc_id = 481051, name = "ID")
  write_sf(filtered_tile, paste(saofra_dir, paste0(str_remove(basename(itile), ".gpkg"),"_tmp.gpkg"), sep="/"))
}

### Merge basin 481051
# Merge filtered GeoPackage tiles
merge_tiles(tile_dir = saofra_dir, tile_names = list.files(saofra_dir, full.names = FALSE, pattern = "basin_.+_tmp.gpkg$"),
            out_dir = saofra_dir,
            file_name = "basin_481051.gpkg",
            name = "ID",
            read = FALSE)


```


(Merret, 2023-10-18)


