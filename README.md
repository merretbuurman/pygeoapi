# pygeoapi

[![DOI](https://zenodo.org/badge/121585259.svg)](https://zenodo.org/badge/latestdoi/121585259)
[![Build](https://github.com/geopython/pygeoapi/actions/workflows/main.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/main.yml)
[![Docker](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml)

[pygeoapi](https://pygeoapi.io) is a Python server implementation of the [OGC API](https://ogcapi.ogc.org) suite of standards. The project emerged as part of the next generation OGC API efforts in 2018 and provides the capability for organizations to deploy a RESTful OGC API endpoint using OpenAPI, GeoJSON, and HTML. pygeoapi is [open source](https://opensource.org/) and released under an [MIT license](https://github.com/geopython/pygeoapi/blob/master/LICENSE.md).

Please read the docs at [https://docs.pygeoapi.io](https://docs.pygeoapi.io) for more information.


## AquaInfra case

Please add these steps when installing pygeoapi for the AquaInfra case, as they are needed to run the `snap_to_network` process!

* Please install `geojson` in the virtual environment that runs the service (most likely running `cd /home/.../pygeoapi/pygeoapi; 
 . ../bin/activate; pip install geojson` will do the job).
* Please create a directory for the input data, e.g. `/home/.../work/pygeo/data`
* Into that directory, add a directory `basin_481051` and into that, put the two input rasters `segment_481051.tif` and `accumulation_481051.tif`. The same for any other river basin you want to support (`481051` being the drainage basin id).
* Then, tell pygeoapi where to find the data, by executing: `export PYGEOAPI_DATA_DIR='/home/.../work/pygeo/data'`.

(Merret, 2023-10-18)
