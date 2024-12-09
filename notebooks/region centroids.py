import pandas as pd

from zipfile import ZipFile

import fiona
# fiona package must be downloaded (e.g. (python) pip install fiona) in order to analyse geodata/shapefiles, for the geometry you also need to install shapely with (pip install shapely)
from shapely.geometry import shape

import sys
sys.path.append('../pummeler')

from geocode_data import geocode_data

county_region_00 = geocode_data('county_region_00').region.to_dict()
county_region_10 = geocode_data('county_region_10').region.to_dict()

from collections import defaultdict

region_shapes_00 = defaultdict(list)
region_shapes_10 = defaultdict(list)

print(county_region_10)

county_region_00['46113'] = 'SD_00_03'
county_region_10['46113'] = 'SD_10_03'
# These keys were missing in the dictionary, thus i assigned the most likely keys

for county in fiona.open('UScounties'):
    # opens the the file 'UScounties', which you need to unpack (e.g. with 7zip)
    shp = shape(county['geometry'])
    fips = county['properties']['FIPS']
    if county['properties']['STATE_NAME'] == 'Alaska':
        region_shapes_00['AK_00_01'].append(shp)
        region_shapes_10['AK_10_01'].append(shp)
        # since we mashed all of Alaska's counties into one we have to to do we same with the shapefiles
    else:
        region_shapes_00[county_region_00[fips]].append(shp)
        region_shapes_10[county_region_10[fips]].append(shp)
# adding their respective shapefiles to the counties

from shapely.ops import cascaded_union

centroids00 = pd.DataFrame.from_records(
    [(k,) + cascaded_union(v).centroid.coords[0] for k, v in region_shapes_00.items()],
    columns=['region', 'lon', 'lat'], index='region').sort_index()
centroids10 = pd.DataFrame.from_records(
    [(k,) + cascaded_union(v).centroid.coords[0] for k, v in region_shapes_10.items()],
    columns=['region', 'lon', 'lat'], index='region').sort_index()
# Now the centroids of the geometry are being calculated with the centroid function with k and v as the x and y coordinates

#fn = '../pummeler/data/regions.h5'
centroids00.to_hdf('regions.h5', 'centroids00', format='table', complib='blosc', complevel=9)
centroids10.to_hdf('regions.h5', 'centroids10', format='table', complib='blosc', complevel=9)
# Adding the calculated centroids to the region.h5 file
