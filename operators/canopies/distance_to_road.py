from pathlib import Path
import shutil
import json

from shapely.ops import transform
from pyproj import Transformer
from shapely.geometry import shape, Point
import fiona
import requests

import dataflows as DF


SEARCH_RADIUS = 20
MAX_DISTANCE = 10

# Tel Aviv Center
center_x, center_y = 34.75, 32.05
crs = f'+proj=tmerc +lat_0={center_y} +lon_0={center_x} +k_0=1 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs'
transformer = Transformer.from_crs('EPSG:4326', crs, always_xy=True)
inv_transformer = Transformer.from_crs(crs, 'EPSG:4326', always_xy=True)
diff_x, diff_y = inv_transformer.transform(SEARCH_RADIUS, SEARCH_RADIUS)
diff_x -= center_x
diff_y -= center_y
print('DIFFS', diff_x, diff_y)

def download_gpkg():
    GPKG_FILE = Path('roads.gpkg')
    GPKG_URL = 'https://s3.eu-west-2.wasabisys.com/opentreebase-public/geo/roads.gpkg'
    if not GPKG_FILE.exists():
        print('Downloading', GPKG_URL)
        with requests.get(GPKG_URL, stream=True) as r:
            with GPKG_FILE.open('wb') as f:
                shutil.copyfileobj(r.raw, f)
    return GPKG_FILE


def feature_cache(gpkg: fiona.Collection, row):
    lon_deg, lat_deg = json.loads(row['__geometry'])['coordinates']
    crs = f'+proj=tmerc +lat_0={lat_deg} +lon_0={lon_deg} +k_0=1 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs'
    transformer = Transformer.from_crs('EPSG:4326', crs, always_xy=True)
    # ids = set()
    features = []
    bbox = (lon_deg-diff_x, lat_deg-diff_y, lon_deg+diff_x, lat_deg+diff_y)
    # print('QUERYING FEATURES...', lon_deg, lat_deg, bbox)
    features = [
        transform(transformer.transform, shape(f['geometry']))
        for _, f in gpkg.items(bbox=bbox)
        if f['properties'].get('fclass') != 'path'
    ]
    return features


def distance_to_road():

    gpkg = fiona.open(str(download_gpkg()), layer='gis_osm_roads_free_1')
    origin = Point(0, 0)

    def func(rows):
        for row in rows:
            features = feature_cache(gpkg, row)
            minimum = None
            if len(features) > 0:
                for geom in features:
                    distance = origin.distance(geom)
                    if minimum is None or distance < minimum:
                        minimum = distance

                if minimum is not None and minimum < MAX_DISTANCE:
                    row['distance_to_road'] = minimum
                    yield row

    return DF.Flow(
        DF.add_field('distance_to_road', 'number'),
        func,
        DF.filter_rows(lambda r: r['distance_to_road'] < MAX_DISTANCE),
    )
