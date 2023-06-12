from pathlib import Path
import shutil
import json

from shapely.ops import transform
from pyproj import Transformer
from shapely.geometry import shape, Point
import fiona
import requests

import dataflows as DF

from treebase.geo_utils import bbox_diffs
from treebase.s3_utils import S3Utils

SEARCH_RADIUS = 20
MAX_DISTANCE = 10


def download_gpkg():
    GPKG_FILE = Path('roads.gpkg')
    GPKG_URL = 'https://s3.eu-west-2.wasabisys.com/opentreebase-public/geo/roads.gpkg'
    if not GPKG_FILE.exists():
        print('Downloading', GPKG_URL)
        with requests.get(GPKG_URL, stream=True) as r:
            with GPKG_FILE.open('wb') as f:
                shutil.copyfileobj(r.raw, f)
    return GPKG_FILE


def distance_to_road():
    gpkg = fiona.open(str(download_gpkg()), layer='gis_osm_roads_free_1')
    origin = Point(0, 0)
    diff_x, diff_y = bbox_diffs(SEARCH_RADIUS)

    def feature_cache(row):
        lon_deg, lat_deg = row['coords']['coordinates']
        crs = f'+proj=tmerc +lat_0={lat_deg} +lon_0={lon_deg} +k_0=1 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs'
        transformer = Transformer.from_crs('EPSG:4326', crs, always_xy=True)
        # ids = set()
        features = []
        bbox = (lon_deg-diff_x, lat_deg-diff_y, lon_deg+diff_x, lat_deg+diff_y)
        # print('QUERYING FEATURES...', lon_deg, lat_deg, bbox)
        features = [
            (transform(transformer.transform, shape(f['geometry'])), f['properties']['name'], f['properties']['osm_id'])
            for _, f in gpkg.items(bbox=bbox)
            if f['properties'].get('fclass') != 'path' and f['properties'].get('name')
        ]
        return features

    def func(rows):
        s3 = S3Utils()
        with s3.cache_file('cache/distance_to_road/cache.json', 'distance_to_road_cache.json') as fn:
            try:
                cache = json.load(open(fn))
            except:
                cache = dict()
            for row in rows:
                lon_deg, lat_deg = row['coords']['coordinates']
                key = '{:.5f},{:.5f}'.format(lon_deg, lat_deg)
                if key in cache:
                    row.update(cache[key])
                    yield row
                    continue
                features = feature_cache(row)
                minimum = None
                if len(features) > 0:
                    for geom, name, id in features:
                        distance = origin.distance(geom)
                        if minimum is None or distance < minimum[0]:
                            minimum = distance, name, id

                    if minimum is not None and minimum[0] < MAX_DISTANCE:
                        cache[key] = dict(
                            distance_to_road=minimum[0],
                            road_name=minimum[1],
                            road_id=minimum[2],
                        )
                        row.update(cache[key])
                        yield row
                if key not in cache:
                    cache[key] = dict()
            json.dump(cache, open(fn, 'w'))

    return DF.Flow(
        DF.add_field('distance_to_road', 'number'),
        DF.add_field('road_name', 'string'),
        DF.add_field('road_id', 'string'),
        func,
        # DF.filter_rows(lambda r: r['distance_to_road'] < MAX_DISTANCE),
    )
