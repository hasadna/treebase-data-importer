import os
from pathlib import Path
import requests
import shutil
import json

import fiona
from pyproj import Transformer
from shapely.ops import transform, unary_union
from shapely.geometry import shape, mapping

from treebase.s3_utils import S3Utils
from treebase.log import logger
from treebase.mapbox_utils import run_tippecanoe, upload_tileset


def main():
    logger.info('PROCESSING CANOPIES')

    s3 = S3Utils()

    filtered_geojson_file = 'extracted_trees.geojson'
    with s3.get_or_create('processed/canopies/extracted_trees.geojson', filtered_geojson_file) as fn:
        if fn:
            geojson_file = 'canopies.geojson'
            with s3.get_or_create('processed/canopies/canopies.geojson', geojson_file) as fn:
                if fn:
                    canopies_gdb_file = 'canopies.gdb.zip'
                    with s3.get_or_create('processed/canopies/canopies.gdb.zip', canopies_gdb_file) as fn:
                        if fn:
                            print('### Downloading from data.gov.il ###')
                            dataset = requests.get('https://data.gov.il/api/action/package_search?q=nationalcanopytrees').json()['result']['results'][0]
                            resource = dataset['resources'][0]['url']
                            resource = resource.replace('/e.', '/')
                            with open(fn, 'wb') as outfile:
                                r = requests.get(resource, headers={'User-Agent': 'datagov-external-client'}, stream=True)
                                if r.status_code == 200:
                                    r.raw.decode_content = True
                                    shutil.copyfileobj(r.raw, outfile)

                    print('### Converting to GeoJSON ###')
                    layername = 'Alltrees'
                    with open(geojson_file, 'w') as outfile:
                        with fiona.open(canopies_gdb_file, layername=layername) as collection:
                            print('CRS', collection.crs)
                            transformer = None
                            if collection.crs['init'] != 'epsg:4326':
                                transformer = Transformer.from_crs(collection.crs['init'], 'epsg:4326', always_xy=True)
                            outfile.write('{"type": "FeatureCollection", "features": [')
                            first = True
                                
                            for item in collection.filter():
                                if item['geometry'] is None:
                                    continue
                                geometry = shape(item['geometry'])
                                if transformer is not None:
                                    geometry = transform(transformer.transform, shape(geometry))
                                if first:
                                    first = False
                                else:
                                    outfile.write(',')
                                area = item['properties']['Shape_Area']
                                geometry = mapping(geometry)
                                outfile.write(json.dumps(dict(
                                    type='Feature',
                                    properties={'area': area},
                                    geometry=geometry,
                                )) + '\n')
                        outfile.write(']}')

            import dataflows as DF
            from .distance_to_road import distance_to_road

            MIN_AREA = 4
            MAX_AREA = 200

            print('### Filtering by area, Calculating distance to road ###')
            DF.Flow(
                DF.load(geojson_file),
                DF.filter_rows(lambda r: r['area'] > MIN_AREA and r['area'] < MAX_AREA),
                DF.add_field('coords', 'object', lambda r: mapping(shape(r['__geometry']).centroid)),
                DF.select_fields(['coords', 'area']),
                DF.set_type('coords', type='geojson', transform=lambda v: json.dumps(v)),
                distance_to_road(),
                DF.update_resource(-1, name='extracted_trees', path='extracted_trees.geojson'),
                DF.dump_to_path('.', format='geojson'),
            ).process()

    print('### Uploading to MapBox ###')
    filename = Path(filtered_geojson_file)
    mbtiles_filename = str(filename.with_suffix('.mbtiles'))
    if run_tippecanoe('-z15', str(filename), '-o', mbtiles_filename,  '-l', 'canopies'):
        upload_tileset(mbtiles_filename, 'treebase.canopies', 'Canopy Data')

def operator(*_):
    main()

if __name__ == "__main__":
    main()