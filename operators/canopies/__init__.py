import os
from pathlib import Path
import requests
import shutil
import json
import tempfile

import fiona
from pyproj import Transformer
from shapely.errors import ShapelyError
from shapely.ops import transform, unary_union
from shapely.geometry import shape, mapping

import dataflows as DF
from .distance_to_road import distance_to_road
from treebase.s3_utils import S3Utils
from treebase.log import logger
from treebase.mapbox_utils import run_tippecanoe, upload_tileset


def geo_props():
    def func(row):
        s = row['__geometry']
        if isinstance(s, str):
            s = json.loads(s)
        s = shape(s)
        row['coords'] = mapping(s.centroid)
        (minx, miny, maxx, maxy) = s.bounds
        row['compactness'] = float(row['area']) / (max((maxx - minx), (maxy - miny))**2)

    return DF.Flow(
        DF.add_field('coords', 'object'),
        DF.add_field('compactness', 'number'),
        func,
    )


def main():
    logger.info('PROCESSING CANOPIES')

    s3 = S3Utils()

    geojson_file = 'canopies.geojson'
    with s3.get_or_create('processed/canopies/canopies.geojson', geojson_file) as fn:
        if fn:
            canopies_gdb_file = 'canopies.zip'
            with s3.get_or_create('processed/canopies/canopies.zip', canopies_gdb_file) as fn:
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

            # Extract zip file to temp folder using tempfile.tempdir
            tmpdirname = tempfile.TemporaryDirectory()
            print('### Extracting zip file ###')
            shutil.unpack_archive(canopies_gdb_file, tmpdirname.name)
            canopies_gdb_file = os.path.join(tmpdirname.name, 'NationalCanopyTreesV2.shp')

            print('### Converting to GeoJSON ###')
            layername = 'NationalCanopyTreesV2'
            used_fids = set()
            clusters = set()
            with fiona.open(canopies_gdb_file, layername=layername) as collection:
                with fiona.open(canopies_gdb_file, layername=layername) as collection_xref:
                    print('CRS', collection.crs)

                    for fid, item in collection.items():
                        if fid in used_fids:
                            continue
                        used_fids.add(fid)
                        if item['geometry'] is None:
                            continue
                        selected = fid
                        geometry = shape(item['geometry'])
                        area = geometry.area
                        for fid2, item2 in collection_xref.items(bbox=geometry.buffer(10).bounds):
                            if fid2 in used_fids:
                                continue
                            geometry2 = shape(item2['geometry'])
                            try:
                                if geometry2.intersects(geometry):
                                    used_fids.add(fid2)
                                    if geometry2.area > area:
                                        selected = fid2
                            except ShapelyError as e:
                                pass
                        clusters.add(selected)
                        if len(used_fids) % 10000 == 0:
                            print('DEDUPING:', len(clusters), len(used_fids))
                print(f'{len(used_fids)} items, {len(clusters)} clusters')

            with open(geojson_file, 'w') as outfile:
                outfile.write('{"type": "FeatureCollection", "features": [')
                first = True
                transformer = None
                crs = None
                try:
                    crs = collection.crs['init']
                except KeyError:
                    crs = 'epsg:2039'
                if crs != 'epsg:4326':
                    transformer = Transformer.from_crs(crs, 'epsg:4326', always_xy=True)
                i = 0
                with fiona.open(canopies_gdb_file, layername=layername) as collection:
                    for fid, item in collection.items():
                        if fid not in clusters:
                            continue
                        geometry = shape(item['geometry'])
                        area = item['properties']['Shape_Area']
                        geometry = transform(transformer.transform, geometry)
                        if first:
                            first = False
                        else:
                            outfile.write(',')
                        geometry = mapping(geometry)
                        outfile.write(json.dumps(dict(
                            type='Feature',
                            properties={'area': area},
                            geometry=geometry,
                        )) + '\n')
                        if i % 1000 == 0:
                            print(f'processed {i} clusters')
                        i += 1
                outfile.write(']}')

            print('### Uploading to MapBox ###', geojson_file)
            filename = Path(geojson_file)
            mbtiles_filename = str(filename.with_suffix('.mbtiles'))
            if run_tippecanoe('-z15', str(filename), '-o', mbtiles_filename,  '-l', 'canopies'):
                upload_tileset(mbtiles_filename, 'treebase.canopies', 'Canopy Data')

    filtered_geojson_file = 'extracted_trees.geojson'
    with s3.get_or_create('processed/canopies/extracted_trees.geojson', filtered_geojson_file) as fn:
        if fn:
            # MIN_AREA = 4
            # MAX_AREA = 200

            print('### Filtering by area, Calculating distance to road ###')
            DF.Flow(
                DF.load(geojson_file),
                # DF.filter_rows(lambda r: r['area'] > MIN_AREA and r['area'] < MAX_AREA),
                geo_props(),
                distance_to_road(),
                DF.set_type('coords', type='geojson', transform=lambda v: json.dumps(v)),
                DF.select_fields(['coords', 'area', 'compactness', 'distance_to_road']),
                DF.update_resource(-1, name='extracted_trees', path='extracted_trees.geojson'),
                DF.dump_to_path('.', format='geojson'),
            ).process()


def operator(*_):
    main()

if __name__ == "__main__":
    main()