import os
from pathlib import Path
import requests
import shutil
import json
import tempfile
import math

from pyproj import Transformer
import fiona
from shapely.errors import ShapelyError
from shapely.ops import transform
from shapely.geometry import shape, mapping, MultiPolygon

import dataflows as DF
from treebase.s3_utils import S3Utils
from treebase.log import logger
from treebase.mapbox_utils import run_tippecanoe, upload_tileset


def geo_props():
    def func(row):
        s = row['__geometry']
        a = row['area']
        if isinstance(s, str):
            s = json.loads(s)
        s = shape(s)
        centroid = s.centroid
        if s.contains(centroid):
            row['coords'] = mapping(centroid)
        l = s.length
        if l > 0:
            row['compactness'] = 4 * math.pi * s.area / l**2
            row['likely_tree'] = row['compactness'] > (a / 150)

    return DF.Flow(
        DF.add_field('coords', 'object'),
        DF.add_field('compactness', 'number'),
        DF.add_field('likely_tree', 'boolean', default=False),
        func,
    )


def main():
    logger.info('PROCESSING CANOPIES')

    s3 = S3Utils()

    canopies_package = 'canopies.gpkg'
    with s3.get_or_create('processed/canopies/canopies.gpkg', canopies_package) as fn:
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

            print('### Converting to Geo Package ###')
            layername = 'NationalCanopyTreesV2'
            # used_fids = set()
            # clusters = set()
            schema = dict(
                geometry='MultiPolygon',
                properties=dict(
                    area='float',
                    _fid='str',
                )
            )
            used_fids = set()
            written = 0
            crs = None
            with fiona.open(canopies_gdb_file, layername=layername) as collection:
                with fiona.open(canopies_gdb_file, layername=layername) as collection_xref:
                    print('CRS', collection.crs)
                    try:
                        crs = collection.crs['init']
                    except KeyError:
                        crs = 'epsg:2039'
                    with fiona.open(canopies_package, 'w',
                                    driver='GPKG',
                                    crs='EPSG:4326',
                                    layer=layername,
                                    schema=schema) as dst:
                        transformer = None
                        if crs != 'epsg:4326':
                            print('TRANSFORMING', repr(crs))
                            transformer = Transformer.from_crs(crs, 'epsg:4326', always_xy=True)
                            print('TRANSFORMER', repr(transformer))
                        for fid, item in collection.items():
                            if fid in used_fids:
                                continue
                            used_fids.add(fid)
                            if item['geometry'] is None:
                                continue
                            selected = fid, item
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
                                            selected = fid2, item2
                                except ShapelyError as e:
                                    pass
                            fid, item = selected
                            geometry = shape(item['geometry'])
                            area = item['properties']['Shape_Area']
                            if transformer:
                                geometry = transform(transformer.transform, geometry)
                            if geometry.geom_type == 'Polygon':
                                geometry = MultiPolygon([geometry])
                            geometry = mapping(geometry)
                            feat = dict(
                                type='Feature',
                                properties={'area': area, '_fid': f'canopy-{fid}'},
                                geometry=geometry,
                            )
                            try:
                                dst.write(feat)
                            except Exception as e:
                                print('ERROR', e, feat)
                                raise
                            written += 1
                            if written % 10000 == 0:
                                print('DEDUPING:', written, len(used_fids))
                print(f'{len(used_fids)} items, {written} clusters')

            # # Write GPKG file using fiona:
            # schema = dict(
            #     geometry='MultiPolygon',
            #     properties=dict(
            #         area='float',
            #         fid='str',
            #     )
            # )
            #     with fiona.open(canopies_gdb_file, layername=layername) as collection:
            #         for fid, item in collection.items():
            #             if fid not in clusters:
            #                 continue
            #             geometry = shape(item['geometry'])
            #             area = item['properties']['Shape_Area']
            #             if transformer:
            #                 geometry = transform(transformer.transform, geometry)
            #             geometry = mapping(geometry)
            #             feat = dict(
            #                 type='Feature',
            #                 properties={'area': area, 'fid': f'canopy-{fid}'},
            #                 geometry=geometry,
            #             )
            #             dst.write(feat)
            #             if i % 1000 == 0:
            #                 print(f'processed {i} clusters')
            #             i += 1

    print('### Enriching with tree data ###')
    QUERY = '''
    with x as (select "meta-tree-id", array_agg("meta-internal-id") as internal_ids, array_agg("meta-collection-type") as collection_types from trees_processed group by 1)
    select * from x where internal_ids::text like '%%canopy%%'
    '''
    canopy_kind = {}
    rows = DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', query=QUERY, name='trees'),
    ).results()[0][0]
    for row in rows:
        canopy_ids = [x for x in row['internal_ids'] if 'canopy' in x]
        assert len(canopy_ids) > 0, repr(row)
        canopy_id = canopy_ids[0]
        likely = 'חישה מרחוק' in row['collection_types']
        matched = 'סקר רגלי' in row['collection_types']
        kind = 'matched' if matched else ('likely' if likely else 'unknown')
        canopy_kind[canopy_id] = dict(kind=kind)

    geojson_file = 'canopies.geojson'
    with fiona.open(canopies_package, 'r') as src:
        meta = src.meta
        meta['driver'] = 'GeoJSON'
        meta['schema']['properties']['kind'] = 'str'
        meta['schema']['properties']['fid'] = 'str'
        del meta['schema']['properties']['_fid']
        with fiona.open(geojson_file, 'w', **meta) as dst:
            for i, f in enumerate(src.filter()):
                geom = f['geometry']
                properties = dict(f['properties'])
                properties['fid'] = properties.pop('_fid')
                properties['kind'] = canopy_kind.get(properties['fid'], {}).get('kind', 'unknown')
                feat = dict(type="Feature", properties=properties, geometry=geom)
                dst.write(feat)

    print('### Uploading to MapBox ###', geojson_file)
    filename = Path(geojson_file)
    mbtiles_filename = str(filename.with_suffix('.mbtiles'))
    if run_tippecanoe('-z15', str(filename), '-o', mbtiles_filename,  '-l', 'canopies'):
        upload_tileset(mbtiles_filename, 'treebase.canopies', 'Canopy Data')

    filtered_geojson_file = 'extracted_trees.geojson'
    with s3.get_or_create('processed/canopies/extracted_trees.geojson', filtered_geojson_file) as fn:
        if fn:
            print('### Filtering by likely tree ###')
            DF.Flow(
                DF.load(geojson_file),
                geo_props(),
                DF.filter_rows(lambda r: bool(r['likely_tree'])),
                DF.filter_rows(lambda r: r['coords'] is not None),
                DF.set_type('coords', type='geojson', transform=lambda v: json.dumps(v)),
                DF.select_fields(['coords', 'area', 'compactness', 'likely_tree', 'fid']),
                DF.update_resource(-1, name='extracted_trees', path='extracted_trees.geojson'),
                DF.dump_to_path('.', format='geojson'),
            ).process()

    from treebase.data_indexes import index_package
    index_package('cache/canopies/canopies_idx', 'canopies', geojson_file)


def operator(*_):
    main()

if __name__ == "__main__":
    main()