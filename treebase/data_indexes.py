import requests
import shutil
from pathlib import Path
import tempfile
import zipfile
import os

import fiona
from pyproj import Transformer
from shapely.geometry import shape, mapping, MultiPolygon, Point
from shapely.ops import transform, unary_union

from rtree import index

from treebase.s3_utils import S3Utils
from treebase.mapbox_utils import run_tippecanoe, upload_tileset, fetch_tilesets


def index_package(key, fn, gpkg):
    s3 = S3Utils()
    with s3.get_or_create('{}.dat'.format(key), '{}.dat'.format(fn)) as fn_:
        with s3.get_or_create('{}.idx'.format(key), '{}.idx'.format(fn)) as fn__:
            if None not in (fn_, fn__):
                idx = index.Index('./{}'.format(fn))
                with fiona.open(gpkg) as src:
                    for i, f in enumerate(src.filter()):
                        geom = shape(f['geometry'])
                        props = dict(f['properties'])
                        rec = dict(
                            props=props,
                            geometry=geom,
                        )
                        idx.insert(i, geom.bounds, rec)
                idx.close()
    return index.Index('./' + fn)

def package_to_mapbox(key, fn, *args):
    # Use fiona to convert the gpkg to geojson
    tileset_name = f'treebase.{key}'
    tilesets = [x['id'] for x in fetch_tilesets()]
    if tileset_name in tilesets:
        print(f'Tileset {tileset_name} already exists, skipping')
        return
    print(f'Preparing tileset {tileset_name}')
    with tempfile.TemporaryDirectory() as tmpdir:
        dst_fn = f'{tmpdir}/tmp.geojson'
        with fiona.open(fn) as src:
            meta = src.meta
            meta['driver'] = 'GeoJSON'
            with fiona.open(dst_fn, 'w', **meta) as dst:
                for i, f in enumerate(src.filter()):
                    geom = shape(f['geometry'])
                    props = dict(f['properties'])
                    dst.write(dict(
                        type='Feature',
                        geometry=mapping(geom),
                        properties=props,
                    ))
        dst.flush()
        mbt = f'{tmpdir}/tmp.mbtiles'
        print(f'Running tippecanoe tileset {tileset_name}')
        if run_tippecanoe('-z13', '-o', mbt,  '-l', key, *args, dst_fn):
            print(f'Now uploading tileset {tileset_name}')
            upload_tileset(mbt, tileset_name, key)
        else:
            raise Exception('Failed to run tippecanoe')


def stat_areas_index():
    s3 = S3Utils()
    with s3.get_or_create('cache/stat_areas/stat_areas.gpkg', 'stat_areas.gpkg') as fn_:
        if fn_ is not None:
            URL = 'https://www.cbs.gov.il/he/mediarelease/doclib/2022/026/ezorim_statistiim_2022.gdb.zip'
            temp_fn = Path('ezorim_statistiim_2022.gdb.zip')
            print('Downloading', URL)
            with requests.get(URL, stream=True) as r:
                with temp_fn.open('wb') as f:
                    shutil.copyfileobj(r.raw, f)

            # Create gpkg file with all the features converted to WGS84 using pyproj
            src = fiona.open(str(temp_fn), layer='statistical_areas_2022')
            transformer = Transformer.from_crs(src.crs, 'EPSG:4326', always_xy=True)
            schema = dict(
                geometry='MultiPolygon',
                properties=dict(
                    code='str',
                    city_code='int',
                    city_name='str',
                    city_name_en='str',
                    area_code='int',
                    rova_name='str',
                    tat_rova_name='str',
                )
            )
            with fiona.open(fn_, 'w',
                            driver='GPKG',
                            crs='EPSG:4326',
                            schema=schema) as dst:
                for i, f in enumerate(src.filter()):
                    geom = shape(f['geometry'])
                    geom = transform(transformer.transform, geom)
                    geom = mapping(geom)
                    properties = dict(f['properties'])
                    properties = dict(
                        code=str(properties['YISHUV_STAT_2022']),
                        city_code=properties['SEMEL_YISHUV'],
                        city_name=properties['SHEM_YISHUV'],
                        city_name_en=(properties['SHEM_YISHUV_ENGLISH'] or '').title() or None,
                        area_code=properties['STAT_2022'],
                        rova_name=properties['ROVA'],
                        tat_rova_name=properties['TAT_ROVA'],
                    )
                    # print(properties)
                    feat = dict(type="Feature", properties=properties, geometry=geom)
                    dst.write(feat)
                    if i % 10000 == 0:
                        print('Wrote', i, 'features')

    package_to_mapbox('stat_areas', 'stat_areas.gpkg')
    return index_package('cache/stat_areas/stat_areas_idx', 'stat_areas', 'stat_areas.gpkg')

def convert_name(name, charset='windows-1255'):
    if name is not None:
        name = bytes(ord(c) for c in name).decode(charset)
    return name

def parcels_index():
    s3 = S3Utils()
    with s3.get_or_create('cache/parcels/parcels.gpkg', 'parcels.gpkg') as fn_:
        if fn_ is not None:
            URL = 'https://data.gov.il/dataset/shape/resource/c68b4df6-c809-4bb5-a546-61fa1528fed5/download/parcel_all.zip'
            # Create temp dir and download the url into a file there
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_fn = Path(temp_dir, 'parcel_all.zip')
                print('Downloading', URL)
                with requests.get(URL, stream=True, headers={'User-Agent': 'datagov-external-client'}) as r:
                    with temp_fn.open('wb') as f:
                        shutil.copyfileobj(r.raw, f)

                print('Got', temp_fn)
                # Unzip the file
                try:
                    with zipfile.ZipFile(temp_fn, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                except zipfile.BadZipFile:
                    print('Bad zip file', temp_fn, open(temp_fn, 'rb').read()[:200])
                    raise
                print('Unzipped', temp_fn)

                # Create gpkg file with all the features converted to WGS84 using pyproj
                src = fiona.open(str(Path(temp_dir, 'PARCEL_ALL.shp')), layer='PARCEL_ALL')
                transformer = Transformer.from_crs(src.crs, 'EPSG:4326', always_xy=True)
                schema = dict(
                    geometry='MultiPolygon',
                    properties=dict(
                        code='str',
                        gush='str',
                        parcel='str',
                        city_code='str',
                        city_name='str',
                        muni_code='str',
                        muni_name='str',
                    )
                )
                with fiona.open(fn_, 'w',
                                driver='GPKG',
                                crs='EPSG:4326',
                                schema=schema) as dst:
                    for i, f in enumerate(src.filter()):
                        geom = shape(f['geometry'])
                        geom = transform(transformer.transform, geom)
                        if geom.geom_type == 'Polygon':
                            geom = MultiPolygon([geom])
                        geom = mapping(geom)
                        properties = dict(f['properties'])
                        properties = dict(
                            code='{}/{}'.format(properties['GUSH_NUM'], properties['PARCEL']),
                            gush=str(properties['GUSH_NUM']),
                            parcel=str(properties['PARCEL']),
                            city_code=str(properties['LOCALITY_I']),
                            city_name=convert_name(properties['LOCALITY_N']),
                            muni_code=str(properties['REG_MUN_ID']),
                            muni_name=convert_name(properties['REG_MUN_NA']),
                        )
                        feat = dict(type="Feature", properties=properties, geometry=geom)
                        try:
                            dst.write(feat)
                        except:
                            print('Failed to write', feat)
                            raise
                        if i % 10000 == 0:
                            print('Wrote', i, 'features')   

    package_to_mapbox('parcels', 'parcels.gpkg', '--minimum-zoom=10')
    return index_package('cache/parcels/parcels_idx', 'parcels', 'parcels.gpkg')


def munis_index():
    s3 = S3Utils()
    with s3.get_or_create('cache/munis/munis.gpkg', 'munis.gpkg') as fn_:
        if fn_ is not None:
            URL = 'https://www.gov.il/files/moin/GvulotShiput.zip'
            # Create temp dir and download the url into a file there
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_fn = Path(temp_dir, 'GvulotShiput.zip')
                print('Downloading', URL)
                with requests.get(URL, stream=True, headers={'User-Agent': 'datagov-external-client'}) as r:
                    with temp_fn.open('wb') as f:
                        shutil.copyfileobj(r.raw, f)

                print('Got', temp_fn)
                # Unzip the file
                try:
                    with zipfile.ZipFile(temp_fn, 'r') as zip_ref:
                        for zi in zip_ref.infolist():
                            fn = zi.filename
                            if 'muni_il' in fn:
                                fn = fn[fn.find('muni_il'):]
                                zi.filename = fn
                                zip_ref.extract(zi, temp_dir)
                                print('Extracted', zi.filename, 'to', temp_dir)
                    print(list(os.walk(temp_dir)))
                except zipfile.BadZipFile:
                    print('Bad zip file', temp_fn, open(temp_fn, 'rb').read()[:200])
                    raise
                print('Unzipped', temp_fn)

                # Create gpkg file with all the features converted to WGS84 using pyproj
                src = fiona.open(str(Path(temp_dir, 'muni_il.shp')), layer='muni_il')
                transformer = Transformer.from_crs(src.crs, 'EPSG:4326', always_xy=True)
                schema = dict(
                    geometry='MultiPolygon',
                    properties=dict(
                        muni_code='str',
                        muni_name='str',
                        muni_name_en='str',
                        muni_region='str',
                    )
                )
                with fiona.open(fn_, 'w',
                                driver='GPKG',
                                crs='EPSG:4326',
                                schema=schema) as dst:
                    for i, f in enumerate(src.filter()):
                        geom = shape(f['geometry'])
                        geom = transform(transformer.transform, geom)
                        if geom.geom_type == 'Polygon':
                            geom = MultiPolygon([geom])
                        geom = mapping(geom)
                        properties = dict(f['properties'])
                        properties = dict(
                            muni_code=properties['CR_LAMAS'],
                            muni_name=convert_name(properties['Muni_Heb'], 'utf8'),
                            muni_name_en=properties['Muni_Eng'],
                            muni_region=convert_name(properties['Machoz'], 'utf8'),
                        )
                        print(repr(properties))
                        feat = dict(type="Feature", properties=properties, geometry=geom)
                        try:
                            dst.write(feat)
                        except:
                            print('Failed to write', feat)
                            raise
                        if i % 10 == 0:
                            print('Wrote', i, 'features')   

    package_to_mapbox('munis', 'munis.gpkg')
    return index_package('cache/munis/munis_idx', 'munis', 'munis.gpkg')


def roads_index(muni_index: index.Index):
    s3 = S3Utils()
    with s3.get_or_create('cache/roads/roads.gpkg', 'roads.gpkg') as fn:
        if fn is not None:
            URL = 'https://download.geofabrik.de/asia/israel-and-palestine-latest-free.shp.zip'
            # Create temp dir and download the url into a file there
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_fn = Path(temp_dir, 'israel_all.zip')
                print('Downloading', URL)
                with requests.get(URL, stream=True) as r:
                    with temp_fn.open('wb') as f:
                        shutil.copyfileobj(r.raw, f)

                print('Got', temp_fn)
                # Unzip the file
                try:
                    with zipfile.ZipFile(temp_fn, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                except zipfile.BadZipFile:
                    print('Bad zip file', temp_fn, open(temp_fn, 'rb').read()[:200])
                    raise
                print('Unzipped', temp_fn)

                # Create gpkg file with all the features converted to WGS84 using pyproj
                src = fiona.open(str(Path(temp_dir, 'gis_osm_roads_free_1.shp')), layer='gis_osm_roads_free_1')
                inv_transformer = Transformer.from_crs('EPSG:4326', 'EPSG:2039', always_xy=True)
                transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)
                schema = dict(
                    geometry='Polygon',
                    properties=dict(
                        road_type='str',
                        road_name='str',
                        muni_code='str',
                        muni_name='str',
                    )
                )
                with fiona.open(fn, 'w',
                                driver='GPKG',
                                crs='EPSG:4326',
                                schema=schema) as dst:
                    for i, f in enumerate(src.filter()):
                        properties = dict(f['properties'])
                        if properties['fclass'] == 'path' or not properties['name']:
                            continue
                        geom = shape(f['geometry'])
                        muni = None
                        munis = muni_index.intersection(geom.bounds, objects=True)
                        for m in munis:
                            if geom.intersects(m.object['geometry']):
                                muni = m.object['props']
                                break
                        if not muni:
                            continue
                        geom = transform(inv_transformer.transform, geom)
                        geom = geom.buffer(10)
                        geom = transform(transformer.transform, geom)
                        # if geom.geom_type == 'Polygon':
                        #     geom = MultiPolygon([geom])
                        geom = mapping(geom)
                        properties = dict(
                            road_type=properties['fclass'],
                            road_name=properties['name'],
                            muni_code=muni['muni_code'],
                            muni_name=muni['muni_name'],
                        )
                        feat = dict(type="Feature", properties=properties, geometry=geom)
                        try:
                            dst.write(feat)
                        except:
                            print('Failed to write', feat)
                            raise
                        if i % 10000 == 0:
                            print('Wrote', i, 'features')   

    package_to_mapbox('roads', 'roads.gpkg')
    return index_package('cache/roads/roads_idx', 'roads', 'roads.gpkg')



def match_rows(index_name, fields):
    def func(rows):
        s3 = S3Utils()
        key = 'cache/{}/{}_idx'.format(index_name, index_name)
        with s3.get_or_create('{}.dat'.format(key), '{}.dat'.format(index_name)) as fn_:
            with s3.get_or_create('{}.idx'.format(key), '{}.idx'.format(index_name)) as fn__:
                print(index_name, ': Got Index Files', fn_, fn__)
                assert fn_ is None and fn__ is None, 'Failed to get index {}, files: {} & {}'.format(index_name, fn_, fn__)
                idx = index.Index('./{}'.format(index_name))
                print(index_name, ': Got Index', idx)
                for i, row in enumerate(rows):
                    x, y = float(row['location-x']), float(row['location-y'])
                    p = Point(x, y)
                    props = None
                    for item in list(idx.intersection((x, y, x, y), objects='raw')):
                        if item['geometry'].contains(p):
                            props = item['props']
                            break
                    if props:
                        for k, v in fields.items():
                            row[k] = props.get(v)
                    else:
                        for k in fields.keys():
                            row[k] = None
                    if i % 10000 == 0:
                        print(index_name, ': Got Point', x, y)
                        print(index_name, ': Got Props', props)
                        print(index_name, ': Matched', i, 'rows')
                    yield row
                idx.close()
    return func


def prepare_indexes():
    stat_areas_index()
    parcels_index()
    muni_idx = munis_index()
    roads_index(muni_idx)

if __name__ == '__main__':
    prepare_indexes()