import requests
import shutil
from pathlib import Path
import tempfile
import zipfile
import os
import copy
import pickle

import dataflows as DF

import fiona
from pyproj import Transformer
from shapely.geometry import shape, mapping, MultiPolygon, Point
from shapely.ops import transform, unary_union
from rtree import index

from treebase.s3_utils import S3Utils
from treebase.mapbox_utils import run_tippecanoe, upload_tileset, fetch_tilesets
from treebase.config import CHECKPOINT_PATH

DATACITY_DB_URL = 'postgresql://readonly:readonly@db.datacity.org.il/datasets'


def fix_muni_code(code):
    if len(code) > 2:
        code = '{:0>4}'.format(code)
    return code


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

def package_to_mapbox(key, fn, cache_key, *, tc_args=None, canopies=None, data=None, data_key=None, data_fields=None):
    s3 = S3Utils()
    transformer = Transformer.from_crs('EPSG:4326', 'EPSG:2039', always_xy=True)
    with s3.get_or_create(cache_key, fn) as test:
        assert test is None
        tileset_name = f'treebase.{key}'
        tileset_name_l = f'{tileset_name}_labels'
        tilesets = [x['id'] for x in fetch_tilesets()]
        already_exists = tileset_name in tilesets and tileset_name_l in tilesets 
        if already_exists:
            print(f'Tileset {tileset_name} already exists, replacing')
        print(f'Preparing tileset {tileset_name}')
        with tempfile.TemporaryDirectory() as tmpdir:
            dst_fn = f'{tmpdir}/tmp.geojson'
            dst_l_fn = f'{tmpdir}/tmp_l.geojson'
            with fiona.open(fn) as src:
                meta = src.meta
                meta['driver'] = 'GeoJSON'
                if canopies is not None:
                    meta['schema']['properties']['canopy_area'] = 'float'
                    meta['schema']['properties']['canopy_area_ratio'] = 'float'
                empty_rec = {}
                if data_fields is not None:
                    meta['schema']['properties'].update(data_fields)
                    empty_rec = {k: None for k in data_fields.keys()}
                canopy_per_capita = False
                if 'population' in meta['schema']['properties'] and 'canopy_area' in meta['schema']['properties']:
                    meta['schema']['properties']['canopy_per_capita'] = 'float'
                    canopy_per_capita = True
                meta_l = copy.deepcopy(meta)
                meta_l['schema']['geometry'] = 'Point'
                with fiona.open(dst_fn, 'w', **meta) as dst:
                    with fiona.open(dst_l_fn, 'w', **meta_l) as dst_l:
                        canopies_suffix = '.canopies.pickle'
                        with s3.cache_file(cache_key + canopies_suffix, fn + canopies_suffix) as canopies_cache_fn:
                            try:
                                canopies_cache = pickle.load(open(canopies_cache_fn, 'rb'))
                            except:
                                canopies_cache = {}
                            for i, f in enumerate(src.filter()):
                                geom = shape(f['geometry'])
                                geom_l = geom.centroid
                                props = dict(f['properties'])

                                geom_area = geom.area
                                if transformer is not None:
                                    geom_area = transform(transformer.transform, geom).area
                                if canopies is not None:
                                    canopies_cache_key = '{:.5f}/{:.5f}'.format(geom_l.x, geom_l.y)
                                    if canopies_cache_key not in canopies_cache:
                                        canopy_list = canopies.intersection(geom.bounds, objects='raw')
                                        canopy = unary_union([x['geometry'] for x in canopy_list]).intersection(geom)
                                        if transformer is not None:
                                            canopy = transform(transformer.transform, canopy)
                                        if canopy is not None:
                                            canopy_info = dict(
                                                canopy_area=canopy.area,
                                                canopy_area_ratio=canopy.area / geom_area,
                                            )
                                            canopies_cache[canopies_cache_key] = canopy_info
                                    props.update(canopies_cache.get(canopies_cache_key, {}))
                                if data is not None and data_key is not None:
                                    for d in data:
                                        props.update(d.get(props[data_key], empty_rec))
                                if canopy_per_capita:
                                    props['canopy_per_capita'] = 0
                                    if 'population' in props and 'canopy_area' in props:
                                        if props['population'] and props['canopy_area']:
                                            props['canopy_per_capita'] = props['canopy_area'] / props['population']
                                dst.write(dict(
                                    type='Feature',
                                    geometry=mapping(geom),
                                    properties=props,
                                ))
                                dst_l.write(dict(
                                    type='Feature',
                                    geometry=mapping(geom_l),
                                    properties=props,
                                ))

                                props['area'] = geom_area
                                props['bounds'] = list(geom.bounds)
                                props['center'] = list(geom.centroid.coords[0])
                                yield props
                                if i % 1000 == 0:
                                    print(f'{key}: Processed {i} features')
                                pickle.dump(canopies_cache, open(canopies_cache_fn, 'wb'))
            mbt = f'{tmpdir}/tmp.mbtiles'
            print(f'Running tippecanoe tileset {tileset_name}')
            if run_tippecanoe('-z13', '-o', mbt,  '-l', key, *tc_args, dst_fn):
                print(f'Now uploading tileset {tileset_name}')
                upload_tileset(mbt, tileset_name, key)
            else:
                raise Exception('Failed to run tippecanoe')
            mbt_l = f'{tmpdir}/tmp_l.mbtiles'
            key = f'{key}_labels'
            tileset_name = tileset_name_l
            if run_tippecanoe('-z13', '-o', mbt_l,  '-l', key, *tc_args, dst_l_fn):
                print(f'Now uploading tileset {tileset_name}')
                upload_tileset(mbt_l, tileset_name, key)
            else:
                raise Exception('Failed to run tippecanoe')

def upload_package(key, fn, cache_key, *, tc_args=None, canopies=None, data=None, data_key=None, data_fields=None):
    steps = []
    if data_fields is not None:
        for k, v in data_fields.items():
            if v == 'float':
                steps.append(DF.add_field(k, 'number'))
            elif v == 'int':
                steps.append(DF.add_field(k, 'integer'))
            elif v == 'str':
                steps.append(DF.add_field(k, 'string'))
    DF.Flow(
        package_to_mapbox(key, fn, cache_key, tc_args=tc_args or [], canopies=canopies, data=data, data_key=data_key, data_fields=data_fields),
        DF.update_resource(-1, name=key),
        DF.checkpoint('upload-package-' + key, CHECKPOINT_PATH),
    ).process()
    DF.Flow(
        DF.checkpoint('upload-package-' + key, CHECKPOINT_PATH),
        DF.dump_to_sql({
            key: {
                'resource-name': key,
            }
        }, 'env://DATASETS_DATABASE_URL')
    ).process()

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

                        muni_code = properties['REG_MUN_ID']
                        muni_name = convert_name(properties['REG_MUN_NA'])
                        city_code = properties['LOCALITY_I']
                        city_name = convert_name(properties['LOCALITY_N'])
                        if not muni_code:
                            muni_code = fix_muni_code(str(city_code))
                            muni_name = city_name
                            city_code = None
                            city_name = None
                        else:
                            muni_code = str(muni_code)
                        if city_code:
                            city_code = str(city_code)

                        properties = dict(
                            code='{}/{}'.format(properties['GUSH_NUM'], properties['PARCEL']),
                            gush=str(properties['GUSH_NUM']),
                            parcel=str(properties['PARCEL']),
                            muni_code=muni_code,
                            muni_name=muni_name,
                            city_code=city_code,
                            city_name=city_name,
                        )
                        feat = dict(type="Feature", properties=properties, geometry=geom)
                        try:
                            dst.write(feat)
                        except:
                            print('Failed to write', feat)
                            raise
                        if i % 10000 == 0:
                            print('Wrote', i, 'features')   

    return index_package('cache/parcels/parcels_idx', 'parcels', 'parcels.gpkg')

def muni_extra_info():
    FILENAME = 'munis_extra_info.pickle'
    s3 = S3Utils()
    with s3.get_or_create('cache/munis/munis_extra_info.pickle', FILENAME) as fn_:
        if fn_ is not None:
            QUERY = '''
                with d as (SELECT YEAR,
                    name,
                    header,
                    value
                FROM lamas_muni
                WHERE header IN ('דמוגרפיה - אוכלוסייה (סה"כ)',
                                'גיאוגרפיה - סך הכל שטח (קמ"ר)',
                                'כללי - סמל הרשות',
                                'מדד חברתי-כלכלי - אשכול (מ-1 עד 10, 1 הנמוך ביותר)') group by 1,2,3,4),
                m as (select header, name, max(year) as maxyear from d group by 1, 2)
                select m.name, m.header, maxyear, d.value from m join d on (year=maxyear and d.name=m.name and d.header=m.header)
            '''
            muni_data =DF.Flow(
                DF.load(DATACITY_DB_URL, query=QUERY),
                DF.checkpoint('muni-extra-info', CHECKPOINT_PATH),
                DF.update_resource(-1, name='muni-extra-info'),
                DF.add_field('item', 'array', lambda row: [row['header'], row['value']]),
                DF.join_with_self('muni-extra-info', ['name'], dict(
                    muni_name=dict(name='name'),
                    props=dict(name='item', aggregate='array')
                )),
                DF.set_type('props', type='object', transform=dict),
                DF.add_field('muni_code', 'string', lambda row: fix_muni_code(row['props'].get('כללי - סמל הרשות'))),
                DF.add_field('population', 'integer', lambda row: row['props'].get('דמוגרפיה - אוכלוסייה (סה"כ)')),
                DF.add_field('area', 'number', lambda row: row['props'].get('גיאוגרפיה - סך הכל שטח (קמ"ר)')),
                DF.add_field('socioeconomic_index', 'number', lambda row: row['props'].get('מדד חברתי-כלכלי - אשכול (מ-1 עד 10, 1 הנמוך ביותר)')),
                DF.validate(),
                DF.delete_fields(['props', 'muni_name']),
                DF.add_field('population_density', 'number', lambda row: row['population'] / row['area'] if row['area'] and row['population'] else None),
                DF.printer()
            ).results()[0][0]
            for row in muni_data:
                row['population_density'] = float(row['population_density']) if row['population_density'] else None
                row['area'] = float(row['area']) if row['area'] else None                
                row['population'] = int(row['population']) if row['population'] else None
                row['socioeconomic_index'] = int(row['socioeconomic_index']) if row['socioeconomic_index'] else None

            muni_data = dict((row.pop('muni_code'), row) for row in muni_data)
            with open(fn_, 'wb') as out:
                pickle.dump(muni_data, out)
        ret = pickle.load(open(FILENAME, 'rb'))
        print('Loaded', len(ret), 'muni extra info')
        return ret


def munis_index():
    s3 = S3Utils()
    muni_map = {}
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
                for i, f in enumerate(src.filter()):
                    geom = shape(f['geometry'])
                    geom = transform(transformer.transform, geom)
                    properties = dict(f['properties'])
                    code = properties['CR_PNIM']
                    if code.startswith('55'):
                        code = code[2:]
                    if code.startswith('99'):
                        continue
                    properties = dict(
                        muni_code=fix_muni_code(code),
                        muni_name=convert_name(properties['Muni_Heb'], 'utf8'),
                        muni_name_en=properties['Muni_Eng'],
                        muni_region=convert_name(properties['Machoz'], 'utf8'),
                    )
                    if 'ללא שיפוט' in properties['muni_name']:
                        continue
                    print(repr(properties))
                    muni_map.setdefault(code, dict(props={}, geoms=[]))['props'] = properties
                    muni_map[code]['geoms'].append(geom)
                with fiona.open(fn_, 'w',
                                driver='GPKG',
                                crs='EPSG:4326',
                                schema=schema) as dst:
                    for item in muni_map.values():
                        geom = unary_union(item['geoms'])
                        if geom.geom_type == 'Polygon':
                            geom = MultiPolygon([geom])
                        geom = mapping(geom)
                        feat = dict(type="Feature", properties=item['props'], geometry=geom)
                        try:
                            dst.write(feat)
                        except:
                            print('Failed to write', feat)
                            raise
                        if i % 10 == 0:
                            print('Wrote', i, 'features')   

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
                with fiona.open(str(Path(temp_dir, 'gis_osm_roads_free_1.shp')), layer='gis_osm_roads_free_1') as src:
                    inv_transformer = Transformer.from_crs('EPSG:4326', 'EPSG:2039', always_xy=True)
                    transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)

                    aggregate = dict()
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
                        if properties['name'] == 'הגדוד העברי':
                            print('Found', properties['name'], 'in', muni)
                        if not muni:
                            continue
                        geom = transform(inv_transformer.transform, geom)
                        geom = geom.buffer(10)
                        geom = transform(transformer.transform, geom)
                        properties = dict(
                            road_type=properties['fclass'],
                            road_name=properties['name'],
                            muni_code=muni['muni_code'],
                            muni_name=muni['muni_name'],
                            road_id=properties['name'] + ', ' + muni['muni_name'],
                        )
                        aggregate.setdefault(properties['road_id'], dict(props={}, geoms=[]))['props'] = properties
                        aggregate[properties['road_id']]['geoms'].append(geom)
                        if i % 10000 == 0:
                            print('Processed', i, 'features')   

                schema = dict(
                    geometry='MultiPolygon',
                    properties=dict(
                        road_type='str',
                        road_name='str',
                        muni_code='str',
                        muni_name='str',
                        road_id='str',
                    )
                )
                with fiona.open(fn, 'w',
                                driver='GPKG',
                                crs='EPSG:4326',
                                schema=schema) as dst:
                    for i, v in enumerate(aggregate.values()):
                        geoms = v['geoms']
                        properties = v['props']
                        geom = unary_union(geoms)
                        if geom.geom_type == 'Polygon':
                            geom = MultiPolygon([geom])
                        geom = mapping(geom)
                        feat = dict(type="Feature", properties=properties, geometry=geom)
                        try:
                            dst.write(feat)
                        except:
                            print('Failed to write', feat)
                            raise
                        if i % 10000 == 0:
                            print('Wrote', i, 'features')   

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
                    if i % 100000 == 0:
                        print(index_name, ': Got Point', x, y)
                        print(index_name, ': Got Props', props)
                        print(index_name, ': Matched', i, 'rows')
                    yield row
                idx.close()
    return func

def data_quality_score(field_name):
    QUERY = f'''
    WITH t as (SELECT "{field_name}", count(1) as total FROM trees_processed GROUP BY 1),
         d as (SELECT "{field_name}", "meta-collection-type" || '/' || "meta-source-type" as key, count(1) as count FROM trees_processed GROUP BY 1, 2)
    SELECT "{field_name}" as key, count(distinct key) as count FROM d join t using ("{field_name}") where count > total/20 GROUP BY 1
    '''
    quality_data =DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', query=QUERY, infer_strategy=DF.load.INFER_STRINGS, cast_strategy=DF.load.CAST_DO_NOTHING),
        DF.set_type('count', type='integer'),
        DF.checkpoint(f'{field_name}-quality', CHECKPOINT_PATH),
        DF.update_resource(-1, name='quality'),
    ).results()[0][0]
    print('GOT QUALITY DATA for ', field_name, len(quality_data), quality_data[:10])
    items = {row['key']: dict(quality_score=row['count']) for row in quality_data}
    return items

def prepare_indexes():
    stat_areas_index()
    parcels_index()
    muni_idx = munis_index()
    roads_index(muni_idx)

def upload_to_mapbox():
    canopies = index_package('cache/canopies/canopies_idx', 'canopies', 'nonexistent')
    upload_package('munis', 'munis.gpkg', 'cache/munis/munis.gpkg', canopies=canopies,
                   data=[muni_extra_info(), data_quality_score('muni_code')], data_key='muni_code', data_fields=dict(
                        population='int',
                        population_density='float',
                        area='float',
                        socioeconomic_index='int',
                        quality_score='int',
                   ))
    upload_package('parcels', 'parcels.gpkg', 'cache/parcels/parcels.gpkg', tc_args=['--minimum-zoom=10'])
    upload_package('stat_areas', 'stat_areas.gpkg', 'cache/stat_areas/stat_areas.gpkg', canopies=canopies,
                    data=[data_quality_score('stat_area_code')], data_key='code', data_fields=dict(
                        quality_score='int',
                   ))
    upload_package('roads', 'roads.gpkg', 'cache/roads/roads.gpkg')

if __name__ == '__main__':
    # prepare_indexes()
    upload_to_mapbox()
    # muni_extra_info()