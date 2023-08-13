from pathlib import Path
import shutil
import dataflows as DF
import re
import json

# from dataflows_ckan import dump_to_ckan
from rtree import index
from openlocationcode import openlocationcode as olc
from shapely.geometry import Point
from pyproj import Transformer

from dataflows_airtable import load_from_airtable

from thefuzz.process import extractOne

from treebase.mapbox_utils import run_tippecanoe, upload_tileset
from treebase.log import logger
from treebase.geo_utils import bbox_diffs
from treebase.s3_utils import S3Utils
from treebase.data_indexes import match_rows, upload_to_mapbox
from treebase.config import CHECKPOINT_PATH

SEARCH_RADIUS = 3

transformer = Transformer.from_crs('epsg:4326', 'epsg:2039', always_xy=True)

def spatial_index(idx):
    def func(rows):
        for i, row in enumerate(rows):
            x = row['location-x']
            y = row['location-y']
            lon, lat = transformer.transform(x, y)
            idx.insert(i, (lon, lat), obj=dict(source=row['_source'], idx=i, point=Point(lon, lat)))
            row['idx'] = i
            row['location-x-il'] = lon
            row['location-y-il'] = lat
            yield row
            if i % 10000 == 0:
                print('INDEXED', i)
        print('INDEXED TOTAL', i)

    return DF.Flow(
        DF.add_field('idx', 'integer'),
        DF.add_field('location-x-il', 'number'),
        DF.add_field('location-y-il', 'number'),
        func
    )


def match_index(idx: index.Index, matched):
    def func(rows):
        clusters = dict()
        for row in rows:
            if row['idx'] not in matched:
                lon, lat = row['location-x'], row['location-y']
                if row['meta-collection-type'] != 'חישה מרחוק':
                    x, y = float(row['location-x-il']), float(row['location-y-il'])
                    p = Point(x, y)
                    minimums = dict()
                    minimums[row['_source']] = (0, row['idx'])
                    for i in idx.intersection((x-SEARCH_RADIUS, y-SEARCH_RADIUS, x+SEARCH_RADIUS, y+SEARCH_RADIUS), objects='raw'):
                        if i['idx'] in matched:
                            continue
                        i_source = i['source']
                        if i_source == row['_source']:
                            continue
                        d = p.distance(i['point'])
                        if d < SEARCH_RADIUS:
                            minimums.setdefault(i_source, (SEARCH_RADIUS, 0))
                            if d < minimums[i_source][0]:
                                minimums[i_source] = d, i['idx']
                    ids = list(id for _, id in minimums.values())
                else:
                    ids = [row['idx']]
                row['meta-tree-id'] = olc.encode(lat, lon, 12)
                if len(ids) > 1:
                    clusters[row['meta-tree-id']] = len(ids)
                    for i in ids:
                        matched[i] = row['meta-tree-id']
                    if len(clusters) % 10000 == 0:
                        print('MATCHED #', len(clusters), ':', row['idx'], '->', ids)
                row['cluster-size'] = len(ids)
            else:
                treeid = matched[row['idx']]
                row['meta-tree-id'] = treeid
                row['cluster-size'] = clusters[treeid]
            yield row
        print('#CLUSTERS', len(clusters))
    return DF.Flow(
        DF.add_field('cluster-size', 'integer'),
        func,
    )

def clean_species():

    WORDS = re.compile(r'[a-zA-Zא-ת]+')
    table = DF.Flow(
        load_from_airtable('appHaG591cVK21CRl', 'Genus', 'Grid view', 'env://AIRTABLE_API_TOKEN'),
        DF.add_field('attributes-species-clean-en', 'string', lambda r: r['id']),
        DF.add_field('attributes-species-clean-he', 'string', lambda r: r['name']),
        DF.select_fields(['attributes-species-clean-en', 'attributes-species-clean-he']),
    ).results()[0][0]
    options = dict([
        (r['attributes-species-clean-en'].lower(), r)
        for r in table
    ] + [
        (r['attributes-species-clean-he'], r)
        for r in table
    ] + [
        (r['attributes-species-clean-en'] + ' ' + r['attributes-species-clean-he'], r)
        for r in table
    ])
    option_keys = list(options.keys())

    def func(rows):
        s3 = S3Utils()
        with s3.cache_file('cache/genus_cleanup/cache.json', 'genus_cleanup.json') as fn:
            try:
                cache = json.load(open(fn))
            except:
                cache = dict()
            for row in rows:
                species = row.get('attributes-species')
                if species:
                    species = ' '.join(WORDS.findall(species.lower()))
                    if species in cache:
                        row.update(cache[species])
                    else:
                        found = extractOne(species, option_keys, score_cutoff=80)
                        if found:
                            best, _ = found
                            option = options[best]
                            row.update(option)
                            cache[species] = option
                        else:
                            cache[species] = dict()
                yield row
            json.dump(cache, open(fn, 'w'))


    return DF.Flow(
        DF.add_field('attributes-species-clean-en', 'string'),
        DF.add_field('attributes-species-clean-he', 'string'),
        func
    )

def collect_duplicates(unique_set):
    def func(rows):
        count = 0
        for row in rows:
            key = (row['location-x'], row['location-y'], row['meta-source'])
            unique_set.add(key)
            yield row
            count += 1
        print(f'UNIQUE {len(unique_set)}/{count}')
    return func

def deduplicate(unique_set):
    def func(rows):
        dropped = 0
        for row in rows:
            key = (row['location-x'], row['location-y'], row['meta-source'])
            if key in unique_set:
                unique_set.remove(key)
                yield row
            else:
                dropped += 1
        print(f'DROPPED {dropped}')

    return func

def main(local=False):
    logger.info('PROCESSING TREE DATASET')
    shutil.rmtree(CHECKPOINT_PATH, ignore_errors=True, onerror=None)

    print('### Loading data and processing ###')
    unique_set = set()
    DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', format='sql', table='trees', query='SELECT * FROM trees'),
        # DF.load('trees.csv'),
        DF.update_resource(-1, name='trees', path='trees.csv'),
        DF.set_type('meta-internal-id', type='string', transform=str),
        DF.add_field('coords', 'geopoint', lambda r: [float(r['location-x']), float(r['location-y'])]),
        collect_duplicates(unique_set),
        DF.checkpoint('tree-processing', CHECKPOINT_PATH),
    ).process()

    print('### geo-indexing ###')
    idx = index.Index()
    DF.Flow(
        DF.checkpoint('tree-processing', CHECKPOINT_PATH),
        deduplicate(unique_set),
        clean_species(),
        DF.add_field('meta-collection-type-idx', 'integer', lambda r: 1 if r['meta-collection-type'] == 'חישה מרחוק' else 0),
        DF.sort_rows('{meta-collection-type-idx}'),
        DF.delete_fields(['meta-collection-type-idx']),
        spatial_index(idx),
        DF.checkpoint('tree-deduping', CHECKPOINT_PATH),
    ).process()

    print('### DeDuping and assigning TreeId ###')

    matched = dict()
    DF.Flow(
        DF.checkpoint('tree-deduping', CHECKPOINT_PATH),
        match_index(idx, matched),
        DF.add_field('cad_code', 'string'),
        DF.add_field('cad_gush', 'string'),
        DF.add_field('cad_parcel', 'string'),
        match_rows('parcels', dict(
            cad_code='code',
            cad_gush='gush',
            cad_parcel='parcel',
        )),
        DF.add_field('stat_area_code', 'string'),
        match_rows('stat_areas', dict(
            stat_area_code='code',
        )),
        DF.add_field('muni_code', 'string'),
        DF.add_field('muni_name', 'string'),
        DF.add_field('muni_name_en', 'string'),
        DF.add_field('muni_region', 'string'),
        match_rows('munis', dict(
            muni_code='muni_code',
            muni_name='muni_name',
            muni_name_en='muni_name_en',
            muni_region='muni_region',
        )),
        DF.add_field('road_name', 'string'),
        DF.add_field('road_type', 'string'),
        DF.add_field('road_id', 'string'),
        match_rows('roads', dict(
            road_name='road_name',
            road_type='road_type',
            road_id='road_id',
        )),
        DF.checkpoint('tree-processing-clusters', CHECKPOINT_PATH)
    ).process()

    print('### Saving result to GeoJSON ###')
    DF.Flow(
        DF.checkpoint('tree-processing-clusters', CHECKPOINT_PATH),
        DF.dump_to_path(f'{CHECKPOINT_PATH}/trees-full', format='csv'),
        DF.dump_to_path(f'{CHECKPOINT_PATH}/trees-full', format='geojson'),
        DF.select_fields(['coords', 'meta-tree-id', 'meta-source', 'attributes-species-clean-he', 'attributes-species-clean-en', 
                          'road_id', 'muni_code', 'stat_area_code', 'cad_code',
                          'attributes-canopy-area', 'attributes-height', 'attributes-bark-diameter',
                          'meta-collection-type', 'meta-source-type']),
        DF.add_field('joint-source-type', type='string', default=lambda row: f'{row["meta-collection-type"]}/{row["meta-source-type"]}'),
        DF.join_with_self('trees', ['meta-tree-id'], fields={
            'tree-id': dict(name='meta-tree-id'),
            'species_he': dict(name='attributes-species-clean-he'),
            'species_en': dict(name='attributes-species-clean-en'),
            'road': dict(name='road_id'),
            'muni': dict(name='muni_code'),
            'stat_area': dict(name='stat_area_code'),
            'cad': dict(name='cad_code'),
            'coords': None,
            'sources': dict(name='meta-source', aggregate='set'),
            'collection': dict(name='meta-collection-type', aggregate='set'),
            'joint-source-type': dict(name='joint-source-type', aggregate='set'),
            'canopy_area': dict(name='attributes-canopy-area', aggregate='max'),
            'height': dict(name='attributes-height', aggregate='max'),
            'bark_diameter': dict(name='attributes-bark-diameter', aggregate='max'),
        }),
        DF.add_field('certainty', type='boolean', default=lambda row: 'סקר רגלי' in row['collection']),
        DF.add_field('unreported', type='boolean', default=lambda row: 'סקר רגלי/מוניציפלי' in row['joint-source-type'] and 'סקר רגלי/ממשלתי' not in row['joint-source-type']),
        DF.delete_fields(['joint-source-type']),
        DF.set_type('collection', type='string', transform=lambda v: ', '.join(v)),
        DF.set_type('sources', type='string', transform=lambda v: ', '.join(v)),
        DF.dump_to_path(f'{CHECKPOINT_PATH}/trees-compact', format='geojson'),
    ).process()

    s3 = S3Utils()
    s3.upload(f'{CHECKPOINT_PATH}/trees-full/trees.csv', 'processed/trees/trees.csv')
    s3.upload(f'{CHECKPOINT_PATH}/trees-full/trees.geojson', 'processed/trees/trees.geojson')

    print('### Uploading trees to MapBox ###')
    filename = Path(f'{CHECKPOINT_PATH}/trees-compact/data/trees.geojson')
    mbtiles_filename = str(filename.with_suffix('.mbtiles'))
    if run_tippecanoe('-z15', str(filename), '-o', mbtiles_filename,  '-l', 'trees'):
        upload_tileset(mbtiles_filename, 'treebase.trees', 'Tree Data')

    print('### Dump to DB ###')
    DF.Flow(
        DF.checkpoint('tree-processing-clusters', CHECKPOINT_PATH),
        DF.dump_to_sql(dict(
            trees_processed={
                'resource-name': 'trees',
                'indexes_fields': [['meta-tree-id'], ['meta-collection-type', 'meta-source-type']],
            }), 'env://DATASETS_DATABASE_URL'
        ),
    ).process()
    DF.Flow(
        DF.checkpoint('tree-processing-clusters', CHECKPOINT_PATH),
        DF.select_fields(['location-x', 'location-y', 'meta-tree-id', 'meta-source', 'attributes-species-clean-he', 'attributes-species-clean-en',
                          'attributes-canopy-area', 'attributes-height', 'attributes-bark-diameter',
                          'road_id', 'muni_code', 'stat_area_code', 'cad_code', 'meta-collection-type', 'meta-source-type', 'meta-internal-id']),
        DF.add_field('joint-source-type', type='string', default=lambda row: f'{row["meta-collection-type"]}/{row["meta-source-type"]}'),
        DF.set_type('attributes-canopy-area', type='number', on_error='clear'),
        DF.set_type('attributes-height', type='number', on_error='clear'),
        DF.set_type('attributes-bark-diameter', type='number', on_error='clear'),
        DF.join_with_self('trees', ['meta-tree-id'], fields={
            'meta-tree-id': None,
            'location-x': None,
            'location-y': None,
            'attributes-species-clean-he': None,
            'attributes-species-clean-en': None,
            'road_id': None,
            'muni_code': None,
            'stat_area_code': None,
            'cad_code': None,
            'meta-source': dict(aggregate='set'),
            'meta-source-type': dict(aggregate='set'),
            'joint-source-type': dict(aggregate='set'),
            'meta-collection-type': dict(aggregate='set'),
            'meta-internal-id': dict(aggregate='set'),
            'attributes-canopy-area': dict(aggregate='max'),
            'attributes-height': dict(aggregate='max'),
            'attributes-bark-diameter': dict(aggregate='max'),
        }),
        DF.add_field('certainty', type='boolean', default=lambda row: 'סקר רגלי' in row['meta-collection-type']),
        DF.add_field('unreported', type='boolean', default=lambda row: 'סקר רגלי/מוניציפלי' in row['joint-source-type'] and 'סקר רגלי/ממשלתי' not in row['joint-source-type']),
        DF.dump_to_sql(dict(
            trees_compact={
                'resource-name': 'trees',
                'indexes_fields': [
                    ['meta-tree-id'],
                    ['certainty'],
                    ['unreported'],
                    ['muni_code'],
                    ['stat_area_code'],
                    ['road_id'],
                    ['cad_code'],
                    ['attributes-species-clean-he', 'attributes-species-clean-en'],
                    ['attributes-canopy-area'],
                    ['attributes-height'],
                    ['attributes-bark-diameter'],
                ]
            }), 'env://DATASETS_DATABASE_URL'
        ),
    ).process()

    print('### Uploading regions to MapBox ###')
    upload_to_mapbox()

    print('### Done ###')

def operator(*_):
    main()

if __name__ == "__main__":
    main()
    # DF.Flow(
    #     [
    #         {'attributes-genus': 'שלטית מקומטת'},
    #         {'attributes-genus': 'שלטית_מקומטת'},
    #         {'attributes-genus': 'שלטית_מקומט'},
    #         {'attributes-genus': 'Peltophorum Dubium'},
    #         {'attributes-genus': 'Peltophorum Dubiu'},
    #         {'attributes-genus': 'שלטית_מקומטת_Peltophorum_Dubium'},        
    #     ],
    #     clean_genus(),
    #     DF.printer()
    # ).process()