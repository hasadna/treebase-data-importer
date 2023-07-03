from pathlib import Path
import shutil
import dataflows as DF
import re
import json

# from dataflows_ckan import dump_to_ckan
from rtree import index
from geopy.distance import distance
from openlocationcode import openlocationcode as olc

from dataflows_airtable import load_from_airtable

from thefuzz.process import extractOne

from treebase.mapbox_utils import run_tippecanoe, upload_tileset
from treebase.log import logger
from treebase.geo_utils import bbox_diffs
from treebase.s3_utils import S3Utils
from treebase.data_indexes import match_rows, upload_to_mapbox

SEARCH_RADIUS = 3
CHECKPOINT_PATH = '/geodata/trees/.checkpoints'

def spatial_index(idx):
    def func(rows):
        for i, row in enumerate(rows):
            idx.insert(i, (float(row['location-x']), float(row['location-y'])), obj=row['_source'])
            row['idx'] = i
            yield row
            if i % 10000 == 0:
                print('INDEXED', i)
        print('INDEXED TOTAL', i)

    return DF.Flow(
        DF.add_field('idx', 'integer'),
        func
    )


def match_index(idx: index.Index, matched):
    diff_x, diff_y = bbox_diffs(SEARCH_RADIUS)
    print('DIFFS', diff_x, diff_y)
    def func(rows):
        clusters = dict()
        for row in rows:
            if row['idx'] not in matched:
                x, y = float(row['location-x']), float(row['location-y'])
                minimums = dict()
                minimums[row['_source']] = (0, row['idx'])
                for i_ in idx.intersection((x-diff_x, y-diff_y, x+diff_x, y+diff_y), objects=True):
                    i: index.Item = i_
                    if i.id in matched:
                        continue
                    i_source = i.object
                    if i_source == row['_source']:
                        continue
                    d = distance((i.bbox[1], i.bbox[0]), (y, x)).meters
                    if d < SEARCH_RADIUS:
                        minimums.setdefault(i_source, (SEARCH_RADIUS, 0))
                        if d < minimums[i_source][0]:
                            minimums[i_source] = d, i.id
                ids = list(id for _, id in minimums.values())
                row['meta-tree-id'] = olc.encode(y, x, 12)
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

def clean_genus():

    WORDS = re.compile(r'[a-zA-Zא-ת]+')
    table = DF.Flow(
        load_from_airtable('appHaG591cVK21CRl', 'Genus', 'Grid view', 'env://AIRTABLE_API_TOKEN'),
        DF.add_field('attributes-genus-clean-en', 'string', lambda r: r['id']),
        DF.add_field('attributes-genus-clean-he', 'string', lambda r: r['name']),
        DF.select_fields(['attributes-genus-clean-en', 'attributes-genus-clean-he']),
    ).results()[0][0]
    options = dict([
        (r['attributes-genus-clean-en'].lower(), r)
        for r in table
    ] + [
        (r['attributes-genus-clean-he'], r)
        for r in table
    ] + [
        (r['attributes-genus-clean-en'] + ' ' + r['attributes-genus-clean-he'], r)
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
                genus = row.get('attributes-genus')
                if genus:
                    genus = ' '.join(WORDS.findall(genus.lower()))
                    if genus in cache:
                        row.update(cache[genus])
                    else:                        
                        found = extractOne(genus, option_keys, score_cutoff=80)
                        if found:
                            best, _ = found
                            option = options[best]
                            row.update(option)
                            cache[genus] = option
                        else:
                            cache[genus] = dict()
                yield row
            json.dump(cache, open(fn, 'w'))


    return DF.Flow(
        DF.add_field('attributes-genus-clean-en', 'string'),
        DF.add_field('attributes-genus-clean-he', 'string'),
        func
    )


def main(local=False):
    logger.info('PROCESSING TREE DATASET')
    shutil.rmtree(CHECKPOINT_PATH, ignore_errors=True, onerror=None)

    print('### Loading data and processing ###')
    DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', format='sql', table='trees', query='SELECT * FROM trees'),
        # DF.load('trees.csv'),
        DF.update_resource(-1, name='trees', path='trees.csv'),
        DF.set_type('meta-internal-id', type='string', transform=str),
        DF.add_field('coords', 'geopoint', lambda r: [float(r['location-x']), float(r['location-y'])]),
        clean_genus(),
        DF.checkpoint('tree-processing', CHECKPOINT_PATH),
    ).process()

    print('### geo-indexing ###')
    idx = index.Index()
    DF.Flow(
        DF.checkpoint('tree-processing', CHECKPOINT_PATH),
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
        match_rows('roads', dict(
            road_name='road_name',
            road_type='road_type',
        )),
        DF.checkpoint('tree-processing-clusters', CHECKPOINT_PATH)
    ).process()

    print('### Saving result to GeoJSON ###')
    DF.Flow(
        DF.checkpoint('tree-processing-clusters', CHECKPOINT_PATH),
        DF.dump_to_path(f'{CHECKPOINT_PATH}/trees-full', format='csv'),
        DF.dump_to_path(f'{CHECKPOINT_PATH}/trees-full', format='geojson'),
        DF.select_fields(['coords', 'meta-tree-id', 'meta-source', 'attributes-genus-clean-he', 'road_name', 'muni_code', 'cad_code']),
        DF.join_with_self('trees', ['meta-tree-id'], fields={
            'tree-id': dict(name='meta-tree-id'),
            'genus': dict(name='attributes-genus-clean-he'),
            'road': dict(name='road_name'),
            'muni': dict(name='muni_code'),
            'cad': dict(name='cad_code'),
            'coords': None,
            'sources': dict(name='meta-source', aggregate='set'),
            'collection': dict(name='meta-collection-type', aggregate='set'),
        }),
        DF.add_field('certainty', type='boolean', default=lambda row: 'סקר רגלי' in row['collection']),
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