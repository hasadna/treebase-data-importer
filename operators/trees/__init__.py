from pathlib import Path
import shutil
import dataflows as DF

# from dataflows_ckan import dump_to_ckan
from rtree import index
from geopy.distance import distance
from openlocationcode import openlocationcode as olc

from treebase.mapbox_utils import run_tippecanoe, upload_tileset
from treebase.log import logger
from treebase.geo_utils import bbox_diffs
from treebase.s3_utils import S3Utils

SEARCH_RADIUS = 3

def spatial_index(idx):
    def func(rows):
        for i, row in enumerate(rows):
            idx.insert(i, (float(row['location-x']), float(row['location-y'])), obj=row['_source'])
            row['idx'] = i
            yield row

    return DF.Flow(
        DF.add_field('idx', 'integer'),
        func
    )


def match_index(idx: index.Index, clusters, matched):
    diff_x, diff_y = bbox_diffs(SEARCH_RADIUS)
    print('DIFFS', diff_x, diff_y)
    def func(rows):
        for row in rows:
            if row['idx'] not in matched:
                x, y = float(row['location-x']), float(row['location-y'])
                minimums = dict()
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
                if len(ids) > 0:
                    for i in ids:
                        matched[i] = row['meta-tree-id']
                    clusters[row['idx']] = ids
                    print('MATCHED', row['idx'], '->', clusters[row['idx']])
                row['cluster-size'] = len(ids)
            else:
                row['meta-tree-id'] = matched[row['idx']]
            yield row
    return DF.Flow(
        DF.add_field('cluster-size', 'integer'),
        func,
    )

def main(local=False):
    logger.info('PROCESSING TREE DATASET')
    shutil.rmtree('.checkpoints', ignore_errors=True, onerror=None)

    print('### Loading data and geo-indexing ###')
    idx = index.Index()
    DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', format='sql', table='trees', query='SELECT * FROM trees'),
        # dump_to_ckan(host, api_key, owner_org, overwrite_existing_data=True, push_to_datastore=False),
        DF.update_resource(-1, name='trees', path='trees.csv'),
        DF.add_field('coords', 'geopoint', lambda r: [float(r['location-x']), float(r['location-y'])]),
        DF.add_field('meta-collection-type-idx', 'integer', lambda r: 1 if r['meta-collection-type'] == 'חישה מרחוק' else 0),
        DF.sort_rows('{meta-collection-type-idx}'),
        DF.delete_fields(['meta-collection-type-idx']),
        spatial_index(idx),
        DF.checkpoint('tree-processing'),
    ).process()

    print('### DeDuping and assigning TreeId ###')

    clusters = dict()
    matched = dict()
    DF.Flow(
        DF.checkpoint('tree-processing'),
        match_index(idx, clusters, matched),
        DF.checkpoint('tree-processing-clusters')
    ).process()

    print('#CLUSTERS', len(clusters))

    print('### Saving result to GeoJSON ###')
    DF.Flow(
        DF.checkpoint('tree-processing-clusters'),
        DF.dump_to_path('trees-full', format='csv'),
        DF.dump_to_path('trees-full', format='geojson'),
        DF.select_fields(['coords', 'meta-tree-id']),
        DF.dump_to_path('trees-compact', format='geojson'),
    ).process()

    s3 = S3Utils()
    s3.upload('trees-full/trees.geojson', 'processed/trees/trees.geojson')
    s3.upload('trees-full/trees.csv', 'processed/trees/trees.csv')

    print('### Uploading to MapBox ###')
    filename = Path('trees-compact/trees.geojson')
    mbtiles_filename = str(filename.with_suffix('.mbtiles'))
    if run_tippecanoe('-z15', str(filename), '-o', mbtiles_filename,  '-l', 'trees'):
        upload_tileset(mbtiles_filename, 'treebase.trees', 'Tree Data')

def operator(*_):
    main()

if __name__ == "__main__":
    main()