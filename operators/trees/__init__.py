from pathlib import Path
import dataflows as DF

# from dataflows_ckan import dump_to_ckan

from treebase.mapbox_utils import run_tippecanoe, upload_tileset
from treebase.log import logger

def main():
    logger.info('PROCESSING TREE DATASET')
    DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', format='sql', table='trees', query='SELECT * FROM trees'),
        # dump_to_ckan(host, api_key, owner_org, overwrite_existing_data=True, push_to_datastore=False),
        DF.update_resource(-1, name='trees', path='trees.csv'),
        DF.add_field('coords', 'geopoint', lambda r: [float(r['location-x']), float(r['location-y'])]),
        DF.dump_to_path('trees', format='geojson'),
    ).process()

    # print('### DeDuping ###')

    # print('### Assigning Tree ID ###')

    print('### Uploading to MapBox ###')
    filename = Path('trees/trees.geojson')
    mbtiles_filename = str(filename.with_suffix('.mbtiles'))
    if run_tippecanoe('-z15', str(filename), '-o', mbtiles_filename,  '-l', 'trees'):
        upload_tileset(mbtiles_filename, 'treebase.trees', 'Tree Data')

def operator(*_):
    main()

if __name__ == "__main__":
    main()