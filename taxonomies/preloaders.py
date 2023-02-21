import os
from pathlib import Path
import tempfile
import shutil
import json

import requests
from slugify import slugify

import fiona
from pyproj import Transformer
from shapely.ops import transform
from shapely.geometry import shape, mapping

from dgp.core import BaseAnalyzer, Validator, Required
from dgp.config.consts import CONFIG_FORMAT, CONFIG_URL

from dgp_server.log import logger

from etl_server.loaders.fileloader import BaseFilePreprocessor


class GPKGAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    def run(self):
        if self.config.get(CONFIG_FORMAT) == 'geo':
            ...

    def flow(self):
        ...


class SHPFileAnalyzer(BaseFilePreprocessor):

    SHAPEFILE_EXTS = ['shp', 'shx', 'dbf', 'prj']

    def test_url(self, url):
        logger.info('SHPFileAnalyzer: TESTING URL {}'.format(url))
        if url.endswith('.shp'):
            return slugify(url.replace('.shp', ''), separator='_') + '.geojson'

    def process_url(self, url, cache_dir):
        logger.info('SHPFileAnalyzer: PROCESSING URL {}'.format(url))
        source_file = None
        for ext in self.SHAPEFILE_EXTS:
            source_url = Path(url).with_suffix('.{}'.format(ext))
            tmp_fn = f'{cache_dir}/dl.{ext}'
            if not tmp_fn.exists():
                with tmp_fn.open('wb') as tmp_f:
                    print(ext, 'DOWNLOADING', source_url)
                    r = requests.get(str(source_url), stream=True).raw
                    shutil.copyfileobj(r, tmp_f)
            if source_file is None:
                source_file = tmp_fn

        first = True
        outfile_fn = f'{cache_dir}/out.geojson'
        with open(outfile_fn, 'w') as outfile:
            outfile.write('{"type": "FeatureCollection", "features": [')
            # Open the file with fiona
            layer = fiona.listlayers(source_file)[0]
            print('LAYER', layer)
            with fiona.open(source_file, layername=layer) as collection:
                print('CRS', collection.crs)
                transformer = None
                if collection.crs['init'] != 'epsg:4326':
                    transformer = Transformer.from_crs(collection.crs['init'], 'epsg:4326', always_xy=True)
                    
                for item in collection.filter():
                    if item['geometry'] is None:
                        continue
                    geometry = item['geometry']
                    if transformer is not None:
                        geometry = mapping(transform(transformer.transform, shape(geometry)))
                    if first:
                        first = False
                    else:
                        outfile.write(',')
                    outfile.write(json.dumps(dict(
                        type='Feature',
                        properties={},
                        geometry=geometry,
                    )))
            outfile.write(']}')
        print('DONE', outfile_fn)
        return outfile_fn


def analyzers(*_):
    return [
        # GPKGAnalyzer,
        SHPFileAnalyzer,
    ]
