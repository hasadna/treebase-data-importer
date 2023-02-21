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
            slug = slugify(url.replace('.shp', ''), separator='_') + '.geojson'
            logger.info('SHPFileAnalyzer: SLUG {}'.format(slug))
            return slug

    def process_url(self, url, cache_dir):
        logger.info('SHPFileAnalyzer: PROCESSING URL {}'.format(url))
        source_file = None
        for ext in self.SHAPEFILE_EXTS:
            source_url = url[:-4] + '.{}'.format(ext)
            tmp_fn = f'{cache_dir}/dl.{ext}'
            if not os.path.exists(tmp_fn):
                with open(tmp_fn, 'wb') as tmp_f:
                    logger.info('DOWNLOADING {}'.format(source_url))
                    r = requests.get(str(source_url), stream=True).raw
                    shutil.copyfileobj(r, tmp_f)
            if source_file is None:
                source_file = tmp_fn

        first = True
        outfile_fn = f'{cache_dir}/out.geojson'
        with open(outfile_fn, 'w', encoding='utf8') as outfile:
            outfile.write('{"type": "FeatureCollection", "features": [')
            # Open the file with fiona
            layer = fiona.listlayers(source_file)[0]
            logger.info('LAYER: {}'.format(layer))
            with fiona.open(source_file, layername=layer, encoding='utf8') as collection:
                logger.info('CRS: {}'.format(collection.crs))
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
                    properties=dict(item['properties']),
                    outfile.write(json.dumps(dict(
                        type='Feature',
                        properties=properties,
                        geometry=geometry,
                    ), ensure_ascii=False))
            outfile.write(']}')
        logger.info('DONE - {}'.format(outfile_fn))
        return outfile_fn


def analyzers(*_):
    logger.info('PRELOADERS: LOADING ANALYZERS')
    return [
        # GPKGAnalyzer,
        SHPFileAnalyzer,
    ]
