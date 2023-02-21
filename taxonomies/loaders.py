import os
from pathlib import Path
import tempfile
import shutil
import json

import requests
from slugify import slugify

import fiona
from pyproj import Transformer
from shapely.ops import transform, unary_union
from shapely.geometry import shape, mapping

from dgp.core import BaseAnalyzer, Validator, Required
from dgp.config.consts import CONFIG_FORMAT, CONFIG_URL

from dgp_server.log import logger

from etl_server.loaders.fileloader import cache_dir, bucket


class GPKGAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    def run(self):
        if self.config.get(CONFIG_FORMAT) == 'geo':
            ...

    def flow(self):
        ...


class SHPFileAnalyzer(BaseAnalyzer):

    SHAPEFILE_EXTS = ['shp', 'shx', 'dbf', 'prj']

    def convert_shpfile(self):
        url = self.config.get(CONFIG_URL)
        if not url or not url.endswith('.shp'):
            return
        logger.warning('SHPFileAnalyzer url=%s', url)

        base = slugify(url.replace('.shp', ''), separator='_') + '.'
        obj_name = base + 'geojson'
        obj = bucket().Object(obj_name)
        if not obj.exists():
            source_file = None
            with tempfile.TemporaryDirectory() as tempdir:
                tempdir = Path(tempdir)
                for ext in self.SHAPEFILE_EXTS:
                    source_url = Path(url).with_suffix('.{}'.format(ext))
                    tmp_fn = (tempdir / base).with_suffix('.{}'.format(ext))
                    if not tmp_fn.exists():
                        with tmp_fn.open('wb') as tmp_f:
                            print(ext, 'DOWNLOADING', source_url)
                            r = requests.get(str(source_url), stream=True).raw
                            shutil.copyfileobj(r, tmp_f)
                    if source_file is None:
                        source_file = tmp_fn
            
            first = True
            outfile_fn = tempdir / obj_name
            with outfile_fn.open('w') as outfile:
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
            print('UPLOADING', outfile_fn)
            obj.upload_file(Filename=str(outfile_fn))


        last_modified = obj.last_modified.strftime('%Y%m%d%H%M%S')
        out_filename = os.path.join(cache_dir(), '{}-{}'.format(last_modified, obj_name))
        if not os.path.exists(out_filename):
            logger.warning('SHPFileAnalyzer downloading')
            obj.download_file(Filename=out_filename)
        return out_filename

    def run(self):
        if self.cached_out_filename:
            current_url = self.config.get(CONFIG_URL)
            logger.warning('SHPFileAnalyzer current_url=%s, cached_out_filename=%s', current_url, self.cached_out_filename)
            if current_url != self.cached_out_filename:
                self.config.set(CONFIG_URL, self.cached_out_filename)
                self.context.reset_stream()

    def analyze(self):
        self.cached_out_filename = self.convert_shpfile()
        self.run()
        return True


def analyzers(*_):
    return [
        GPKGAnalyzer,
        SHPFileAnalyzer,
    ]
