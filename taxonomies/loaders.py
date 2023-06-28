import json

import datapackage
import dataflows as DF

from dgp.core.base_enricher import enrichments_flows
from dgp.core import BaseAnalyzer
from dgp.config.consts import CONFIG_HEADER_FIELDS
from dgp.config.log import logger

class ExtractGeoCoords(BaseAnalyzer):

    REQUIRES = [CONFIG_HEADER_FIELDS]
    KEY = 'extra.geocoords.needs_extraction'

    def test(self):
        return True
    
    def run(self):
        headers = self.config.get(CONFIG_HEADER_FIELDS)
        needs_extraction = '__geometry' in headers
        self.config.set(self.KEY, needs_extraction)
        if needs_extraction:
            headers = headers + ['__geometry_lon', '__geometry_lat']
            self.config.set(CONFIG_HEADER_FIELDS, headers)

    def get_coords(self, i):
        def func(row):
            geometry = row['__geometry']
            if isinstance(geometry, str):
                try:
                    geometry = json.loads(geometry) or {}
                except:
                    geometry = {}
            if geometry.get('type') == 'Point':
                coords = geometry['coordinates']
                if coords and len(coords) == 2:
                    return coords[i]
            return None
        return func

    def flow(self):
        if self.config.get(self.KEY):
            return DF.Flow(
                DF.add_field('__geometry_lon', 'number', resources=-1, default=self.get_coords(0)),
                DF.add_field('__geometry_lat', 'number', resources=-1, default=self.get_coords(1)),
            )
        else:
            return DF.Flow()


def analyzers(*_):
    return [
        # GPKGAnalyzer,
        ExtractGeoCoords,
    ]

# def flows(config, context):
#     return enrichments_flows(
#         config, context,
#         ExtractGeoCoords,
#     )
