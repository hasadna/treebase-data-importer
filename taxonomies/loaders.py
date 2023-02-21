import json

import datapackage
import dataflows as DF

from dgp.core.base_enricher import enrichments_flows
from dgp.core import BaseAnalyzer
from dgp.config.consts import CONFIG_HEADER_FIELDS
from dgp.config.log import logger

class ExtractGeoCoords(BaseAnalyzer):

    def test(self):
        headers = self.config.get(CONFIG_HEADER_FIELDS)
        return '__geometry' in headers
    
    def run(self):
        headers = self.config.get(CONFIG_HEADER_FIELDS)
        headers = headers + ['__geometry_lon', '__geometry_lat']
        self.config.set(CONFIG_HEADER_FIELDS, headers)

    def get_coords(self, i):
        def func(row):
            geometry = row['__geometry']
            if isinstance(geometry, str):
                try:
                    geometry = json.loads(geometry)
                except:
                    geometry = {}
            if geometry.get('type') == 'Point':
                coords = geometry['coordinates']
                if coords and len(coords) == 2:
                    return coords[i]
            return None
        return func

    def flow(self):
        if self.test():
            return DF.Flow(
                DF.add_field('__geometry_lon', 'number', resources=-1, default=self.get_coords(0)),
                DF.add_field('__geometry_lat', 'number', resources=-1, default=self.get_coords(1)),
            )


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
