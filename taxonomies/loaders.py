import json

import datapackage
import dataflows as DF

from dgp.core.base_enricher import ColumnTypeTester, ColumnReplacer, \
        DatapackageJoiner, enrichments_flows, BaseEnricher

class ExtractGeoCoords(BaseEnricher):

    def test(self):
        return True
    
    def has_geometry(self):
        def func(dp: datapackage.Package):
            resource: datapackage.Resource = dp.resources[-1]
            for f in resource.schema.fields:
                if f.name == '__geometry':
                    return True
            return False
        return func

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

    def postflow(self):
        return DF.Flow(
            DF.conditional(self.has_geometry(), DF.Flow(
                DF.add_field('__geometry_lon', 'number', resources=-1, default=self.get_coords(0)),
                DF.add_field('__geometry_lat', 'number', resources=-1, default=self.get_coords(1)),
            ))
        )


def flows(config, context):
    return enrichments_flows(
        config, context,
        ExtractGeoCoords,
    )
