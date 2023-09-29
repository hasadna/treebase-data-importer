import re

from pyproj import Transformer

import dataflows as DF
from dgp.core.base_enricher import enrichments_flows, BaseEnricher, ColumnTypeTester



NUMS = re.compile(r'[0-9]+')

class ConvertGeoCoordinates(ColumnTypeTester):

    REQUIRED_COLUMN_TYPES = [
        'location:x',
        'location:y',
        'location:grid',
    ]
    PROHIBITED_COLUMN_TYPES = []

    def convert_geo_coordinates(self):
        def func(rows):
            transformer = Transformer.from_crs('epsg:2039', 'epsg:4326', always_xy=True)
            for row in rows:
                grid = row['location-grid']
                if grid and grid.lower() in ('epsg:2039', 'itm', 'ישראל'):
                    x = row['location-x']
                    y = row['location-y']
                    if x and y:
                        lon, lat = transformer.transform(x, y)
                        row['location-x'] = lon
                        row['location-y'] = lat
                yield row
        return func

    def conditional(self):
        return DF.Flow(
            self.convert_geo_coordinates()
        )


class ExtractNumbersFromText(BaseEnricher):

    FIELDS = [
        'attributes-age',
        'attributes-canopy-area',
        'attributes-height',
        'attributes-bark-diameter',
        'attributes-bark-circumference',
    ]

    def test(self):
        return True

    def extract_numbers_from_text(self):
        def func(row):
            for field in self.FIELDS:
                if field in row:
                    val = row[field]
                    if isinstance(val, str):
                        try:
                            row[field] = float(val)
                        except ValueError:
                            nums = NUMS.findall(val)
                            if len(nums) > 0:
                                row[field] = (sum(float(x) for x in nums) / len(nums))
                            else:
                                row[field] = None
            return row
        return func

    def retype_field(self, fieldname):
        def predicate(fieldname_):
            def func(dp):
                all_fields = [f['name'] for f in dp.descriptor['resources'][0]['schema']['fields']]
                return fieldname_ in all_fields
            return func
        return DF.conditional(
            predicate(fieldname),
            DF.Flow(
                DF.set_type(fieldname, type='number')
            )
        )

    def postflow(self):
        return DF.Flow(
            self.extract_numbers_from_text(),
            *[
                self.retype_field(field)
                for field in self.FIELDS
            ]
        )


class EnsureInternalIdString(BaseEnricher):

    FIELD_NAME = 'meta-internal-id',

    def test(self):
        return True

    def fix_internal_id(self):
        def predicate():
            def func(dp):
                all_fields = [f['name'] for f in dp.descriptor['resources'][0]['schema']['fields']]
                return self.FIELD_NAME in all_fields
            return func
        return DF.conditional(
            predicate(),
            DF.Flow(
                DF.set_type(self.FIELD_NAME, type='number', transform=lambda x: str(x) if x is not None else None)
            )
        )

    def postflow(self):
        return DF.Flow(
            self.fix_internal_id(),
        )


def flows(config, context):
    return enrichments_flows(
        config, context,
        EnsureInternalIdString,
        ExtractNumbersFromText,
        ConvertGeoCoordinates,
    )
