import re

import dataflows as DF

from dgp.core.base_enricher import enrichments_flows, BaseEnricher


NUMS = re.compile(r'[0-9]+')

class ExtractNumbersFromText(BaseEnricher):

    FIELDS = [
        'attributes-age',
        'attributes-canopy-area',
        'attributes-height',
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
                            row[field] = (sum(float(x) for x in nums) / len(nums))
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



def flows(config, context):
    return enrichments_flows(
        config, context,
        ExtractNumbersFromText,
    )
