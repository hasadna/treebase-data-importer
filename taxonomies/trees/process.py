import re

import dataflows as DF

from dgp.core.base_enricher import enrichments_flows, BaseEnricher


NUMS = re.compile(r'\d+')

class ExtractNumbersFromText(BaseEnricher):

    FIELDS = [
        'attributes-age'
    ]

    def extract_numbers_from_text(self):
        def func(row):
            for field in self.FIELDS:
                if field in row:
                    val = row[field]
                    if isinstance(val, str):
                        nums = NUMS.findall(val)
                        row[field] = int(sum(int(x) for x in nums) / len(nums))
            return row
        return func

    def postflow(self):
        return DF.Flow(
            self.extract_numbers_from_text(),
        )



def flows(config, context):
    return enrichments_flows(
        config, context,
        ExtractNumbersFromText,
    )
