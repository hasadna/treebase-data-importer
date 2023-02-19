from urllib.parse import urljoin

from dgp.core import BaseDataGenusProcessor, BaseAnalyzer, Validator, Required
from dgp.core.base_enricher import enrichments_flows, BaseEnricher
from dgp.config.consts import CONFIG_JSON_PROPERTY, CONFIG_HEADER_FIELDS,\
    RESOURCE_NAME, CONFIG_FORMAT, CONFIG_URL

from dataflows import Flow, add_field, delete_fields

import tabulator
import ijson
from pyquery import PyQuery as pq


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

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    def run(self):
        if self.config.get(CONFIG_FORMAT) == 'geo':
            ...

    def flow(self):
        ...


def analyzers(*_):
    return [
        GPKGAnalyzer,
        SHPFileAnalyzer,
    ]
