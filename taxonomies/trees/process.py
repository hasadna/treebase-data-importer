from dgp.core.base_enricher import enrichments_flows

from datacity_server.processors import \
    FilterEmptyFields, AddressFixer, GeoCoder, GeoProjection


def flows(config, context):
    return enrichments_flows(
        config, context,
    )
