from dgp.core.base_enricher import enrichments_flows



def flows(config, context):
    return enrichments_flows(
        config, context,
    )
