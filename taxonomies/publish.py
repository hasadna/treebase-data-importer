import os

from sqlalchemy import create_engine

from dataflows import Flow, conditional, add_field, ResourceWrapper, PackageWrapper

from dgp.core.base_enricher import enrichments_flows, BaseEnricher
from dgp.config.consts import RESOURCE_NAME, CONFIG_TAXONOMY_CT
from dgp_server.publish_flow import publish_flow, append_to_primary_key


engine = create_engine(os.environ['DATASETS_DATABASE_URL'])


class MissingColumnsAdder(BaseEnricher):

    def test(self):
        return True

    def no_such_field(self, field_name):
        def func(dp):
            return all(field_name != f.name for f in dp.resources[0].schema.fields)
        return func

    def postflow(self):
        steps = []
        for ct in self.config.get(CONFIG_TAXONOMY_CT):
            name = ct['name'].replace(':', '-')
            dataType = ct['dataType']
            unique = ct.get('unique')
            if unique:
                flow = Flow(
                    add_field(name, dataType, '-', resources=RESOURCE_NAME),
                    append_to_primary_key(name)
                )
            else:
                flow = Flow(
                    add_field(name, dataType, None, resources=RESOURCE_NAME),
                )
            steps.append(
                conditional(
                    self.no_such_field(name),
                    flow
                )
            )
        return Flow(*steps)


class EnsureUniqueFields(BaseEnricher):

    def test(self):
        return True

    def clearMissingValues(self):
        def func(package: PackageWrapper):
            for resource in package.pkg.descriptor['resources']:
                if resource['name'] == RESOURCE_NAME:
                    resource['schema']['missingValues'] = ['']
            yield package.pkg
            yield from package
        return func

    def ensure(self, fields):
        def func(rows: ResourceWrapper):
            if rows.res.name == RESOURCE_NAME:
                for row in rows:
                    for f in fields:
                        row[f] = row[f] if row[f] not in (None, '') else '-'
                    yield row
            else:
                yield from rows
        return func

    def postflow(self):
        fields = []
        for ct in self.config.get(CONFIG_TAXONOMY_CT):
            name = ct['name'].replace(':', '-')
            dataType = ct['dataType']
            unique = ct.get('unique')
            if unique and dataType == 'string':
                fields.append(name)
        return Flow(
            self.clearMissingValues(),
            self.ensure(fields)
        )

class DBWriter(BaseEnricher):

    def test(self):
        return True

    def postflow(self):
        return publish_flow(self.config, engine, mode='append', fast=True)


def flows(config, context):
    return enrichments_flows(
        config, context,
        MissingColumnsAdder,
        EnsureUniqueFields,
        DBWriter,
    )
