import boto3
import re

from models.column import Column
from models.table import Table


class Glue:
    @property
    def connection(self):
        return boto3.client('glue')

    def get_databases(self):
        return [database['Name'] for database in self.connection.get_databases()['DatabaseList']]

    def get_tables(self, schema=None):
        if schema:
            return [
                Table(table['Table']['Name'], [GlueTranslator.from_source(column['Name'], column['Type']) for column in
                                      table['StorageDescriptor']['Columns']], schema=schema)
                for table in self.connection.get_tables(DatabaseName=schema)['TableList']
            ]
        else:
            return [
                Table(table['Table']['Name'], [GlueTranslator.from_source(column['Name'], column['Type']) for column in
                                      table['StorageDescriptor']['Columns']], schema=database)
                for database in self.get_databases()
                for table in self.connection.get_tables(DatabaseName=database)['TableList']
            ]

    def get_table(self, table, schema="public"):
        table = self.connection.get_table(
            DatabaseName=schema,
            Name=table
        )

        return Table(table['Table']['Name'], [GlueTranslator.from_source(column['Name'], column['Type']) for column in table['Table']['StorageDescriptor']['Columns']], schema=schema)


class GlueTranslator:
    TRANSLATION = {
        "int": { "translation": "INTEGER" },
        "string": { "translation": "VARCHAR" },
        "decimal": { "translation": "DECIMAL" },
        "timestamp": { "translation": "TIMESTAMP" },
        "date": { "translation": "DATE" },
        "bigint": { "translation": "INTEGER" },
        "boolean": { "translation": "BOOLEAN" },
        "double": { "translation": "DOUBLE" },
    }

    @staticmethod
    def from_source(name, type):
        groups = re.match(r'(\w+)(?:\((\d+)(?:, (\d+)) * \))*\s*(NOT NULL)?', type)

        if(groups.group(1) == 'struct'):
            return Column(name, groups.group(1), fields=GlueTranslator.parse_struct(type))
        else:
            nullable = False if groups.group(4) else True

            if groups.group(2) and groups.group(3):
                return Column(name, GlueTranslator.TRANSLATION[groups.group(1)]['translation'], numeric_precision=groups.group(2), scale=groups.group(3), nullable=nullable)
            elif groups.group(2):
                return Column(name, GlueTranslator.TRANSLATION[groups.group(1)]['translation'], length=groups.group(2), nullable=nullable)
            else:
                return Column(name, GlueTranslator.TRANSLATION[groups.group(1)]['translation'], nullable=nullable)

    @staticmethod
    def parse_struct(original_string):
        fields = []

        name = ''
        type = ''

        original_string = original_string[7:-1]

        for char in original_string:
            if type.startswith('struct'):
                type += char

                if type.count('<') == type.count('>') and char == ',':
                    fields.append(GlueTranslator.from_source(name, type[1:len(type)]))
            elif char == ',':
                fields.append(GlueTranslator.from_source(name, type[1:len(type)]))

                name = ''
                type = ''
            elif char == ':' or len(type) > 0:
                type += char
            else:
                name += char

        return fields