import psycopg2

from models.table import Table
from models.column import Column


class Redshift:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    @property
    def connection(self):
        return psycopg2.connect(dbname=self.database, host=self.host, port=self.port, user=self.user,
                                password=self.password)

    def fetch(self, queries):
        with self.connection.cursor() as cursor:
            for query in queries:
                cursor.execute(query)

            result = cursor.fetchall()

        return result

    def get_tables(self, schema="public"):
        queries = [
            f"""
                SELECT t.table_name
                FROM information_schema.tables t
                WHERE t.table_schema = '{schema}' AND t.table_type = 'BASE TABLE';
            """
        ]

        results = self.fetch(queries)

        return [self.get_table(name[0], schema) for (name) in results]


    def get_table(self, table, schema="public"):
        queries = [
            f"""
                SELECT "column_name" AS "name", "data_type" AS "type", "character_maximum_length" AS "length", "numeric_precision" AS "precision", "numeric_precision_radix" AS "precision_radix", "numeric_scale" AS "scale", "column_default" AS "default", "is_nullable" AS "nullable"
                FROM SVV_COLUMNS
                WHERE table_schema = '{schema}' AND table_name = '{table}';
            """
        ]

        return Table(table, [RedshiftTranslator.from_source(name, type, length, numeric_precision, precision_radix, scale, default, nullable) for (name, type, length, numeric_precision, precision_radix, scale, default, nullable) in self.fetch(queries)], schema=schema)


class RedshiftTranslator:
    TRANSLATION = {
        "smallint": { "translation": "INTEGER" },
        "int2": { "translation": "INTEGER" },
        "integer": { "translation": "INTEGER" },
        "int": { "translation": "INTEGER" },
        "int4": { "translation": "INTEGER" },
        "bigint": { "translation": "INTEGER" },
        "int8": { "translation": "INTEGER" },
        "decimal": { "translation": "DECIMAL" },
        "numeric": { "translation": "DECIMAL" },
        "real": { "translation": "REAL" },
        "float4": { "translation": "REAL" },
        "double precision": { "translation": "DOUBLE" },
        "boolean": { "translation": "BOOLEAN" },
        "bool": { "translation": "BOOLEAN" },
        "char": { "translation": "CHAR" },
        "character": { "translation": "CHAR" },
        "nchar": { "translation": "CHAR" },
        "bpchar": { "translation": "CHAR" },
        "varchar": { "translation": "VARCHAR" },
        "character varying": { "translation": "VARCHAR" },
        "nvarchar": { "translation": "VARCHAR" },
        "text": { "translation": "VARCHAR" },
        "date": { "translation": "DATE" },
        "timestamp": { "translation": "TIMESTAMP" },
        "timestamp without time zone": { "translation": "TIMESTAMP" },
        "timestamptz": { "translation": "TIMESTAMP WITH TIME ZONE" }
    }

    @staticmethod
    def from_source(name, type, length, numeric_precision, precision_radix, scale, default, nullable):
        return Column(name, RedshiftTranslator.TRANSLATION[type]['translation'], length, numeric_precision, precision_radix, scale, default, nullable)
