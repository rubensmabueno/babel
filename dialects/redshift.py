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

    def get_table(self, table, schema="public"):
        queries = [
            f"""
                SELECT "column_name" AS "name", "data_type" AS "type", "character_maximum_length" AS "length", "numeric_precision" AS "precision", "numeric_precision_radix" AS "precision_radix", "numeric_scale" AS "scale", "column_default" AS "default", "is_nullable" AS "nullable"
                FROM SVV_COLUMNS
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                UNION
                SELECT "columnname" AS "name", "external_type" AS "type"
                FROM SVV_EXTERNAL_COLUMNS
                WHERE schemaname = '{schema}' AND tablename = '{table}';
                ;
            """
        ]

        return Table(table, [RedshiftTranslator.from_source(name, type, length, numeric_precision, precision_radix, scale, default, nullable) for (name, type, length, numeric_precision, precision_radix, scale, default, nullable) in self.fetch(queries)], schema=schema)


class RedshiftTranslator:
    TRANSLATION = {
        "SMALLINT": { "translation": "INTEGER" },
        "INT2": { "translation": "INTEGER" },
        "INTEGER": { "translation": "INTEGER" },
        "INT": { "translation": "INTEGER" },
        "INT4": { "translation": "INTEGER" },
        "BIGINT": { "translation": "INTEGER" },
        "INT8": { "translation": "INTEGER" },
        "DECIMAL": { "translation": "DECIMAL" },
        "NUMERIC": { "translation": "DECIMAL" },
        "REAL": { "translation": "REAL" },
        "FLOAT4": { "translation": "REAL" },
        "DOUBLE PRECISION": { "translation": "DOUBLE" },
        "BOOLEAN": { "translation": "BOOLEAN" },
        "BOOL": { "translation": "BOOLEAN" },
        "CHAR": { "translation": "CHAR" },
        "CHARACTER": { "translation": "CHAR" },
        "NCHAR": { "translation": "CHAR" },
        "BPCHAR": { "translation": "CHAR" },
        "VARCHAR": { "translation": "VARCHAR" },
        "CHARACTER VARYING": { "translation": "VARCHAR" },
        "NVARCHAR": { "translation": "VARCHAR" },
        "TEXT": { "translation": "VARCHAR" },
        "DATE": { "translation": "DATE" },
        "TIMESTAMP": { "translation": "TIMESTAMP" },
        "TIMESTAMP WITHOUT TIME ZONE": { "translation": "TIMESTAMP" },
        "TIMESTAMPTZ": { "translation": "TIMESTAMP WITH TIME ZONE" }
    }

    @staticmethod
    def from_source(name, type, length, numeric_precision, precision_radix, scale, default, nullable):
        return Column(name, RedshiftTranslator.TRANSLATION[type], length, numeric_precision, precision_radix, scale, default, nullable)
