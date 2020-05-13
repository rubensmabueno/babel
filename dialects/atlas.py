from atlasclient.client import Atlas as AtlasClient


class Atlas:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    @property
    def connection(self):
        return AtlasClient(self.host, port=self.port, username=self.user, password=self.password)

    def get_instance_guid(self, instance):
        params = {
          "typeName": "Instance",
          "query": f"from Instance where name = '{instance}'",
          "limit": 1,
          "offset": 0
        }

        search_results = self.connection.search_basic(**params)
        guids = [entity.guid for search_result in search_results for entity in search_result.entities]

        if len(guids) > 0:
            return guids[0]

    def get_schema_guid(self, schema):
        params = {
          "typeName": "Schema",
          "query": f"from Schema where name = '{schema}'",
          "limit": 1,
          "offset": 0
        }

        search_results = self.connection.search_basic(**params)
        guids = [entity.guid for search_result in search_results for entity in search_result.entities]

        if len(guids) > 0:
            return guids[0]

    def get_table_guid(self, table):
        params = {
          "typeName": "Table",
          "query": f"from Table where name = '{table}'",
          "limit": 1,
          "offset": 0
        }

        print(params)

        search_results = self.connection.search_basic(**params)
        guids = [entity.guid for search_result in search_results for entity in search_result.entities]

        if len(guids) > 0:
            return guids[0]

    def create_schema(self, schema, instance=None):
        schema_guid = self.get_schema_guid(schema)

        if not schema_guid:
            params = {
                "referredEntities": {},
                "entity": {
                    "typeName": "Schema",
                    "attributes": {
                        "instance": {
                            "guid": self.get_instance_guid(instance),
                            "typeName": "Instance",
                        },
                        "qualifiedName": schema,
                        "name": schema
                    }
                }
            }

            schema_guid = self.connection.entity_post.create(data=params)['mutatedEntities']['CREATE'][0]['guid']

        return schema_guid

    def create_table(self, table, instance=None):
        schema_guid = self.create_schema(table.schema, instance=instance)
        table_guid = self.get_table_guid(f"{table.schema}.{table.name}")

        if not table_guid:
            params = {
                "referredEntities": {},
                "entity": {
                    "typeName": "Table",
                    "attributes": {
                        "schema": {
                            "guid": schema_guid,
                            "typeName": "Schema",
                        },
                        "qualifiedName": f"{table.schema}.{table.name}",
                        "name": table.name,
                        "type": "external"
                    }
                }
            }

            response = self.connection.entity_post.create(data=params)

            table_guid = response['mutatedEntities']['CREATE'][0]['guid']

        self.create_columns(table, table.columns)

        return table_guid

    def create_columns(self, table, columns, prefix_column_name='', parent_column_guid=None):
        table_guid = self.get_table_guid(f"{table.schema}.{table.name}")
        columns_guid = []

        for column in columns:
            column_name = prefix_column_name + column.name

            params = {
                "referredEntities": {},
                "entity": {
                    "typeName": "Column",
                    "attributes": {
                        "table": {
                            "guid": table_guid,
                            "typeName": "Table",
                        },
                        "qualifiedName": f"{table.schema}.{table.name}.{column_name}",
                        "name": column_name,
                        "type": column.type
                    }
                }
            }

            if parent_column_guid:
                params["entity"]['attributes']['column'] = { "guid": parent_column_guid, "typeName": "Column" }

            print(params)
            guid = list(self.connection.entity_post.create(data=params)['guidAssignments'].values())[0]

            if column.fields:
                self.create_columns(table, column.fields, prefix_column_name=f"{column_name}.", parent_column_guid=guid)

            columns_guid.append((f"{table.schema}.{table.name}.{column.name}", guid))

        return columns_guid

    def delete_table(self, table):
        table_guid = self.get_table_guid(f"{table.schema}.{table.name}")

        if table_guid:
            for column in self.connection.entity_guid(table_guid).entity['relationshipAttributes']['columns']:
                print(column)

                self.delete(f"/v2/relationship/guid/{column['relationshipGuid']}")
                self.delete(f"/v2/entity/guid/{column['guid']}")

            self.delete(f"/v2/entity/guid/{table_guid}")

        return True

    def delete(self, url_path):
        return self.connection.delete(f"{self.host}/api/atlas{url_path}", timeout=60)
