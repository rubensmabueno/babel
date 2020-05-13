class Table:
    def __init__(self, name, columns, schema="", instance=""):
        self.name = name
        self.columns = columns
        self.schema = schema

    def __str__(self):
        columns = ", ".join([ column.__str__() for column in self.columns ])

        return f"{self.schema}.{self.name}: ({columns})"
