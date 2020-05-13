class Column:
    def __init__(self, name, type, length=None, numeric_precision=None, precision_radix=None, scale=None, default=None, nullable=None, fields=None):
        self.name = name
        self.type = type
        self.length = length
        self.numeric_precision = numeric_precision
        self.precision_radix = precision_radix
        self.scale = scale
        self.default = default
        self.nullable = nullable
        self.fields = fields

    def __str__(self):
        full_str = f"{self.name} {self.type}"

        if self.length:
            full_str += f"({self.length})"

        if self.fields:
            full_str += self.stringfy_fields(self.fields)

        if self.default:
            full_str += f" DEFAULT {self.default}"

        if not self.nullable:
            full_str += f" NOT NULL"

        return full_str

    def stringfy_fields(self, fields):
        full_str = "<"

        for index, field in enumerate(fields):
            full_str += field.__str__()

            if index != (len(self.fields) - 1):
                full_str += ", "

        return f"{full_str}>"