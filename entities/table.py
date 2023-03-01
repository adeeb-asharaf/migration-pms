from config.config import Config


class Table:
    def __init__(self, name: str):
        self.name = name
        self.fields = dict()
        self.dataTypes = dict()

    @staticmethod
    def fromDB(tableName):
        return Table(tableName)

    def addColumns(self, name, isMasked, dataType):
        self.fields[name] = isMasked
        self.dataTypes[name] = Config.CHARACTER_TYPE
        if dataType in ['integer', 'boolean', 'decimal']:
            self.dataTypes[name] = Config.NUMERIC_TYPE

    def getDataQuery(self):
        field_string = ""
        for field in self.fields:
            field_string += '"' + field + '"' + ","
        query = "select " + field_string[:-1] + " from " + self.name
        return query

    def getCSVColumnHeader(self):
        return ','.join(self.fields.keys())

    def getSQLInsertStatement(self):
        return "INSERT INTO public." + self.name + " ( " + self.getCSVColumnHeader() + " ) VALUES "
