from clients.csv_manager import CSVManager
from clients.db_connection import DBConnection
from config.config import Config
from entities.table import Table


class TableInfo:
    def __init__(self):
        self.db = DBConnection()
        self.tables = []
        self.table_query = "SELECT table_name as name FROM information_schema.tables where table_schema= 'public' " \
                           "and table_catalog= '" + Config.DB_NAME + "'"
        self.field_query = "SELECT column_name,data_type FROM information_schema.columns WHERE table_schema = " \
                           "'public' AND table_name = "
        self.ignored_tables = CSVManager().read_ignored_tables()
        self.fields_to_mask = CSVManager().read_fields_to_mask()

    def getFieldQuery(self, table_name):
        return self.field_query + "'" + table_name + "';"

    def setTableList(self):
        results = self.db.read(self.table_query)
        for result in results:
            tableName = result['name']
            if tableName not in self.ignored_tables:
                print("Reading Table Meta Data: " + tableName)
                table = Table.fromDB(tableName)
                self.readFieldNames(table)
                self.tables.append(table)

    def isMaskable(self, tableName, columnName):
        if tableName in self.fields_to_mask:
            if columnName in self.fields_to_mask[tableName]:
                return True
        return False

    def readFieldNames(self, table: Table):
        results = self.db.read(self.getFieldQuery(table.name))
        for result in results:
            columnName = result['column_name']
            dataType = result['data_type']
            mask = self.isMaskable(table.name, columnName)
            table.addColumns(columnName, mask, dataType)

    def getTables(self):
        self.setTableList()
        return self.tables
