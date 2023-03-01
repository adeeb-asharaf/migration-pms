from typing import Any

from clients.csv_manager import CSVManager
from clients.db_connection import DBConnection
from clients.sql_manager import SQLManager
from config.config import Config
from entities.table import Table
from services.mask_data import MaskData


class TableData:
    def __init__(self):
        self.db = DBConnection()
        self.csv = CSVManager()
        self.sql = SQLManager()

    def processValue(self, table, field, value):
        if value is None:
            return value
        value = str(value)
        if table.fields[field]:
            value = MaskData.get_masked_data(field)
        if table.dataTypes[field] == Config.CHARACTER_TYPE:
            value = "'" + value + "'"
        return value

    def processRow(self, row, table) -> str:
        values = []
        for column in table.fields:
            processedValue = self.processValue(table, column, row[column])
            values.append(str(processedValue))
        return ','.join(values)

    def processResults(self, results: Any, table: Table) -> [str]:
        processedResults = []
        for result in results:
            processedResult = self.processRow(result, table)
            processedResults.append(processedResult)
        return processedResults

    def getTableDataFromDB(self, table: Table):
        print("Processing:", table.name)
        results = self.db.read(table.getDataQuery())
        processedResults = self.processResults(results, table)
        print("Writing CSV File:", table.name)
        self.csv.write_csv_output(table, processedResults)
        print("Updating SQL Dump:", table.name)
        self.sql.writeSQLFile(table, processedResults)

    def dumpTables(self, tables: [Table]):
        for table in tables:
            self.getTableDataFromDB(table)
        self.sql.finishSQLFile()
