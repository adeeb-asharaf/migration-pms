from config.config import Config
from entities.table import Table


class SQLManager:
    def __init__(self):
        self.filePath = Config.SQL_PATH + Config.OUTPUT_FILE_NAME
        self.fileHandler = open(self.filePath, "w")

    def writeSQLFile(self, table: Table, processedResults):
        beginText = "-----------" + table.name + "-------------- \n"
        beginText += "-----------BEGIN-------------- \n"
        self.fileHandler.write(beginText)
        insertStatement = table.getSQLInsertStatement()
        for result in processedResults:
            insertQuery = insertStatement + "(" + result + ");"
            self.fileHandler.write(insertQuery + "\n")
        endText = "-----------END-------------- \n"
        self.fileHandler.write(endText)

    def finishSQLFile(self):
        self.fileHandler.close()
