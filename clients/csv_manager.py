import csv

from config.config import Config
from entities.table import Table


class CSVManager:
    def __init__(self):
        self.masked_fields = dict()
        self.ignore_tables = [str]
        self.config = Config()

    def read_ignored_tables(self) -> [str]:
        with open(self.config.IGNORE_TABLES_FILE_NAME) as csv_file:
            ignore_tables = csv.reader(csv_file, delimiter=',')
            for table in ignore_tables:
                self.ignore_tables.append(table[0])
        return self.ignore_tables

    def read_fields_to_mask(self):
        with open(self.config.MASK_FIELD_FILE_NAME) as csv_file:
            mask_fields = csv.reader(csv_file, delimiter=',')
            for row in mask_fields:
                self.masked_fields[row[0]] = row[1:]
        return self.masked_fields

    def write_csv_output(self, table: Table, processedResults: [str]):
        with open(self.config.CSV_PATH + table.name + ".csv", "w") as csv_file:
            csv_file.write(table.getCSVColumnHeader() + '\n')
            for result in processedResults:
                csv_file.write(result + '\n')
