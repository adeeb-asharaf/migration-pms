from services.table_data import TableData
from services.table_info import TableInfo


def dump():
    tables = TableInfo().getTables()
    TableData().dumpTables(tables)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dump()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
