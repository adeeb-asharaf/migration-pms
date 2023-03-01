import psycopg2
from psycopg2 import Error
import psycopg2.extras

from config.config import Config
import psycopg2

class DBConnection:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                user=Config.DB_USER_NAME,
                password=Config.DB_PASSWORD,
                host=Config.DB_END_POINT,
                database=Config.DB_NAME
            )
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except(Exception, Error) as error:
            self.close()
            print("Error connecting to RDS:" + str(error))

    def read(self, query: str):
        try:
            self.connect()
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            self.close()
            return records
        except (Exception, Error) as error:
            self.close()
            print("Error reading from RDS:" + str(error))
        return None

    def write(self, query: str) -> bool:
        try:
            self.connect()
            self.cursor.execute(query)
            self.connection.commit()
            self.close()
            return True
        except (Exception, Error) as error:
            self.close()
            print(query)
            print("Error writing to RDS:" + str(error))
        return False

    def close(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
