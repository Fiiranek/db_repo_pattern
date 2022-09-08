import psycopg2
from psycopg2 import Error
import os


class Database:
    def __init__(self):
        self.conn = None
        self.error = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                database=os.environ["DB_NAME"],
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                host=os.environ["DB_HOST"],
                port=os.environ["DB_PORT"])
        except Error as error:
            self.conn = None
            self.error = error

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
