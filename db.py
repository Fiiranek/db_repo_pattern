import psycopg2
from psycopg2 import Error
import os
from config import config, Config


class Database:
    def __init__(self):
        self.conn = None
        self.error = None
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                database=Config.get_val('DB_NAME'),
                user=Config.get_val("DB_USER"),
                password=Config.get_val("DB_PASSWORD"),
                host=Config.get_val("DB_HOST"),
                port=Config.get_val("DB_PORT"))
        except Error as error:
            self.conn = None
            self.error = error
            print(str(error))

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
