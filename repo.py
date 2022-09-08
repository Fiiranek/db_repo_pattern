from enum import Enum
import psycopg2
from psycopg2 import sql
from .db import Database


class DBDataTypes(Enum):
    SERIAL = 1
    BOOLEAN = 2
    INTEGER = 3
    CHARACTER_VARYING = 4
    DATE = 5


class RepoField:
    def __init__(self, name, data_type, length, is_null, has_default, is_primary=False):
        self.name = name
        self.type = data_type
        self.length = length
        self.is_null = is_null
        self.has_default = has_default
        self.is_primary = is_primary


class Repo:
    def __init__(self, fields, schema, table):
        self.database = Database()
        self.fields = fields
        self.schema = schema
        self.table = table

    def create(self, insert_values):
        self.database.connect()
        if self.database.conn:
            with self.database.conn.cursor() as cursor:
                try:
                    insert_fields = [field for field in self.fields if not field.has_default]
                    sql_string = "INSERT INTO {table} ({fields}) VALUES ("\
                                 + ", ".join(["%s" for field in insert_fields])\
                                 + ");"
                    cursor.execute(sql.SQL(sql_string)
                                   .format(
                                        table=sql.Identifier(self.table),
                                        fields=sql.SQL(",").join([sql.Identifier(field.name) for field in insert_fields])
                                    ), insert_values)
                    self.database.conn.commit()
                    print(cursor.rowcount)
                    self.database.close()
                    return True, "successfully added"
                except psycopg2.Error as er:
                    self.database.close()
                    return False, er.pgerror
        else:
            self.database.close()
            return False, "no db connection"

    def get_all(self):
        self.database.connect()
        if self.database.conn:
            with self.database.conn.cursor() as cursor:
                try:
                    sql_string = "SELECT * FROM {table}"
                    cursor.execute(sql.SQL(sql_string).format(table=sql.Identifier(self.table)))
                    db_data = cursor.fetchall()
                    data = list(db_data)
                    self.database.close()
                    return True, data
                except psycopg2.Error as er:
                    self.database.close()
                    return False, er.pgerror
        else:
            self.database.close()
            return False, "no db connection"

    def get_some(self, field_name, field_value):
        self.database.connect()
        if self.database.conn:
            with self.database.conn.cursor() as cursor:
                try:
                    sql_string = "SELECT * FROM {table} WHERE {field}=%s"
                    cursor.execute(
                        sql.SQL(sql_string).format(
                            table=sql.Identifier(self.table),
                            field=sql.Identifier(field_name)
                        ), [field_value])
                    db_data = cursor.fetchall()
                    data = list(db_data)
                    self.database.close()
                    return True, data
                except psycopg2.Error as er:
                    self.database.close()
                    return False, er.pgerror
        else:
            self.database.close()
            return False, "no db connection"

    def get_one(self, field_name, field_value):
        self.database.connect()
        if self.database.conn:
            with self.database.conn.cursor() as cursor:
                try:
                    sql_string = "SELECT * FROM {table} WHERE {field}=%s"
                    cursor.execute(
                        sql.SQL(sql_string).format(
                            table=sql.Identifier(self.table),
                            field=sql.Identifier(field_name)
                        ), [field_value])
                    db_data = cursor.fetchone()
                    self.database.close()
                    return True, db_data
                except psycopg2.Error as er:
                    self.database.close()
                    return False, er.pgerror
        else:
            self.database.close()
            return False, "no db connection"

    def remove(self, field_name, field_values):
        self.database.connect()
        field_values = [(value,) for value in field_values]
        if self.database.conn:
            with self.database.conn.cursor() as cursor:
                try:
                    sql_string = "DELETE FROM {table} WHERE " + "OR ".join(["{field}=%s " for value in field_values])
                    cursor.execute(
                        sql.SQL(sql_string).format(
                            table=sql.Identifier(self.table),
                            field=sql.Identifier(field_name)
                        ), field_values)
                    affected = cursor.rowcount
                    self.database.conn.commit()
                    self.database.close()
                    return True, ("records removed: " + str(affected))
                except psycopg2.Error as er:
                    self.database.close()
                    return False, er.pgerror
        else:
            self.database.close()
            return False, "no db connection"

    def update(self, condition_field_name, condition_field_value, update_fields_values):
        self.database.connect()
        update_fields_values = [(value,) for value in update_fields_values]
        update_fields_values.append((condition_field_value,))
        if self.database.conn:
            with self.database.conn.cursor() as cursor:
                try:
                    sql_string = "UPDATE {table} SET "\
                                 + ", ".join([(field.name+"=%s") for field in self.fields if not field.has_default])\
                                 + " WHERE {condition_field}=%s"
                    cursor.execute(
                        sql.SQL(sql_string).format(
                            table=sql.Identifier(self.table),
                            condition_field=sql.Identifier(condition_field_name)
                        ), update_fields_values)
                    affected = cursor.rowcount
                    self.database.conn.commit()
                    self.database.close()
                    return True, ("records updated: " + str(affected))
                except psycopg2.Error as er:
                    self.database.close()
                    return False, er.pgerror
        else:
            self.database.close()
            return False, "no db connection"
