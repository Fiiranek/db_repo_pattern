import psycopg2
from psycopg2 import sql
from .db import Database


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
        self.db = Database()
        self.fields = fields
        self.schema = schema
        self.table = table

    def check_db_connection(self, func):
        def wrapper():
            if not self.db.conn:
                print('There is no db connection')
                return
            func()

        return wrapper()

    @check_db_connection
    def create(self, insert_values):

        with self.db.conn.cursor() as cursor:
            try:
                insert_fields = [field for field in self.fields if not field.has_default]
                sql_string = "INSERT INTO {table} ({fields}) VALUES (" \
                             + ", ".join(["%s" for field in insert_fields]) \
                             + ");"
                cursor.execute(sql.SQL(sql_string)
                    .format(
                    table=sql.Identifier(self.table),
                    fields=sql.SQL(",").join([sql.Identifier(field.name) for field in insert_fields])
                ), insert_values)
                self.db.conn.commit()
                print(cursor.rowcount)
                self.db.close()
                return True, "successfully added"
            except psycopg2.Error as er:
                self.db.close()
                return False, er.pgerror

    @check_db_connection
    def get_all(self):
        with self.db.conn.cursor() as cursor:
            try:
                sql_string = "SELECT * FROM {table}"
                cursor.execute(sql.SQL(sql_string).format(table=sql.Identifier(self.table)))
                db_data = cursor.fetchall()
                data = list(db_data)
                self.db.close()
                return True, data
            except psycopg2.Error as er:
                self.db.close()
                return False, er.pgerror

    @check_db_connection
    def get_some(self, field_name, field_value):
        with self.db.conn.cursor() as cursor:
            try:
                sql_string = "SELECT * FROM {table} WHERE {field}=%s"
                cursor.execute(
                    sql.SQL(sql_string).format(
                        table=sql.Identifier(self.table),
                        field=sql.Identifier(field_name)
                    ), [field_value])
                db_data = cursor.fetchall()
                data = list(db_data)
                self.db.close()
                return True, data
            except psycopg2.Error as er:
                self.db.close()
                return False, er.pgerror

    @check_db_connection
    def get_one(self, field_name, field_value):

        with self.db.conn.cursor() as cursor:
            try:
                sql_string = "SELECT * FROM {table} WHERE {field}=%s"
                cursor.execute(
                    sql.SQL(sql_string).format(
                        table=sql.Identifier(self.table),
                        field=sql.Identifier(field_name)
                    ), [field_value])
                db_data = cursor.fetchone()
                self.db.close()
                return True, db_data
            except psycopg2.Error as er:
                self.db.close()
                return False, er.pgerror

    @check_db_connection
    def remove(self, field_name, field_values):

        field_values = [(value,) for value in field_values]
        if self.db.conn:
            with self.db.conn.cursor() as cursor:
                try:
                    sql_string = "DELETE FROM {table} WHERE " + "OR ".join(["{field}=%s " for value in field_values])
                    cursor.execute(
                        sql.SQL(sql_string).format(
                            table=sql.Identifier(self.table),
                            field=sql.Identifier(field_name)
                        ), field_values)
                    affected = cursor.rowcount
                    self.db.conn.commit()
                    self.db.close()
                    return True, ("records removed: " + str(affected))
                except psycopg2.Error as er:
                    self.db.close()
                    return False, er.pgerror
        else:
            self.db.close()
            return False, "no db connection"

    @check_db_connection
    def update(self, condition_field_name, condition_field_value, update_fields_values):

        update_fields_values = [(value,) for value in update_fields_values]
        update_fields_values.append((condition_field_value,))
        if self.db.conn:
            with self.db.conn.cursor() as cursor:
                try:
                    sql_string = "UPDATE {table} SET " \
                                 + ", ".join([(field.name + "=%s") for field in self.fields if not field.has_default]) \
                                 + " WHERE {condition_field}=%s"
                    cursor.execute(
                        sql.SQL(sql_string).format(
                            table=sql.Identifier(self.table),
                            condition_field=sql.Identifier(condition_field_name)
                        ), update_fields_values)
                    affected = cursor.rowcount
                    self.db.conn.commit()
                    self.db.close()
                    return True, ("records updated: " + str(affected))
                except psycopg2.Error as er:
                    self.db.close()
                    return False, er.pgerror
        else:
            self.db.close()
            return False, "no db connection"
