import json
import pymysql.cursors

from helpers.helpers import print_html
import os
from dotenv import load_dotenv

load_dotenv()


migrations = [
    """ALTER TABLE `products` ADD `remote_key_id` VARCHAR(101) NULL DEFAULT NULL AFTER `uuid`; 
    ALTER TABLE `products` ADD UNIQUE(`remote_key_id`);
    """,
    """
    ALTER TABLE `product_variants` ADD `remote_key_id` VARCHAR(101) NULL DEFAULT NULL, ADD UNIQUE `remote_key_id` (`remote_key_id`); 
    """,
    """
    ALTER TABLE `users` ADD `remote_key_id` VARCHAR(101) NULL DEFAULT NULL, ADD UNIQUE `remote_key_id` (`remote_key_id`); 
    """,
    """
    ALTER TABLE `orders` ADD `remote_key_id` VARCHAR(101) NULL DEFAULT NULL, ADD UNIQUE `remote_key_id` (`remote_key_id`); 
    """,
]


class SQLConnector:
    connection = None
    _results = None
    _debug = False

    def __init__(self, debug=False) -> None:
        self.connection = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            port=int(os.getenv("DB_PORT")),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor,
        )
        self._debug = debug
        pass

    def onDebug(self, msg):
        if self._debug:
            print("[db.debug] ", msg)
            print("[db.debug._results] ", json.dumps(self._results, default=str))
        return self

    def migrate(self):
        with self.connection.cursor() as cursor:
            print ("[migration] started")
            for migration in migrations:
                try:
                    cursor.execute(migration)
                    self.connection.commit()
                except Exception as e:
                    # rollback
                    self.onDebug("[migration] rollback" + str(e))
                    self.connection.rollback()
                pass
        return self
       

    def getAll(self, table_name, where_clause=None, fields=None, select="*"):
        with self.connection.cursor() as cursor:
            if where_clause is None:
                sql = f"SELECT {select} FROM {table_name}"
                cursor.execute(sql)
                result = cursor.fetchall()
                self._results = result
                return self
            sql = f"SELECT {select}  FROM {table_name} WHERE {where_clause}"
            cursor.execute(sql, fields)
            result = cursor.fetchall()
            self._results = result
            return self

    def getOne(self, table_name, where_clause=None, fields=None, select="*"):
        with self.connection.cursor() as cursor:
            if where_clause is None:
                sql = f"SELECT {select} FROM {table_name}"
                cursor.execute(sql)
                result = cursor.fetchone()
                self._results = result
                return self
            sql = f"SELECT {select} FROM {table_name} WHERE {where_clause}"
            cursor.execute(sql, fields)
            result = cursor.fetchone()
            self._results = result
            return self

    def sanatize(self, data):
        return {key: value.replace("'", '"') if isinstance(value, str) else value for key, value in data.items()}


    def update(self, table_name, where_clause, data):
        data = self.sanatize(data)
        with self.connection.cursor() as cursor:
            fields = ", ".join([f"{key} = '{value}'" for key, value in data.items()])
            # add updated at now to fields
            fields = fields + ", updated_at = NOW()"
            sql = f"UPDATE {table_name} SET {fields} WHERE {where_clause}"
            self.onDebug("[sql.update] %s" % sql)
            cursor.execute(sql)
            self.connection.commit()
            self.getOne(table_name, where_clause)
            return self

    def insert(self, table_name, data, where_clause=None):
        data = self.sanatize(data)
        with self.connection.cursor() as cursor:
            fields = ", ".join(data.keys())
            values = ", ".join([f"'{value}'" for value in data.values()])

            # add created at and updated at
            fields += ", updated_at, created_at"
            values += ", NOW(), NOW()"

            sql = f"INSERT INTO {table_name} ({fields}) VALUES ({values})"
            cursor.execute(sql)
            self.onDebug("[sql.update] %s" % sql)
            self.connection.commit()
            if where_clause is not None:
                self.getOne(table_name, where_clause)
            return self

    def upsert(self, table_name, data, updatedData, where_clause):
        data = self.sanatize(data)
        if self.getOne(table_name, where_clause).toJSON() is None:
            return self.insert(table_name, data, where_clause=where_clause)
        return self.update(table_name, where_clause, updatedData)

    def toJSON(self):
        if (
            self._results is None
            or len(self._results) == 0
            or self._results is False
            or self._results == "null"
        ):
            return None
        return json.dumps(self._results, default=str)

    def fetch(self):
        return json.loads(self.toJSON())

    def toHTML(self):
        return print_html(self.toJSON())
