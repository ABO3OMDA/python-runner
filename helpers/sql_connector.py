import json
import pymysql.cursors
import threading
import time
import random
from contextlib import contextmanager

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
    # NEW TAX FIELD MIGRATIONS
    """
    ALTER TABLE `products` ADD `price_without_tax` DECIMAL(10,2) DEFAULT 0 AFTER `price`,
    ADD `tax_rate` DECIMAL(5,2) DEFAULT 0 AFTER `price_without_tax`,
    ADD `tax_amount` DECIMAL(10,2) DEFAULT 0 AFTER `tax_rate`,
    ADD `tax_inclusive` BOOLEAN DEFAULT TRUE AFTER `tax_amount`;
    """,
    """
    ALTER TABLE `product_variants` ADD `price_without_tax` DECIMAL(10,2) DEFAULT 0 AFTER `price`,
    ADD `cost_price` DECIMAL(10,2) DEFAULT 0 AFTER `price_without_tax`,
    ADD `tax_rate` DECIMAL(5,2) DEFAULT 0 AFTER `cost_price`,
    ADD `tax_amount` DECIMAL(10,2) DEFAULT 0 AFTER `tax_rate`,
    ADD `tax_inclusive` BOOLEAN DEFAULT TRUE AFTER `tax_amount`,
    ADD `percentage` DECIMAL(5,2) DEFAULT 0 AFTER `tax_inclusive`;
    """,
    """
    ALTER TABLE `orders` ADD `tax_amount` DECIMAL(10,2) DEFAULT 0 AFTER `coupon_coast`,
    ADD `tax_details` JSON NULL AFTER `tax_amount`,
    ADD `odoo_order_id` VARCHAR(255) NULL DEFAULT NULL AFTER `remote_key_id`,
    ADD `stock_status` VARCHAR(100) NULL DEFAULT NULL AFTER `odoo_order_id`;
    """,
]


class ConnectionPool:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.pool = []
            self.pool_size = 2  # Reduced to avoid overwhelming the DB
            self.current_size = 0
            self.lock = threading.Lock()
            self.initialized = True
    
    def get_connection(self):
        with self.lock:
            if self.pool:
                return self.pool.pop()
            elif self.current_size < self.pool_size:
                self.current_size += 1
                return self._create_connection()
            else:
                # If pool is full, create a temporary connection
                return self._create_connection()
    
    def return_connection(self, conn):
        if conn and conn.open:
            with self.lock:
                if len(self.pool) < self.pool_size:
                    self.pool.append(conn)
                else:
                    conn.close()
                    self.current_size -= 1
    
    def _create_connection(self, retries=3):
        for attempt in range(retries):
            try:
                return pymysql.connect(
                    host=os.getenv("DB_HOST"),
                    user=os.getenv("DB_USER"),
                    port=int(os.getenv("DB_PORT")),
                    password=os.getenv("DB_PASSWORD"),
                    database=os.getenv("DB_NAME"),
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=10,
                    read_timeout=30,
                    write_timeout=30,
                )
            except pymysql.err.OperationalError as e:
                if e.args[0] == 1040 and attempt < retries - 1:  # Too many connections
                    wait_time = (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff with jitter
                    print(f"[Connection Pool] Too many connections, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{retries})")
                    time.sleep(wait_time)
                else:
                    raise
    
    def close_all(self):
        with self.lock:
            for conn in self.pool:
                try:
                    conn.close()
                except:
                    pass
            self.pool.clear()
            self.current_size = 0

class SQLConnector:
    _results = None
    _debug = False
    _pool = ConnectionPool()

    def __init__(self, debug=False) -> None:
        self._debug = debug
        self.connection = None

    @contextmanager
    def get_connection(self):
        conn = self._pool.get_connection()
        try:
            self.connection = conn
            yield conn
        finally:
            self._pool.return_connection(conn)
            self.connection = None

    def onDebug(self, msg):
        if self._debug:
            print("[db.debug] ", msg)
            print("[db.debug._results] ", json.dumps(self._results, default=str))
        return self

    def migrate(self):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                print ("[migration] started")
                for migration in migrations:
                    try:
                        cursor.execute(migration)
                        conn.commit()
                    except Exception as e:
                        # rollback
                        self.onDebug("[migration] rollback" + str(e))
                        conn.rollback()
                    pass
        return self
       

    def getAll(self, table_name, where_clause=None, fields=None, select="*"):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
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
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
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
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                fields = ", ".join([f"{key} = '{value}'" for key, value in data.items()])
                # add updated at now to fields
                fields = fields + ", updated_at = NOW()"
                sql = f"UPDATE {table_name} SET {fields} WHERE {where_clause}"
                self.onDebug("[sql.update] %s" % sql)
                cursor.execute(sql)
                conn.commit()
                self.getOne(table_name, where_clause)
                return self

    def insert(self, table_name, data, where_clause=None):
        data = self.sanatize(data)
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                fields = ", ".join(data.keys())
                values = ", ".join([f"'{value}'" for value in data.values()])

                # add created at and updated at
                fields += ", updated_at, created_at"
                values += ", NOW(), NOW()"

                sql = f"INSERT INTO {table_name} ({fields}) VALUES ({values})"
                cursor.execute(sql)
                self.onDebug("[sql.update] %s" % sql)
                conn.commit()
                if where_clause is not None:
                    self.getOne(table_name, where_clause)
                return self

    def delete(self, table_name, where_clause):
        """Delete records from table based on where clause"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = f"DELETE FROM {table_name} WHERE {where_clause}"
                self.onDebug("[sql.delete] %s" % sql)
                cursor.execute(sql)
                conn.commit()
                self._results = cursor.rowcount
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
        json_data = self.toJSON()
        if json_data is None:
            return None
        try:
            return json.loads(json_data)
        except (json.JSONDecodeError, TypeError) as e:
            print(f"❌ JSON decode error: {str(e)}")
            return None

    def toHTML(self):
        return print_html(self.toJSON())
