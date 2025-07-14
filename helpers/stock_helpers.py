import datetime
import json

from numpy import number
from helpers.helpers import slugify
from helpers.odoo_connector import OdooConnector
from helpers.sql_connector import SQLConnector
from helpers.user_helpers import UsersHelper


class StockPickingOrder:
    connector: OdooConnector
    sql_connector: SQLConnector
    default_fields = []

    def __init__(self, connector: OdooConnector, sql_connector: SQLConnector):
        self.connector = connector
        self.sql_connector = sql_connector
        self.default_fields = [
            "id",
            "state",
        ]

   

    def getBySaleOrder(self, sale_order_id):
        picking = self.connector.search("stock.picking", [("sale_id", "=", sale_order_id)])
        if len(picking) <= 0:
            return None
        print("[INFO] picking: ", picking)
        picking = self.getById(picking[0])
        return picking

    def getById(self, id):
        picking = self.connector.read("stock.picking", id, self.default_fields)
        if len(picking) <= 0:
            return None
        print("[INFO] picking: ", picking)
        picking = picking[0]
        return picking

   