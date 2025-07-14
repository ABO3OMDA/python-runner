import datetime
import json

from numpy import number
from helpers.helpers import slugify
from helpers.odoo_connector import OdooConnector
from helpers.sql_connector import SQLConnector
from helpers.stock_helpers import StockPickingOrder
from helpers.user_helpers import UsersHelper


class SalesOrderHelper:
    connector: OdooConnector
    sql_connector: SQLConnector
    default_fields = []

    def __init__(self, connector: OdooConnector, sql_connector: SQLConnector):
        self.connector = connector
        self.sql_connector = sql_connector
        self.default_fields = [
            "id",
            "name",
            "partner_id",
            "date_order",
            "amount_total",
            "state",
            "delivery_message",
            "delivery_status",
            "invoice_status",
        ]

    def getOdooLines(self, data):
        order_details = data.get("order")

        if data.get("user") is None:
            raise Exception("User is required")
        if order_details is None:
            raise Exception("order is required")

        if (
            order_details.get("products") is None
            or len(order_details.get("products")) <= 0
        ):
            raise Exception("products is required")

        products = order_details.get("products")
        odooLines = []

        for product_line in products:
            if product_line.get("sku") is None:
                raise Exception("sku is required")
            if product_line.get("quantity") is None:
                raise Exception("quantity is required")
            if product_line.get("price") is None:
                raise Exception("price is required")

            product_id = self.connector.search(
                "product.product",
                [("default_code", "=", product_line.get("sku"))],
                offset=0,
                limit=1,
            )

            if len(product_id) <= 0:
                raise Exception("Product %s not found" % product_line.get("sku"))

            # check product availablity
            [odooProduct] = self.connector.read(
                "product.product",
                product_id,
                ["qty_available", "name", "uom_id", "display_name"],
            )

            #print("[sales_order] odooProduct : ", odooProduct)

            if float(odooProduct["qty_available"]) < float(
                product_line.get("quantity")
            ):
                raise Exception(
                    "Product %s doesnt have enough quantity for this order "
                    % product_line.get("sku")
                )

            odooLines.append(
                {
                    "product_id": product_id[0],
                    "product_uom_qty": float(product_line.get("quantity")),
                    "price_unit": float(product_line.get("price")),
                    "name": odooProduct.get("display_name"),
                    "product_uom": odooProduct.get("uom_id")[0],
                    "price_subtotal": float(product_line.get("price"))
                    * float(product_line.get("quantity")),
                }
            )

        return odooLines

    def onSalesOrderRequested(self, data):
        odooLines = self.getOdooLines(data)
        order_details = data.get("order")

        if order_details.get("order_number") is None:
            raise Exception("order_number is required")

        order_number = "WSSO-%s" % order_details.get("order_number")
        if self.getByOrderNumber(order_number) is not None:
            raise Exception("order_number already exists!")

        if len(odooLines) > 0:
            # create sales order
            user_helper = UsersHelper(self.connector, self.sql_connector)
            user_id = user_helper.upserOdooUser(data.get("user"))

            sales_order = {
                "partner_id": user_id,
                "date_order": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "name": order_number,
                "lines": odooLines,
            }
            #print("[sales_order] onSalesOrderRequested : ", sales_order)
            return self.create(sales_order)
        else:
            raise Exception("No products found in the order")

    def getById(self, id):
        order = self.connector.read("sale.order", id, self.default_fields)
        if len(order) <= 0:
            return None
        order = order[0]
        order['delivery'] = StockPickingOrder(self.connector, self.sql_connector).getBySaleOrder(order['id'])
        return order

    def getByOrderNumber(self, order_number):
        ids = self.connector.search(
            "sale.order", [("name", "like", order_number)], offset=0, limit=1
        )
        if len(ids) <= 0:
            return None
        return self.getById(ids[0])

    def onSalesOrderUpdated(self, data):
        order_number = data.get("order_number")
        if order_number is None:
            raise Exception("order_number is required")

        order = self.getByOrderNumber(order_number)
        if order is None:
            return None

        return self.update(order, data)

    def update(self, order, data):
        status = data.get("state")
        if status is None:
            raise Exception("status is required")

        #print("[sales_order] update : ", order)
        allowed_states = ["draft", "sent", "sale"]
        # check if order in allowed states
        if order["state"] not in allowed_states:
            raise Exception("Invalid state, current order state is %s" % order["state"])

        return self.connector.write("sale.order", order["id"], {"state": status})

    def create(self, data):
        # delete lines and save it to its own variables
        lines = data["lines"]
        del data["lines"]
        #print("[sales_order] create : ", data)
        order = self.connector.create("sale.order", data)

        lines = self.connector.create(
            "sale.order.line",
            [
                {
                    "order_id": order,
                    "product_id": line["product_id"],
                    "product_uom_qty": line["product_uom_qty"],
                    "price_unit": line["price_unit"],
                    "name": line["name"],
                    "product_uom": line["product_uom"],
                    "price_subtotal": line["price_subtotal"],
                }
                for line in lines
            ],
        )

        return self.getById(order)
