import sys
import xmlrpc.client
from time import sleep

from json2html import *

from helpers.file_helper import read_time_stamp, write_time_stamp
from helpers.helpers import flatten, odooReadSearch
from helpers.odoo_connector import OdooConnector
from helpers.product_helpers import ProductHelper
from helpers.sql_connector import SQLConnector

sys.path.insert(0, 'helpers/')

def __product_service_runner__():
    #print("Hello from  product_service_runner.py")
    connector = OdooConnector()

    last_sync_at = read_time_stamp("product_time_stamp.txt")
    #print("Last sync at: ", last_sync_at)


    product_templates = odooReadSearch(
        connector,
        "product.template",
        where_clause=["write_date", ">=", last_sync_at],
        sFields=[
            "name",
            "list_price",
            "standard_price",
            "type",
            "qty_available",
            "product_tag_ids",
            "default_code",
            "id",
            "write_date",
            "weight",
        ],
        limit= 10000, 
        offset= 0,
    )
    products = odooReadSearch(
        connector,
        "product.product",
        where_clause=["product_tmpl_id", "in", [pt["id"] for pt in product_templates]],
        sFields=[
            "name",
            "display_name",
            "code",
            "default_code",
            "id",
            "product_template_variant_value_ids",
            "product_tmpl_id",
            "qty_available",
            "lst_price",
            "standard_price",
            "weight",
        ],
        limit= 10000,
        offset= 0,
    )

    # fetch product attributes
    product_attr = odooReadSearch(
        connector,
        "product.template.attribute.value",
        where_clause=[
            "id",
            "in",
            flatten([p["product_template_variant_value_ids"] for p in products]),
        ],
        sFields=["id", "html_color", "name", "attribute_line_id"],
        limit= 10000,
        offset= 0,
    )

    print("product_templates:", len(product_templates))
    print("products:", len(products))
    print("product_attr:", len(product_attr))

    for pt in product_templates:
        variants = []
        attrs = []
        for p in products:
            if p["product_tmpl_id"][0] == pt["id"]:
                variants.append(p)
                #print("[product] ", p["display_name"])
        for v in variants:
            for a in product_attr:
                if a["id"] in v["product_template_variant_value_ids"]:
                    attrs.append(a)
        print("\n------------ %s" % pt["name"])
        print("Variants: ", len(variants))
        ProductHelper(connector, SQLConnector(debug=False)).upsert_product_template(pt, variants, attrs)

    print("Done, sleep now...")
    write_time_stamp("product_time_stamp.txt")

    sleep(60)
    __product_service_runner__()


