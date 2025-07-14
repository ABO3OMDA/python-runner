import json
from helpers import sql_connector
from helpers.helpers import slugify
from helpers.odoo_connector import OdooConnector
from helpers.sql_connector import SQLConnector



class ProductHelper:

    connector: OdooConnector
    sql_connector: SQLConnector


    def __init__(self, connector: OdooConnector, sql_connector: SQLConnector):
        self.connector = connector
        self.sql_connector = sql_connector
        
    def upsert_product_variant(self, v, attrs, product_id):
        #print(">> variant_id", v["id"])

        if (
            v["default_code"] is False
            or v["default_code"] is None
            or v["default_code"] == "False"
        ):
            #print(">>> variant has no sku")
            return

        details = []
        for a in attrs:
            details.append(
                {
                    "id": a["id"],
                    "name": a["html_color"] if a["html_color"] else a["name"],
                    "type": "Color" if a["html_color"] else "Text",
                    "typeName": a["attribute_line_id"][1],
                    "isActive": 1,
                }
            )

            # sort details by id
            details = sorted(details, key=lambda k: k["type"])
            
            #print(">>> attr_name", a["name"])

        if len(details) == 0:
            name = v["display_name"] if v["display_name"] is not None else "default"
            details = [
                {
                    "id": v["id"],
                    "name": name,
                    "type": "Text",
                    "typeName": name,
                    "isActive": 0,
                }
            ]

        sqlProdVariant = (
            self.sql_connector
            .upsert(
                "product_variants",
                {
                    "name": v["display_name"],
                    "product_id": product_id,
                    "sku": v["default_code"],
                    "stock": v["qty_available"],
                    "price": v["lst_price"],
                    "cost_price": v["standard_price"],
                    "percentage": (
                        0
                        if v["lst_price"] == 0
                        else round(v["standard_price"] / v["lst_price"] * 100)
                    ),
                    "weight": v["weight"] * 1000,
                    "details": json.dumps(details),
                    "status": 0,
                },
                updatedData={
                    "stock": v["qty_available"],
                    "details": json.dumps(details),
                    "name": v["display_name"],
                },
                where_clause=" `sku` = '%s' " % v["default_code"],
            )
            .fetch()
        )
        #print("\n")
        return sqlProdVariant


    def upsert_product_template(self, p, variants, attrs):
        #print("> product_id", p["id"])
        #print("> variants", len(variants))

        sqlProd = (
            self.sql_connector
            .upsert(
                "products",
                {
                    "name": p["name"],
                    "short_name": p["name"],
                    "slug": slugify(p["name"] + "-" + str(p["id"] ) + "-" + str(p["default_code"])),
                    "sku": p["default_code"],
                    "qty": p["qty_available"],
                    "thumb_image": "storage/website_images/Screenshot 2024-07-02 145345.png",
                    "category_id": 12,
                    "sub_category_id": 10,
                    "child_category_id": 0,
                    "weight": p["weight"] * 1000,
                    "seo_title": p["name"],
                    "seo_description": p["name"],
                    "price": p["list_price"],
                    "cost_price": p["standard_price"],
                    "short_description": p["name"],
                    "long_description": p["name"],
                    "status": 0,
                    "approve_by_admin": 0,
                    "uuid": "o_imported_%s" % p["id"],
                    "remote_key_id": p["id"],
                },
                updatedData={
                    "qty": p["qty_available"],
                    "remote_key_id": p["id"],
                    "cost_price": p["standard_price"],
                },
                where_clause=" `remote_key_id` = '%s' " % p["id"],
            )
            .fetch()
        )

        if sqlProd["id"] is None:
            #print("product already exists")
            return
        for v in variants:
            related_attr = []
            related_attr = [
                a for a in attrs if a["id"] in v["product_template_variant_value_ids"]
            ]
            # unique related_attr where id
            related_attr = list({v["id"]: v for v in related_attr}.values())
            self.upsert_product_variant(v, related_attr, sqlProd["id"])

            #print("[product] related_attr", len(related_attr))

        skus = [
            x
            for x in list(set([x["default_code"] for x in variants]))
            if isinstance(x, str)
        ]
        if len(skus) > 0:
            where = "`product_id` = '%s' AND `sku` not in ('%s')" % (
                p["id"],
                "', '".join(skus),
            )
            SQLConnector().update("product_variants", where, {"status": 0})

            print ("[product] skus", len(skus))
        print ("[product] done")
