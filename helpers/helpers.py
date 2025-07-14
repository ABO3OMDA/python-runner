import json
import os
import uuid
import xmlrpc.client

from dotenv import load_dotenv
from IPython.display import HTML
from json2html import *

from helpers.odoo_connector import OdooConnector

load_dotenv()


def print_html(json_data):
    html = json2html.convert(json=json_data)  # noqa: F405
    return HTML(html)


def pretty_print(json_data):
    return json.dumps(json_data, indent=4)


def slugify(text):
    return text.lower().replace(" ", "_")


def get_uuid():
    return str(uuid.uuid4())


def flatten(l):
    return [item for sublist in l for item in sublist]



def odooReadSearch(connector: OdooConnector, model, where_clause=(), sFields=[],  offset=0, limit=0):
    if not sFields:
        fields =   connector.get_model_fields(model)
        sFields = fields.keys()
        sFields = list(sFields)
    search_ids = connector.search(model, [where_clause], offset=offset, limit=limit)
    return connector.read(model, search_ids, sFields)