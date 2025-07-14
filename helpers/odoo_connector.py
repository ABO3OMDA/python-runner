
import os

import xmlrpc.client

from dotenv import load_dotenv

load_dotenv()

class odoo_configs:
    url = os.getenv("ODOO_URL")
    db = os.getenv("ODOO_DB")
    username = os.getenv("ODOO_USER")
    password = os.getenv("ODOO_PASS")
    
    def __init__(self):
        self.url = os.getenv("ODOO_URL")
        self.db =  os.getenv("ODOO_DB")
        self.username = os.getenv("ODOO_USER")
        self.password = os.getenv("ODOO_PASS")
        pass

class OdooConnector:
    def __init__(self):
        self.url = odoo_configs().url
        self.db = odoo_configs().db
        self.username = odoo_configs().username
        self.password = odoo_configs().password
        self.common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(self.url))
        self.uid = self.common.authenticate(self.db, self.username, self.password, {})
        self.models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self.url))

    def search(self, model, domain, offset=0, limit=0):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'search', [domain], {'offset': offset, 'limit': limit})

    def read(self, model, ids, fields):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'read', [ids], {'fields': fields})

    def write(self, model, ids, values):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'write', [ids, values])

    def create(self, model, values):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'create', [values])

    def unlink(self, model, ids):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'unlink', [ids])

    def get_model_fields(self, model):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'fields_get', [], {'attributes': ['string']})

    def get_model_domain(self, model):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'fields_get', [], {'attributes': ['domain']})

    def get_model_constraints(self, model):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'fields_get', [], {'attributes': ['constraints']})

    def get_model_defaults(self, model):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'default_get', [])

    def get_model_access(self, model):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'check_access_rights', ['write', False])

    def get_model_access_create(self, model):
        return self.models.execute_kw(self.db, self.uid, self.password, model, 'check_access_rights', ['create', False])
