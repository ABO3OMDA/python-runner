from helpers.helpers import slugify
from helpers.odoo_connector import OdooConnector
from helpers.sql_connector import SQLConnector



class UsersHelper: 
    connector: OdooConnector
    sql_connector: SQLConnector
    default_fields = []

    def __init__(self, connector: OdooConnector, sql_connector: SQLConnector):
        self.connector = connector
        self.sql_connector = sql_connector
        self.default_fields = ["name", "email", "phone", "mobile", "id", "write_date" , "create_date" , "address"]
        

    def upserOdooUser(self, data):

        if (data.get("email") is None):
            raise Exception("email is required")
        
        if (data.get("name") is None): 
            raise Exception("Name is required")
        
        if (data.get('phone') is None):
            raise Exception("Phone is required")

        if (data.get("contact_address") is None):
            raise Exception("contact_address is required")

        exists = self.connector.search("res.partner", [("email", "=", data["email"])], offset=0, limit=1)
        if (exists):
            return exists[0]
        return self.create(data)
    

    def getById(self, id):
        return self.connector.read("res.partner", id, self.default_fields)
    
    def create(self, data):
        address_line = data['contact_address']
        data['name'] = data['name'] + " - " + address_line
        contact = self.connector.create("res.partner", data)
        # contact address
        self.connector.create("res.partner" , [
            {
                "name": address_line,
                "contact_address_inline": address_line,
                "contact_address": address_line,
                "parent_id": contact,
                "type": "delivery",
                "stree": address_line
            }
        ])
        return contact