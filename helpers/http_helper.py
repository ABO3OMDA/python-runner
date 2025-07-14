import json
from django.http import HttpResponseBadRequest

from helpers import odoo_connector
from helpers.odoo_connector import OdooConnector
from helpers.salesorder_helpers import SalesOrderHelper
from helpers.sql_connector import SQLConnector


class HttpHelper: 
    requestHandler = None
    odoo_connector: OdooConnector
    sql_connector: SQLConnector


    def __init__(self, requestHandler):
        self.requestHandler = requestHandler
        self.odoo_connector = OdooConnector()
        self.sql_connector = SQLConnector()



    def onGET(self, path):
        try: 
            response = {
                "message": "Success!",
                "data": {}
            }

            parsed_path  = ""
            self.sendJsonResponse(response)
            return self
        
        except Exception as e:
            return self.sendJsonResponse({"error": str(e)}, 400)
     

    def onPOST(self, body):
        try: 
            body = json.loads(body.decode('utf-8'))
            data = None
            
            if self.requestHandler.path == "/check/salesorder":
                data = SalesOrderHelper(self.odoo_connector, self.sql_connector).getOdooLines(body)
                self.sendJsonResponse({
                    "message": "Success!",
                    "data": data
                })
                return self

            if self.requestHandler.path == "/salesorder":
                data = SalesOrderHelper(self.odoo_connector, self.sql_connector).onSalesOrderRequested(body)
                #print("[INFO] onSalesOrderRequested.data: ", data)
                self.sendJsonResponse({
                    "message": "Success!",
                    "data": data
                })
                return self
            

            if self.requestHandler.path == "/salesorderbyid":
                order_id = body.get("order_id")
                data = SalesOrderHelper(self.odoo_connector, self.sql_connector).getById(order_id)
                if data is None:
                    return self.sendJsonResponse({"error": "Order not found"}, 404)
                self.sendJsonResponse({
                    "message": "Success!",
                    "data": data
                })
                return self


            if self.requestHandler.path == "/salesorderbyordernumber":
                order_number = body.get("order_number")
                data = SalesOrderHelper(self.odoo_connector, self.sql_connector).getByOrderNumber(order_number)
                if data is None:
                    return self.sendJsonResponse({"error": "Order not found"}, 404)
                self.sendJsonResponse({
                    "message": "Success!",
                    "data": data
                })
                return self
            
     
            
            return self.sendJsonResponse({"error": "404"}, 404)
        
        except Exception as e:
            return self.sendJsonResponse({"error": str(e)}, 400)

    def onPut(self, body):
        try: 
            body = json.loads(body.decode('utf-8'))
            data = None
            
            if self.requestHandler.path == "/salesorder":
                data = SalesOrderHelper(self.odoo_connector, self.sql_connector).onSalesOrderUpdated(body)
                self.sendJsonResponse({
                    "message": "Success!",
                    "data": data
                })
                return self
            
            return self.sendJsonResponse({"error": "404"}, 404)
        
        except Exception as e:
            return self.sendJsonResponse({"error": str(e)}, 400)

    def sendJsonResponse(self, data , httpCode = 200):
        self.sendResponse(httpCode, json.dumps(data), contentType="application/json")
        return self

    def sendResponse(self, httpCode, httpBody, contentType = "text/html"):
        self.requestHandler.send_response(httpCode)
        self.requestHandler.send_header('Content-type', contentType)
        self.requestHandler.end_headers()
        self.requestHandler.wfile.write(httpBody.encode('utf-8'))
        return self
    