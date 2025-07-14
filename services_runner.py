from time import sleep
from helpers.sql_connector import SQLConnector
from product_service_runner import __product_service_runner__


SQLConnector().migrate()
sleep(5)


__product_service_runner__()
