from time import sleep
import sys
from helpers.sql_connector import SQLConnector
from product_service_runner import __product_service_runner__

def main():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            print(f"[Migration] Attempting migration (attempt {attempt + 1}/{max_retries})")
            SQLConnector().migrate()
            print("[Migration] Migration completed successfully")
            break
        except Exception as e:
            print(f"[Migration] Failed attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 5  # 5, 10, 20, 40 seconds
                print(f"[Migration] Retrying in {wait_time} seconds...")
                sleep(wait_time)
            else:
                print("[Migration] All migration attempts failed, exiting")
                sys.exit(1)
    
    sleep(5)
    
    try:
        print("[Service] Starting product service runner")
        __product_service_runner__()
    except Exception as e:
        print(f"[Service] Product service runner failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
