import sys
import xmlrpc.client
import base64
import requests
import os
from datetime import datetime, timedelta
from time import sleep

from json2html import *

from helpers.file_helper import read_time_stamp, write_time_stamp
from helpers.helpers import flatten, odooReadSearch
from helpers.odoo_connector import OdooConnector
from helpers.product_helpers import ProductHelper
from helpers.sql_connector import SQLConnector

sys.path.insert(0, 'helpers/')

def get_odoo_image_url(product_id, field_name='image_1920'):
    """Generate Odoo public image URL - no downloading needed"""
    import requests
    
    try:
        # Generate Odoo public URL
        if field_name == 'image_1920':
            image_url = f"https://odoo.eboutiques.com/public/product_image/{product_id}/image_1920"
        else:
            # For other field names, try generic image_1
            image_url = f"https://odoo.eboutiques.com/public/product_image/{product_id}/image_1"
        
        print(f"🔗 Checking Odoo image URL: {image_url}")
        
        # Quick check if URL exists
        response = requests.head(image_url, timeout=5)
        if response.status_code == 200:
            print("✅ Odoo image URL is valid")
            return image_url
        else:
            print("⚠️  Odoo image URL not available")
            return "no_product_image.jpg"
            
    except Exception as e:
        print(f"❌ Failed to check image URL for product {product_id}: {str(e)}")
        return "no_product_image.jpg"

def sync_product_updates(connector, sql_connector, helper, limit=50):
    """Sync recent product updates from Odoo"""
    
    # Get timestamp for recent changes (last 24 hours or since last sync)
    last_sync_at = read_time_stamp("product_time_stamp.txt")
    print(f"Syncing products updated since: {last_sync_at}")
    
    try:
        # Get recently updated product templates
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
                "taxes_id",
                "supplier_taxes_id",
                "categ_id",
                "image_1920",  # Main product image
                "image_1024",  # Alternative image size  
                "image_512",   # Smaller image size
                "active"       # Product status
            ],
            limit=limit,
            offset=0,
        )
        
        print(f"📊 Found {len(product_templates)} updated product templates")
        
        if not product_templates:
            print("No updated products found")
            return
        
        # Get corresponding product variants
        template_ids = [pt["id"] for pt in product_templates]
        products = odooReadSearch(
            connector,
            "product.product",
            where_clause=["product_tmpl_id", "in", template_ids],
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
                "taxes_id",
                "supplier_taxes_id",
                "active",
                "image_1920"  # Variant image
            ],
            limit=limit * 5,  # More variants than templates
            offset=0,
        )
        
        print(f"📦 Found {len(products)} updated product variants")
        
        # Get product attributes
        all_variant_value_ids = flatten([p["product_template_variant_value_ids"] for p in products])
        product_attr = []
        
        if all_variant_value_ids:
            product_attr = odooReadSearch(
                connector,
                "product.template.attribute.value",
                where_clause=["id", "in", all_variant_value_ids],
                sFields=["id", "html_color", "name", "attribute_line_id"],
                limit=1000,
                offset=0,
            )
        
        # Process each updated product template
        for pt in product_templates:
            try:
                print(f"\n{'='*60}")
                print(f"📦 Processing: {pt['name']}")
                print(f"🔄 Last updated: {pt['write_date']}")
                print(f"💰 Price: {pt['list_price']}")
                print(f"📊 Stock: {pt['qty_available']}")
                print(f"🏷️  Has taxes: {bool(pt.get('taxes_id'))}")
                print(f"🖼️  Has image: {bool(pt.get('image_1920'))}")
                print(f"🔍 Image fields: image_1920={bool(pt.get('image_1920'))}, image_1024={bool(pt.get('image_1024'))}, image_512={bool(pt.get('image_512'))}")
                print(f"✅ Active: {pt.get('active', True)}")
                
                # Download product image if available (try different image fields)
                image_path = "storage/website_images/Screenshot 2024-07-02 145345.png"  # default
                if pt.get('image_1920'):
                    image_path = get_odoo_image_url(pt["id"], 'image_1920')
                elif pt.get('image_1024'):
                    image_path = get_odoo_image_url(pt["id"], 'image_1024') 
                elif pt.get('image_512'):
                    image_path = get_odoo_image_url(pt["id"], 'image_512')
                
                # Get variants for this template
                variants = [p for p in products if p["product_tmpl_id"][0] == pt["id"]]
                
                # Get attributes for these variants
                template_attrs = []
                for v in variants:
                    for a in product_attr:
                        if a["id"] in v["product_template_variant_value_ids"]:
                            template_attrs.append(a)
                
                # Remove duplicates
                template_attrs = list({a["id"]: a for a in template_attrs}.values())
                
                print(f"🔧 Found {len(variants)} variants with {len(template_attrs)} attributes")
                
                # Check if product exists in Laravel database
                existing = sql_connector.getOne("products", f"`remote_key_id` = '{pt['id']}'").fetch()
                if existing:
                    print(f"🔄 Updating existing product (Laravel ID: {existing['id']})")
                else:
                    print(f"➕ Creating new product")
                
                # Process the product with enhanced data
                enhanced_pt = pt.copy()
                enhanced_pt['downloaded_image_path'] = image_path
                
                # Sync the product
                helper.upsert_product_template(enhanced_pt, variants, template_attrs)
                
                print(f"✅ Successfully processed: {pt['name']}")
                
            except Exception as e:
                print(f"❌ Error processing product {pt.get('name', 'Unknown')}: {str(e)}")
                continue
        
        print(f"\n🎉 Sync completed! Processed {len(product_templates)} products")
        
    except Exception as e:
        print(f"❌ Sync failed: {str(e)}")
        import traceback
        traceback.print_exc()

def quick_quantity_sync(connector, sql_connector, limit=50):
    """Quick sync for quantity changes only"""
    print(f"\n🔢 Quick quantity sync...")
    
    try:
        # Get recently synced products with potential quantity changes
        synced_products = sql_connector.getAll(
            "products", 
            "remote_key_id IS NOT NULL", 
            select="id, remote_key_id, name, qty"
        ).fetch()
        
        if not synced_products:
            print("❌ No synced products found")
            return
        
        print(f"📊 Found {len(synced_products)} synced products")
        
        # Limit to first 20 for performance
        synced_products = synced_products[:20]
        odoo_ids = [int(p['remote_key_id']) for p in synced_products]
        
        print(f"🔍 Checking quantities for products: {odoo_ids}")
        
        # Get current Odoo quantities (using qty_available - will check other fields later)
        odoo_products = connector.read('product.template', odoo_ids, ['id', 'qty_available'])
        
        print(f"📊 Got {len(odoo_products)} products from Odoo")
        
        updated_count = 0
        checked_count = 0
        
        for odoo_product in odoo_products:
            laravel_product = next((p for p in synced_products if int(p['remote_key_id']) == odoo_product['id']), None)
            
            if laravel_product:
                odoo_qty = int(odoo_product['qty_available'])
                laravel_qty = int(laravel_product['qty'])
                checked_count += 1
                
                print(f"  🔍 {laravel_product['name']}: Laravel={laravel_qty}, Odoo={odoo_qty}")
                
                if odoo_qty != laravel_qty:
                    sql_connector.update("products", f"`id` = '{laravel_product['id']}'", {"qty": odoo_qty})
                    print(f"    🔄 UPDATED: {laravel_qty} → {odoo_qty}")
                    updated_count += 1
                else:
                    print(f"    ✅ No change needed")
        
        print(f"✅ Checked {checked_count} products, updated {updated_count} quantities")
        
    except Exception as e:
        print(f"❌ Quick quantity sync failed: {str(e)}")

def quick_image_sync(connector, sql_connector, helper, limit=30):
    """Quick sync for image changes"""
    print(f"\n🖼️  Quick image sync...")
    
    try:
        # Get products updated in last hour
        from datetime import datetime, timedelta
        recent_time = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        
        recent_products = odooReadSearch(
            connector,
            "product.template", 
            where_clause=["write_date", ">=", recent_time],
            sFields=["id", "name", "image_1920", "write_date"],
            limit=limit
        )
        
        if not recent_products:
            print("❌ No recently updated products")
            return
        
        updated_count = 0
        for product in recent_products:
            try:
                # Check if exists in Laravel
                laravel_product = sql_connector.getOne("products", f"`remote_key_id` = '{product['id']}'").fetch()
                if not laravel_product:
                    continue
                
                # Check main image
                if product.get('image_1920'):
                    new_url = f"https://odoo.eboutiques.com/public/product_image/{product['id']}/image_1920"
                    current_url = laravel_product.get('thumb_image', '')
                    
                    if new_url != current_url:
                        # Quick URL validation
                        import requests
                        try:
                            response = requests.head(new_url, timeout=3)
                            if response.status_code == 200:
                                sql_connector.update("products", f"`id` = '{laravel_product['id']}'", {"thumb_image": new_url})
                                print(f"  🔄 Updated image: {product['name']}")
                                updated_count += 1
                        except:
                            pass  # Skip if URL check fails
                
                # Quick gallery resync
                helper.sync_product_gallery(product['id'], laravel_product['id'])
                
            except Exception as e:
                print(f"  ⚠️  Error syncing {product.get('name', 'unknown')}: {str(e)}")
                continue
        
        print(f"✅ Updated {updated_count} product images")
        
    except Exception as e:
        print(f"❌ Quick image sync failed: {str(e)}")

def __product_service_runner__():
    """Enhanced product service runner with update detection"""
    print("🚀 Starting enhanced product sync with updates, images, and quantities...")
    
    try:
        connector = OdooConnector()
        sql_connector = SQLConnector()
        helper = ProductHelper(connector, sql_connector)
        
        # Run migrations to ensure latest database structure
        print("🔧 Running database migrations...")
        sql_connector.migrate()
        
        # 1. Sync recent updates
        sync_product_updates(connector, sql_connector, helper, limit=20)
        
        # 2. Quick quantity sync (every run)
        quick_quantity_sync(connector, sql_connector, limit=50)
        
        # 3. Image change detection (every run)
        quick_image_sync(connector, sql_connector, helper, limit=30)
        
        # Update timestamp for next run
        write_time_stamp("product_time_stamp.txt")
        print("📝 Updated sync timestamp")
        
    except Exception as e:
        print(f"❌ Service runner failed: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("😴 Sleeping for 30 seconds before next sync...")
    sleep(30)
    
    # Recursive call for continuous sync
    __product_service_runner__()

if __name__ == "__main__":
    __product_service_runner__()
