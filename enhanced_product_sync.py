#!/usr/bin/env python3
"""
Enhanced product sync with quantity and image change detection
"""
import sys
import os
from datetime import datetime, timedelta
from time import sleep

sys.path.insert(0, 'helpers/')

from helpers.file_helper import read_time_stamp, write_time_stamp
from helpers.helpers import flatten, odooReadSearch
from helpers.odoo_connector import OdooConnector
from helpers.product_helpers import ProductHelper
from helpers.sql_connector import SQLConnector

def detect_quantity_changes(connector, sql_connector, helper, limit=100):
    """Detect and sync products with quantity changes"""
    print("\n🔢 Detecting quantity changes...")
    
    try:
        # Get all synced products from Laravel
        synced_products = sql_connector.getAll(
            "products", 
            "`remote_key_id` IS NOT NULL AND `remote_key_id` != ''"
        ).fetch()
        
        if not synced_products:
            print("❌ No synced products found")
            return 0
        
        print(f"📊 Checking {len(synced_products)} synced products for quantity changes")
        
        # Get Odoo product IDs
        odoo_ids = [int(p['remote_key_id']) for p in synced_products]
        
        # Fetch current quantities from Odoo in batches
        batch_size = 50
        updated_count = 0
        
        for i in range(0, len(odoo_ids), batch_size):
            batch_ids = odoo_ids[i:i + batch_size]
            
            # Get current Odoo quantities
            odoo_products = connector.read(
                'product.template',
                batch_ids,
                ['id', 'qty_available', 'write_date']
            )
            
            for odoo_product in odoo_products:
                # Find corresponding Laravel product
                laravel_product = next(
                    (p for p in synced_products if int(p['remote_key_id']) == odoo_product['id']), 
                    None
                )
                
                if laravel_product:
                    odoo_qty = int(odoo_product['qty_available'])
                    laravel_qty = int(laravel_product['qty'])
                    
                    if odoo_qty != laravel_qty:
                        print(f"🔄 Quantity change detected:")
                        print(f"   Product: {laravel_product['name']}")
                        print(f"   Laravel QTY: {laravel_qty} → Odoo QTY: {odoo_qty}")
                        
                        # Update Laravel quantity
                        sql_connector.update(
                            "products",
                            f"`id` = '{laravel_product['id']}'",
                            {"qty": odoo_qty}
                        )
                        
                        # Also update variants
                        update_variant_quantities(connector, sql_connector, odoo_product['id'], laravel_product['id'])
                        
                        updated_count += 1
                        print(f"   ✅ Updated quantity: {laravel_qty} → {odoo_qty}")
        
        print(f"🎉 Updated {updated_count} products with quantity changes")
        return updated_count
        
    except Exception as e:
        print(f"❌ Quantity change detection failed: {str(e)}")
        return 0

def update_variant_quantities(connector, sql_connector, template_id, laravel_product_id):
    """Update variant quantities for a specific product"""
    try:
        # Get Odoo variants
        variant_ids = connector.search('product.product', [('product_tmpl_id', '=', template_id)])
        if not variant_ids:
            return
            
        variants = connector.read('product.product', variant_ids, ['id', 'default_code', 'qty_available'])
        
        for variant in variants:
            if variant.get('default_code'):  # Only update variants with SKU
                # Update Laravel variant
                sql_connector.update(
                    "product_variants",
                    f"`product_id` = '{laravel_product_id}' AND `remote_key_id` = '{variant['id']}'",
                    {"stock": variant['qty_available']}
                )
                
    except Exception as e:
        print(f"⚠️  Variant quantity update failed for template {template_id}: {str(e)}")

def detect_image_changes(connector, sql_connector, helper, limit=100):
    """Detect and sync products with image changes"""
    print("\n🖼️  Detecting image changes...")
    
    try:
        # Get products that might have image changes (recently updated)
        last_sync_at = read_time_stamp("product_time_stamp.txt")
        
        # Get products updated in the last 2 hours (more frequent for images)
        recent_time = (datetime.now() - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
        
        updated_products = odooReadSearch(
            connector,
            "product.template",
            where_clause=["write_date", ">=", recent_time],
            sFields=[
                "id", "name", "write_date", 
                "image_1920", "image_1024", "image_512",
                # Gallery image fields
                "image_1", "image_2", "image_3", "image_4", "image_5",
                "image_6", "image_7", "image_8", "image_9", "image_10"
            ],
            limit=limit
        )
        
        if not updated_products:
            print("❌ No recently updated products found")
            return 0
        
        print(f"📊 Checking {len(updated_products)} recently updated products for image changes")
        
        updated_count = 0
        
        for product in updated_products:
            try:
                # Check if this product exists in Laravel
                laravel_product = sql_connector.getOne(
                    "products", 
                    f"`remote_key_id` = '{product['id']}'"
                ).fetch()
                
                if not laravel_product:
                    continue
                
                print(f"\n🔍 Checking images for: {product['name']} (ID: {product['id']})")
                
                # Check main image change
                main_image_changed = check_main_image_change(product, laravel_product, helper)
                
                # Check gallery image changes
                gallery_changed = check_gallery_image_changes(product, laravel_product, helper, sql_connector)
                
                if main_image_changed or gallery_changed:
                    updated_count += 1
                    print(f"✅ Updated images for: {product['name']}")
                
            except Exception as e:
                print(f"⚠️  Error checking images for product {product.get('id', 'unknown')}: {str(e)}")
                continue
        
        print(f"🎉 Updated {updated_count} products with image changes")
        return updated_count
        
    except Exception as e:
        print(f"❌ Image change detection failed: {str(e)}")
        return 0

def check_main_image_change(odoo_product, laravel_product, helper):
    """Check if main product image has changed"""
    try:
        if not odoo_product.get('image_1920'):
            return False
        
        # Generate new Odoo URL
        new_image_url = f"https://odoo.eboutiques.com/public/product_image/{odoo_product['id']}/image_1920"
        current_image = laravel_product.get('thumb_image', '')
        
        # If URLs are different, update
        if new_image_url != current_image:
            print(f"  🔄 Main image changed:")
            print(f"     Old: {current_image}")
            print(f"     New: {new_image_url}")
            
            # Test if new URL is valid
            import requests
            try:
                response = requests.head(new_image_url, timeout=5)
                if response.status_code == 200:
                    # Update Laravel product
                    helper.sql_connector.update(
                        "products",
                        f"`id` = '{laravel_product['id']}'",
                        {"thumb_image": new_image_url}
                    )
                    print(f"     ✅ Main image updated")
                    return True
                else:
                    print(f"     ⚠️  New image URL not accessible")
            except Exception as e:
                print(f"     ⚠️  Failed to verify new image URL: {str(e)}")
        
        return False
        
    except Exception as e:
        print(f"  ❌ Main image check failed: {str(e)}")
        return False

def check_gallery_image_changes(odoo_product, laravel_product, helper, sql_connector):
    """Check if gallery images have changed"""
    try:
        print(f"  🖼️  Checking gallery images...")
        
        # Re-sync gallery (this will detect additions/removals)
        gallery_count = helper.sync_product_gallery(odoo_product['id'], laravel_product['id'])
        
        if gallery_count > 0:
            print(f"  ✅ Gallery updated: {gallery_count} images")
            return True
        
        return False
        
    except Exception as e:
        print(f"  ❌ Gallery check failed: {str(e)}")
        return False

def enhanced_product_sync_runner():
    """Enhanced product sync with quantity and image change detection"""
    print("🚀 Starting enhanced product sync with change detection...")
    
    try:
        connector = OdooConnector()
        sql_connector = SQLConnector()
        helper = ProductHelper(connector, sql_connector)
        
        # 1. Regular product sync (for new products and major changes)
        print("\n" + "="*60)
        print("1️⃣  REGULAR PRODUCT SYNC")
        print("="*60)
        
        from product_service_runner import sync_product_updates
        sync_product_updates(connector, sql_connector, helper, limit=20)
        
        # 2. Quantity change detection
        print("\n" + "="*60)
        print("2️⃣  QUANTITY CHANGE DETECTION")
        print("="*60)
        
        qty_updates = detect_quantity_changes(connector, sql_connector, helper, limit=100)
        
        # 3. Image change detection  
        print("\n" + "="*60)
        print("3️⃣  IMAGE CHANGE DETECTION")
        print("="*60)
        
        img_updates = detect_image_changes(connector, sql_connector, helper, limit=50)
        
        # Update timestamp
        write_time_stamp("product_time_stamp.txt")
        
        print(f"\n🎉 SYNC SUMMARY:")
        print(f"   - Quantity updates: {qty_updates}")
        print(f"   - Image updates: {img_updates}")
        print(f"   - Next sync in 30 seconds...")
        
    except Exception as e:
        print(f"❌ Enhanced sync failed: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("😴 Sleeping for 30 seconds...")
    sleep(30)
    
    # Recursive call for continuous sync
    enhanced_product_sync_runner()

if __name__ == "__main__":
    enhanced_product_sync_runner()