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

def quick_quantity_sync(connector, sql_connector, limit=50):
    """Quick sync for quantity changes only - FIXED VERSION"""
    print(f"\n🔢 Quick quantity sync...")
    
    try:
        # Get recently synced products with potential quantity changes
        synced_products = sql_connector.getAll(
            "products", 
            "`remote_key_id` IS NOT NULL AND `remote_key_id` != ''", 
            select="id, remote_key_id, name, qty"
        ).fetch()
        
        if not synced_products:
            print("❌ No synced products found")
            return 0
        
        print(f"📊 Found {len(synced_products)} synced products")
        
        # Process in batches to avoid overwhelming the system
        batch_size = 20
        total_updated = 0
        total_checked = 0
        
        for i in range(0, len(synced_products), batch_size):
            batch = synced_products[i:i + batch_size]
            odoo_ids = [int(p['remote_key_id']) for p in batch]
            
            print(f"🔍 Checking batch {i//batch_size + 1}: {len(odoo_ids)} products")
            
            try:
                # Get current Odoo quantities
                odoo_products = connector.read('product.template', odoo_ids, ['id', 'qty_available', 'name'])
                
                if not odoo_products:
                    continue
                
                for odoo_product in odoo_products:
                    # Find corresponding Laravel product
                    laravel_product = next(
                        (p for p in batch if int(p['remote_key_id']) == odoo_product['id']), 
                        None
                    )
                    
                    if laravel_product:
                        odoo_qty = int(odoo_product.get('qty_available', 0))
                        laravel_qty = int(laravel_product.get('qty', 0))
                        total_checked += 1
                        
                        if odoo_qty != laravel_qty:
                            print(f"  🔄 {laravel_product['name']}: Laravel={laravel_qty}, Odoo={odoo_qty}")
                            
                            # Update product quantity
                            try:
                                update_result = sql_connector.update(
                                    "products", 
                                    f"`id` = '{laravel_product['id']}'", 
                                    {"qty": odoo_qty}
                                )
                                
                                # Verify update was successful
                                if update_result and update_result._results:
                                    print(f"    ✅ UPDATED: {laravel_qty} → {odoo_qty}")
                                    
                                    # Also update variants for this product
                                    update_variant_quantities(
                                        connector, 
                                        sql_connector, 
                                        odoo_product['id'], 
                                        laravel_product['id']
                                    )
                                    
                                    total_updated += 1
                                else:
                                    print(f"    ❌ Update failed for product ID {laravel_product['id']}")
                                    
                            except Exception as e:
                                print(f"    ❌ Error updating product {laravel_product['id']}: {str(e)}")
                        else:
                            print(f"  ✅ {laravel_product['name']}: No change (qty={laravel_qty})")
                            
            except Exception as e:
                print(f"  ❌ Error processing batch: {str(e)}")
                continue
        
        print(f"\n✅ Quantity sync completed: Checked {total_checked}, Updated {total_updated}")
        return total_updated
        
    except Exception as e:
        print(f"❌ Quick quantity sync failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0

def update_variant_quantities(connector, sql_connector, template_id, laravel_product_id):
    """Update variant quantities for a specific product"""
    try:
        # Get all variants for this template
        variant_ids = connector.search('product.product', [('product_tmpl_id', '=', template_id)])
        
        if not variant_ids:
            return
        
        # Get variant details
        variants = connector.read('product.product', variant_ids, ['id', 'default_code', 'qty_available'])
        
        updated_variants = 0
        
        for variant in variants:
            if variant.get('default_code'):  # Only update variants with SKU
                try:
                    # Update Laravel variant stock
                    update_data = {"stock": int(variant.get('qty_available', 0))}
                    
                    # Update by remote_key_id if available
                    where_clause = f"`product_id` = '{laravel_product_id}' AND `remote_key_id` = '{variant['id']}'"
                    
                    # First try to update by remote_key_id
                    result = sql_connector.update("product_variants", where_clause, update_data)
                    
                    # If no rows affected, try by SKU
                    if not result or not result._results:
                        where_clause = f"`product_id` = '{laravel_product_id}' AND `sku` = '{variant['default_code']}'"
                        result = sql_connector.update("product_variants", where_clause, update_data)
                    
                    if result and result._results:
                        updated_variants += 1
                        print(f"      ✅ Updated variant {variant['default_code']}: stock={variant['qty_available']}")
                        
                except Exception as e:
                    print(f"      ⚠️ Failed to update variant {variant.get('default_code', 'unknown')}: {str(e)}")
        
        if updated_variants > 0:
            print(f"    📦 Updated {updated_variants} variants")
            
    except Exception as e:
        print(f"    ⚠️ Variant quantity update failed for template {template_id}: {str(e)}")

def detect_quantity_changes_enhanced(connector, sql_connector, limit=100):
    """Enhanced quantity change detection with better error handling"""
    print("\n🔢 Enhanced quantity change detection...")
    
    try:
        # Get products that need quantity checks
        # Focus on products with non-zero quantities first (more likely to change)
        synced_products = sql_connector.getAll(
            "products", 
            "`remote_key_id` IS NOT NULL AND `remote_key_id` != '' AND `qty` > 0",
            select="id, remote_key_id, name, qty, updated_at"
        ).fetch()
        
        if not synced_products:
            print("❌ No synced products with stock found")
            return 0
        
        # Sort by last update to prioritize recently changed products
        synced_products.sort(key=lambda x: x.get('updated_at', ''), reverse=True)
        
        # Limit to most recent products
        synced_products = synced_products[:limit]
        
        print(f"📊 Checking {len(synced_products)} products for quantity changes")
        
        updated_count = 0
        errors_count = 0
        
        # Process in smaller batches for better reliability
        batch_size = 10
        
        for i in range(0, len(synced_products), batch_size):
            batch = synced_products[i:i + batch_size]
            batch_ids = [int(p['remote_key_id']) for p in batch]
            
            try:
                # Get Odoo data with retry logic
                odoo_data = None
                for attempt in range(3):
                    try:
                        odoo_data = connector.read(
                            'product.template',
                            batch_ids,
                            ['id', 'qty_available', 'name', 'write_date']
                        )
                        break
                    except Exception as e:
                        if attempt < 2:
                            print(f"  ⚠️ Retry {attempt + 1}/3 for batch read...")
                            sleep(1)
                        else:
                            raise
                
                if not odoo_data:
                    continue
                
                for odoo_product in odoo_data:
                    laravel_product = next(
                        (p for p in batch if int(p['remote_key_id']) == odoo_product['id']),
                        None
                    )
                    
                    if laravel_product:
                        odoo_qty = int(odoo_product.get('qty_available', 0))
                        laravel_qty = int(laravel_product.get('qty', 0))
                        
                        if odoo_qty != laravel_qty:
                            print(f"\n🔄 Quantity change detected:")
                            print(f"   Product: {laravel_product['name']}")
                            print(f"   Laravel: {laravel_qty} → Odoo: {odoo_qty}")
                            print(f"   Last Odoo update: {odoo_product.get('write_date', 'Unknown')}")
                            
                            # Perform the update with verification
                            try:
                                # Update with explicit commit
                                with sql_connector.get_connection() as conn:
                                    with conn.cursor() as cursor:
                                        update_sql = f"""
                                        UPDATE products 
                                        SET qty = {odoo_qty}, 
                                            updated_at = NOW() 
                                        WHERE id = {laravel_product['id']}
                                        """
                                        cursor.execute(update_sql)
                                        conn.commit()
                                        
                                        # Verify the update
                                        cursor.execute(
                                            f"SELECT qty FROM products WHERE id = {laravel_product['id']}"
                                        )
                                        result = cursor.fetchone()
                                        
                                        if result and result['qty'] == odoo_qty:
                                            print(f"   ✅ Successfully updated to {odoo_qty}")
                                            updated_count += 1
                                            
                                            # Update variants too
                                            update_variant_quantities(
                                                connector,
                                                sql_connector,
                                                odoo_product['id'],
                                                laravel_product['id']
                                            )
                                        else:
                                            print(f"   ❌ Update verification failed")
                                            errors_count += 1
                                            
                            except Exception as e:
                                print(f"   ❌ Update failed: {str(e)}")
                                errors_count += 1
                                
            except Exception as e:
                print(f"  ❌ Batch processing error: {str(e)}")
                errors_count += len(batch)
                continue
        
        print(f"\n🎉 Enhanced quantity sync completed:")
        print(f"   ✅ Updated: {updated_count} products")
        print(f"   ❌ Errors: {errors_count}")
        
        return updated_count
        
    except Exception as e:
        print(f"❌ Enhanced quantity detection failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0

def verify_database_connection(sql_connector):
    """Verify database connection is working properly"""
    try:
        # Test simple query
        result = sql_connector.getOne("products", "1=1 LIMIT 1").fetch()
        if result:
            print(f"✅ Database connection verified")
            return True
        else:
            print(f"⚠️ Database empty or connection issue")
            return False
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        return False

def enhanced_product_sync_runner():
    """Enhanced product sync with better quantity change detection"""
    print("🚀 Starting enhanced product sync with improved quantity updates...")
    
    try:
        connector = OdooConnector()
        sql_connector = SQLConnector()
        helper = ProductHelper(connector, sql_connector)
        
        # Verify database connection first
        if not verify_database_connection(sql_connector):
            print("❌ Database connection issue - retrying in 60 seconds...")
            sleep(60)
            return enhanced_product_sync_runner()
        
        # Run migrations
        print("🔧 Running database migrations...")
        sql_connector.migrate()
        
        # 1. Regular product sync (for new products and major changes)
        print("\n" + "="*60)
        print("1️⃣ REGULAR PRODUCT SYNC")
        print("="*60)
        
        from product_service_runner import sync_product_updates
        sync_product_updates(connector, sql_connector, helper, limit=10)
        
        # 2. Enhanced quantity change detection
        print("\n" + "="*60)
        print("2️⃣ ENHANCED QUANTITY CHANGE DETECTION")
        print("="*60)
        
        qty_updates = detect_quantity_changes_enhanced(connector, sql_connector, limit=50)
        
        # 3. Quick quantity sync for remaining products
        print("\n" + "="*60)
        print("3️⃣ QUICK QUANTITY SYNC")
        print("="*60)
        
        quick_updates = quick_quantity_sync(connector, sql_connector, limit=100)
        
        # 4. Image change detection (less frequent)
        if datetime.now().minute % 5 == 0:  # Run every 5 minutes
            print("\n" + "="*60)
            print("4️⃣ IMAGE CHANGE DETECTION")
            print("="*60)
            
            from enhanced_product_sync import detect_image_changes
            img_updates = detect_image_changes(connector, sql_connector, helper, limit=20)
        else:
            img_updates = 0
        
        # Update timestamp
        write_time_stamp("product_time_stamp.txt")
        
        print(f"\n🎉 SYNC SUMMARY:")
        print(f"   - Enhanced quantity updates: {qty_updates}")
        print(f"   - Quick quantity updates: {quick_updates}")
        print(f"   - Image updates: {img_updates}")
        print(f"   - Total quantity updates: {qty_updates + quick_updates}")
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