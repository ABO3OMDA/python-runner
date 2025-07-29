#!/usr/bin/env python3
"""
Test the working image examples (693, 701) and force sync them
"""
import sys
sys.path.insert(0, 'helpers/')

from helpers.odoo_connector import OdooConnector
from helpers.product_helpers import ProductHelper
from helpers.sql_connector import SQLConnector

def test_and_sync_working_products():
    """Test and force sync products with known working images"""
    print("🧪 Testing and syncing products with working images")
    print("="*60)
    
    working_product_ids = [693, 701]
    
    try:
        connector = OdooConnector()
        sql_connector = SQLConnector()
        helper = ProductHelper(connector, sql_connector)
        
        for product_id in working_product_ids:
            print(f"\n🔍 Testing product {product_id}:")
            
            # 1. Check if product exists in Odoo
            try:
                odoo_product = connector.read('product.template', [product_id], ['id', 'name', 'image_1920'])
                if odoo_product:
                    product = odoo_product[0]
                    print(f"✅ Found in Odoo: {product['name']}")
                    print(f"   Has main image: {bool(product.get('image_1920'))}")
                else:
                    print(f"❌ Product {product_id} not found in Odoo")
                    continue
            except Exception as e:
                print(f"❌ Error reading from Odoo: {str(e)}")
                continue
            
            # 2. Check if product exists in Laravel
            laravel_product = sql_connector.getOne("products", f"`remote_key_id` = '{product_id}'").fetch()
            if laravel_product:
                print(f"✅ Found in Laravel: ID {laravel_product['id']}")
                laravel_id = laravel_product['id']
            else:
                print(f"❌ Product {product_id} not synced to Laravel yet")
                continue
            
            # 3. Test image URLs manually
            import requests
            
            # Test main image
            main_url = f"https://odoo.eboutiques.com/public/product_image/{product_id}/image_1920"
            print(f"\n📷 Testing main image: {main_url}")
            try:
                response = requests.head(main_url, timeout=5)
                print(f"   Status: {response.status_code}")
                if response.status_code == 200:
                    print("   ✅ Main image works!")
                else:
                    print("   ❌ Main image failed")
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
            
            # Test gallery images
            print(f"\n🖼️  Testing gallery images:")
            working_gallery = []
            for i in range(1, 6):  # Test first 5 gallery images
                gallery_url = f"https://odoo.eboutiques.com/public/product_image/{product_id}/image_{i}"
                try:
                    response = requests.head(gallery_url, timeout=3)
                    if response.status_code == 200:
                        working_gallery.append(i)
                        print(f"   ✅ image_{i} works!")
                    else:
                        print(f"   ❌ image_{i}: {response.status_code}")
                except Exception as e:
                    print(f"   ❌ image_{i}: {str(e)}")
            
            print(f"\n📊 Summary for product {product_id}:")
            print(f"   Working gallery images: {working_gallery}")
            
            # 4. Force sync gallery for this product
            if working_gallery:
                print(f"\n🔄 Force syncing gallery...")
                try:
                    gallery_count = helper.sync_product_gallery(product_id, laravel_id)
                    print(f"✅ Synced {gallery_count} gallery images")
                except Exception as e:
                    print(f"❌ Gallery sync failed: {str(e)}")
        
        print(f"\n🎉 Testing completed!")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

def force_sync_specific_product(product_id=693):
    """Force sync a specific product to test the sync process"""
    print(f"\n🚀 Force syncing product {product_id}")
    print("="*40)
    
    try:
        connector = OdooConnector()
        sql_connector = SQLConnector()
        helper = ProductHelper(connector, sql_connector)
        
        # Get product from Odoo
        products = connector.read('product.template', [product_id], [
            'id', 'name', 'list_price', 'qty_available', 'image_1920'
        ])
        
        if not products:
            print(f"❌ Product {product_id} not found in Odoo")
            return
        
        product = products[0]
        print(f"📦 Product: {product['name']}")
        
        # Get variants
        variant_ids = connector.search('product.product', [('product_tmpl_id', '=', product_id)])
        variants = []
        if variant_ids:
            variants = connector.read('product.product', variant_ids, [
                'id', 'display_name', 'default_code', 'qty_available', 'lst_price', 'standard_price'
            ])
        
        print(f"🔧 Found {len(variants)} variants")
        
        # Force sync
        result = helper.upsert_product_template(product, variants, [])
        
        if result:
            print(f"✅ Force sync successful!")
        else:
            print(f"❌ Force sync failed")
            
    except Exception as e:
        print(f"❌ Force sync failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_and_sync_working_products()
    
    # Also test force sync
    force_sync_specific_product(693)