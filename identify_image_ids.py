#!/usr/bin/env python3
"""
Test to identify what the working image IDs (693, 701) represent in Odoo
"""
import sys
import os
sys.path.insert(0, 'helpers/')

from helpers.odoo_connector import OdooConnector

def identify_image_ids():
    """Check what type of IDs 693 and 701 are in Odoo"""
    
    connector = OdooConnector()
    test_ids = [693, 701]
    
    print("🔍 Identifying image ID types...")
    print("="*60)
    
    for test_id in test_ids:
        print(f"\n🆔 Testing ID: {test_id}")
        print("-" * 30)
        
        # Check if it's a product.template ID
        try:
            template_data = connector.read('product.template', [test_id], ['id', 'name', 'image_1920'])
            if template_data:
                template = template_data[0]
                print(f"✅ PRODUCT TEMPLATE FOUND:")
                print(f"   ID: {template['id']}")
                print(f"   Name: {template['name']}")
                print(f"   Has image_1920: {bool(template.get('image_1920'))}")
                
                # Check for gallery images (image_1, image_2, etc.)
                gallery_fields = ['image_1', 'image_2', 'image_3', 'image_4', 'image_5']
                template_gallery = connector.read('product.template', [test_id], gallery_fields)
                if template_gallery:
                    gallery = template_gallery[0]
                    gallery_count = sum(1 for field in gallery_fields if gallery.get(field))
                    print(f"   Gallery images: {gallery_count}")
                    for field in gallery_fields:
                        if gallery.get(field):
                            print(f"     - {field}: ✅")
            else:
                print("❌ NOT a product.template ID")
        except Exception as e:
            print(f"❌ Error checking product.template: {str(e)}")
        
        # Check if it's a product.product ID  
        try:
            product_data = connector.read('product.product', [test_id], ['id', 'name', 'display_name', 'image_1920', 'product_tmpl_id'])
            if product_data:
                product = product_data[0]
                print(f"✅ PRODUCT VARIANT FOUND:")
                print(f"   ID: {product['id']}")
                print(f"   Name: {product['display_name']}")
                print(f"   Template ID: {product['product_tmpl_id']}")
                print(f"   Has image_1920: {bool(product.get('image_1920'))}")
            else:
                print("❌ NOT a product.product ID")
        except Exception as e:
            print(f"❌ Error checking product.product: {str(e)}")
    
    print(f"\n🎯 CONCLUSION:")
    print("Based on the results above, the working image IDs are:")
    print("- If PRODUCT TEMPLATE was found: Use product.template IDs")
    print("- If PRODUCT VARIANT was found: Use product.product IDs")
    print("\nThis will help us fix the syncer to use the correct ID type.")

def check_syncer_id_usage():
    """Check what IDs the syncer is currently using"""
    
    print(f"\n🔧 CURRENT SYNCER ID USAGE:")
    print("="*60)
    
    # Show what the syncer currently does
    print("Current syncer behavior:")
    print("1. Main product images: Uses product.template ID (p['id'])")
    print("2. Gallery images: Uses product.template ID (product_template_id)")  
    print("3. Variant images: Uses product.product ID (v['id'])")
    
    print(f"\nIf the working URLs use product.template IDs:")
    print("✅ Main images: CORRECT")
    print("✅ Gallery images: CORRECT") 
    print("❓ Variant images: MIGHT BE WRONG (should use template ID too?)")
    
    print(f"\nIf the working URLs use product.product IDs:")
    print("❌ Main images: WRONG")
    print("❌ Gallery images: WRONG")
    print("✅ Variant images: CORRECT")

if __name__ == "__main__":
    try:
        identify_image_ids()
        check_syncer_id_usage()
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()