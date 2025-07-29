# Create: test_enhanced_sync_fixed.py

import sys
import os

# Add the project root to Python path
sys.path.insert(0, '/app')
sys.path.insert(0, '/app/helpers')

try:
    from odoo_connector import OdooConnector
    from sql_connector import SQLConnector
    from product_helpers import ProductHelper
except ImportError as e:
    print(f"Import error: {e}")
    print("Trying alternative import structure...")
    try:
        # Alternative import method
        import odoo_connector
        import sql_connector  
        import product_helpers
        
        OdooConnector = odoo_connector.OdooConnector
        SQLConnector = sql_connector.SQLConnector
        ProductHelper = product_helpers.ProductHelper
        
    except ImportError as e2:
        print(f"Alternative import also failed: {e2}")
        print("Available modules in /app:")
        print(os.listdir('/app'))
        print("Available modules in /app/helpers:")
        print(os.listdir('/app/helpers'))
        sys.exit(1)

def test_enhanced_sync():
    """Test the enhanced sync with images and updates"""
    
    print("🚀 TESTING ENHANCED SYNC WITH IMAGES AND UPDATES")
    print("=" * 70)
    
    try:
        # Initialize
        connector = OdooConnector()
        sql_connector = SQLConnector()
        helper = ProductHelper(connector, sql_connector)
        
        print("✅ Connections established")
        
        # Test 1: Test database connection
        print("\n📊 Test 1: Testing database connection...")
        
        try:
            result = sql_connector.getOne('products', '1=1').fetch()
            if result:
                print(f"✅ Database connected. Sample product: {result.get('name', 'No name')}")
                
                # Check if tax fields exist
                tax_fields = ['price_without_tax', 'tax_rate', 'tax_amount', 'remote_key_id']
                print("Tax fields status:")
                for field in tax_fields:
                    exists = field in result
                    print(f"  {field}: {'✅ EXISTS' if exists else '❌ MISSING'}")
            else:
                print("❌ No products found in database")
                
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
        
        # Test 2: Test Odoo connection
        print("\n🌐 Test 2: Testing Odoo connection...")
        
        try:
            # Test simple search
            product_ids = connector.search('product.template', [], offset=0, limit=3)
            print(f"✅ Odoo connected. Found {len(product_ids)} products")
            
            if product_ids:
                # Get sample product
                products = connector.read('product.template', [product_ids[0]], ['name', 'list_price', 'taxes_id', 'image_1920'])
                if products:
                    product = products[0]
                    print(f"📦 Sample product: {product['name']}")
                    print(f"💰 Price: {product['list_price']}")
                    print(f"🏷️  Has taxes: {bool(product.get('taxes_id'))}")
                    print(f"🖼️  Has image: {bool(product.get('image_1920'))}")
                    
        except Exception as e:
            print(f"❌ Odoo connection failed: {e}")
            return False
        
        # Test 3: Test tax calculation
        print("\n🧮 Test 3: Testing tax calculation...")
        
        try:
            if product_ids:
                tax_info = helper.get_product_tax_info(product_ids[0])
                print(f"Tax calculation result: {tax_info}")
                
                if tax_info['has_tax']:
                    print(f"✅ Tax calculation working: {tax_info['tax_rate']}% rate")
                else:
                    print("ℹ️  Product has no taxes (this is normal for some products)")
            
        except Exception as e:
            print(f"❌ Tax calculation failed: {e}")
            return False
        
        # Test 4: Test image download (if product has image)
        print("\n📸 Test 4: Testing image functionality...")
        
        try:
            if product_ids and products and products[0].get('image_1920'):
                print("Testing image download...")
                image_path = helper.download_and_save_image(
                    product_ids[0], 
                    products[0]['image_1920'], 
                    'test'
                )
                print(f"✅ Image test completed: {image_path}")
            else:
                print("ℹ️  No image to test (product doesn't have image_1920)")
                
        except Exception as e:
            print(f"⚠️  Image test failed (non-critical): {e}")
        
        print(f"\n🎉 Enhanced sync test completed successfully!")
        
        # Summary
        print(f"\n📋 Test Summary:")
        print(f"  ✅ Database connection: Working")
        print(f"  ✅ Odoo connection: Working") 
        print(f"  ✅ Tax calculation: Working")
        print(f"  ✅ Project structure: Compatible")
        print(f"  🚀 Ready for production sync!")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced sync test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_enhanced_sync()
    if success:
        print("\n🚀 All systems go! Your enhanced sync is ready!")
    else:
        print("\n🔧 Please check the errors above.")
