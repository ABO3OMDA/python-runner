#!/usr/bin/env python3
"""
Test to find the correct quantity field in Odoo (onHand vs qty_available)
"""
import sys
sys.path.insert(0, 'helpers/')

from helpers.odoo_connector import OdooConnector

def test_quantity_fields():
    """Test different quantity fields to find the correct one"""
    print("🔍 Testing Odoo quantity fields")
    print("="*60)
    
    try:
        connector = OdooConnector()
        
        # Get available fields for product.template
        print("📋 Getting available fields for product.template...")
        fields = connector.get_model_fields('product.template')
        
        # Look for quantity-related fields
        qty_fields = []
        for field_name, field_info in fields.items():
            if 'qty' in field_name.lower() or 'quantity' in field_name.lower() or 'stock' in field_name.lower() or 'hand' in field_name.lower():
                qty_fields.append((field_name, field_info.get('string', 'No description')))
        
        print(f"\n📊 Found {len(qty_fields)} quantity-related fields:")
        for field_name, description in qty_fields:
            print(f"  - {field_name}: {description}")
        
        # Test with a specific product
        print(f"\n🧪 Testing with specific products...")
        
        # Get a few products to test
        product_ids = connector.search('product.template', [], limit=3)
        
        if not product_ids:
            print("❌ No products found")
            return
        
        # Test different quantity fields
        test_fields = [
            'qty_available',    # Currently used
            'qty_on_hand',      # Likely the correct "onHand"
            'virtual_available', # Forecasted quantity
            'free_qty',         # Free quantity
            'qty_at_date',      # Quantity at date
        ]
        
        for product_id in product_ids[:2]:  # Test first 2 products
            print(f"\n🔍 Testing product ID {product_id}:")
            
            try:
                # Get product name first
                product_info = connector.read('product.template', [product_id], ['name'])
                product_name = product_info[0]['name'] if product_info else f"Product {product_id}"
                print(f"   Name: {product_name}")
                
                # Test each quantity field
                for field in test_fields:
                    try:
                        result = connector.read('product.template', [product_id], [field])
                        if result:
                            value = result[0].get(field, 'N/A')
                            print(f"   {field}: {value}")
                        else:
                            print(f"   {field}: No data")
                    except Exception as e:
                        print(f"   {field}: Error - {str(e)}")
                        
            except Exception as e:
                print(f"   ❌ Error testing product {product_id}: {str(e)}")
        
        # Also test product.product (variants)
        print(f"\n🔧 Testing product.product (variants)...")
        variant_ids = connector.search('product.product', [], limit=2)
        
        for variant_id in variant_ids[:1]:  # Test first variant
            print(f"\n🔍 Testing variant ID {variant_id}:")
            
            try:
                variant_info = connector.read('product.product', [variant_id], ['display_name'])
                variant_name = variant_info[0]['display_name'] if variant_info else f"Variant {variant_id}"
                print(f"   Name: {variant_name}")
                
                for field in test_fields:
                    try:
                        result = connector.read('product.product', [variant_id], [field])
                        if result:
                            value = result[0].get(field, 'N/A')
                            print(f"   {field}: {value}")
                        else:
                            print(f"   {field}: No data")
                    except Exception as e:
                        print(f"   {field}: Error - {str(e)}")
                        
            except Exception as e:
                print(f"   ❌ Error testing variant {variant_id}: {str(e)}")
        
        print(f"\n💡 RECOMMENDATION:")
        print(f"Based on the results above:")
        print(f"- If 'qty_on_hand' shows the correct physical stock → Use qty_on_hand")
        print(f"- If 'qty_available' matches what you see in Odoo → Keep qty_available")  
        print(f"- If 'virtual_available' includes future stock → Avoid this")
        print(f"- Check which field matches the 'onHand' value you see in Odoo admin")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_quantity_fields()