#!/usr/bin/env python3
"""
Find the correct 'onHand' quantity field in Odoo by testing available fields
"""
import sys
sys.path.insert(0, 'helpers/')

from helpers.odoo_connector import OdooConnector

def find_onhand_field():
    """Find the field that corresponds to 'onHand' in Odoo"""
    print("🔍 Finding the correct 'onHand' quantity field in Odoo")
    print("="*60)
    
    try:
        connector = OdooConnector()
        
        # Get all available fields for product.template
        print("📋 Getting all fields for product.template...")
        fields = connector.get_model_fields('product.template')
        
        # Find quantity-related fields
        qty_related_fields = []
        search_terms = ['qty', 'quantity', 'stock', 'hand', 'available', 'on_hand', 'inventory']
        
        for field_name, field_info in fields.items():
            field_desc = field_info.get('string', '').lower()
            if any(term in field_name.lower() or term in field_desc for term in search_terms):
                qty_related_fields.append({
                    'name': field_name,
                    'description': field_info.get('string', 'No description'),
                    'type': field_info.get('type', 'unknown')
                })
        
        print(f"\n📊 Found {len(qty_related_fields)} quantity-related fields:")
        for field in qty_related_fields:
            print(f"  - {field['name']}: {field['description']} ({field['type']})")
        
        # Test with a specific product to see actual values
        print(f"\n🧪 Testing with a real product...")
        
        # Get first product
        product_ids = connector.search('product.template', [], limit=1)
        if not product_ids:
            print("❌ No products found to test")
            return
        
        product_id = product_ids[0]
        
        # Get product name
        product_info = connector.read('product.template', [product_id], ['name'])
        product_name = product_info[0]['name'] if product_info else f"Product {product_id}"
        
        print(f"\n🔍 Testing with product: {product_name} (ID: {product_id})")
        print("-" * 50)
        
        # Test each quantity field that exists
        working_fields = []
        
        for field in qty_related_fields:
            field_name = field['name']
            try:
                result = connector.read('product.template', [product_id], [field_name])
                if result and field_name in result[0]:
                    value = result[0][field_name]
                    working_fields.append({
                        'name': field_name,
                        'value': value,
                        'description': field['description']
                    })
                    print(f"✅ {field_name}: {value} ({field['description']})")
                else:
                    print(f"❌ {field_name}: No data")
            except Exception as e:
                print(f"❌ {field_name}: Error - {str(e)}")
        
        # Also check product.product (variants) for comparison
        print(f"\n🔧 Checking product.product (variants) for comparison...")
        variant_ids = connector.search('product.product', [('product_tmpl_id', '=', product_id)], limit=1)
        
        if variant_ids:
            variant_id = variant_ids[0]
            
            print(f"Testing variant ID: {variant_id}")
            print("-" * 30)
            
            for field in qty_related_fields:
                field_name = field['name']
                try:
                    result = connector.read('product.product', [variant_id], [field_name])
                    if result and field_name in result[0]:
                        value = result[0][field_name]
                        print(f"✅ {field_name}: {value} (variant)")
                    else:
                        print(f"❌ {field_name}: No data (variant)")
                except Exception as e:
                    print(f"❌ {field_name}: Error (variant) - {str(e)}")
        
        # Provide recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        print("="*50)
        
        if working_fields:
            print("Working quantity fields found:")
            for field in working_fields:
                print(f"  - {field['name']}: {field['value']} ({field['description']})")
            
            print(f"\n🎯 Most likely candidates for 'onHand':")
            candidates = []
            for field in working_fields:
                if any(term in field['name'].lower() for term in ['hand', 'available', 'qty']):
                    if 'virtual' not in field['name'].lower() and 'forecast' not in field['description'].lower():
                        candidates.append(field)
            
            if candidates:
                for candidate in candidates:
                    print(f"  ✅ {candidate['name']}: {candidate['value']} - {candidate['description']}")
            else:
                print("  ⚠️  No clear candidates found. Use 'qty_available' as fallback.")
        else:
            print("❌ No working quantity fields found!")
        
        print(f"\n📝 NEXT STEPS:")
        print("1. Check which field value matches what you see as 'onHand' in Odoo admin")
        print("2. Update the syncer to use the correct field")
        print("3. If unsure, 'qty_available' is usually the safe default")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_onhand_field()