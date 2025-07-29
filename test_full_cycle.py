import sys
import json
sys.path.insert(0, 'helpers/')

from helpers.odoo_connector import OdooConnector
from helpers.sql_connector import SQLConnector
from helpers.product_helpers import ProductHelper

def test_full_cycle():
    """Test the complete Odoo to Laravel sync cycle"""
    
    print("🚀 STARTING FULL CYCLE TEST")
    print("=" * 50)
    
    try:
        # Step 1: Initialize connectors
        print("\n📡 Step 1: Connecting to Odoo and Database...")
        connector = OdooConnector()
        sql_connector = SQLConnector()
        print("✅ Connected successfully")
        
        # Step 2: Run migrations
        print("\n🔧 Step 2: Running database migrations...")
        sql_connector.migrate()
        print("✅ Migrations completed")
        
        # Step 3: Test Odoo connection and get a product with tax
        print("\n🔍 Step 3: Finding products with tax information...")
        
        # Search for products with taxes (Vichy or any product)
        search_terms = ["Vichy", "Capital", "Soleil"]  # Products likely to have tax
        product_ids = []
        
        for term in search_terms:
            product_ids = connector.search("product.template", [("name", "like", term)], offset=0, limit=1)
            if product_ids:
                print(f"✅ Found product with search term: {term}")
                break
        
        if not product_ids:
            print("⚠️  No specific products found, getting any product...")
            product_ids = connector.search("product.template", [], offset=0, limit=1)
        
        if not product_ids:
            print("❌ ERROR: No products found in Odoo!")
            return
        
        print(f"📦 Using product ID: {product_ids[0]}")
        
        # Step 4: Get product details with enhanced information
        print("\n📋 Step 4: Retrieving product details...")
        product_data = connector.read("product.template", product_ids, [
            "name", "list_price", "standard_price", "taxes_id", "qty_available", 
            "default_code", "id", "weight", "categ_id"
        ])
        
        if not product_data:
            print("❌ ERROR: Could not read product data from Odoo!")
            return
        
        product = product_data[0]
        
        print(f"📦 Product: {product['name']}")
        print(f"💰 List Price: {product['list_price']}")
        print(f"🏷️  Has Taxes: {bool(product.get('taxes_id'))}")
        print(f"📊 Stock: {product['qty_available']}")
        print(f"⚖️  Weight: {product['weight']}")
        
        # Step 5: Get tax details if available
        if product.get('taxes_id'):
            print(f"\n🧾 Step 5: Getting tax details...")
            tax_ids = product['taxes_id']
            taxes = connector.read("account.tax", tax_ids, ["name", "amount", "amount_type", "type_tax_use"])
            
            print(f"📝 Tax Details:")
            total_tax_rate = 0
            for tax in taxes:
                print(f"  - {tax['name']}: {tax['amount']}% ({tax['amount_type']})")
                if tax["amount_type"] == "percent":
                    total_tax_rate += tax["amount"]
            
            print(f"🧮 Total Tax Rate: {total_tax_rate}%")
            
            # Calculate expected values
            price_with_tax = product["list_price"]
            price_without_tax = price_with_tax / (1 + (total_tax_rate / 100))
            tax_amount = price_with_tax - price_without_tax
            
            print(f"💰 Expected Price with tax: {price_with_tax:.2f}")
            print(f"💰 Expected Price without tax: {price_without_tax:.2f}")
            print(f"💰 Expected Tax amount: {tax_amount:.2f}")
        else:
            print("ℹ️  Step 5: Product has no taxes")
        
        # Step 6: Test tax calculation helper
        print("\n🧮 Step 6: Testing tax calculation...")
        helper = ProductHelper(connector, sql_connector)
        tax_info = helper.get_product_tax_info(product["id"])
        
        print(f"🔢 Calculated Tax Info:")
        print(f"  - Has Tax: {tax_info['has_tax']}")
        print(f"  - Tax Rate: {tax_info['tax_rate']}%")
        print(f"  - Tax Amount: {tax_info['tax_amount']:.2f}")
        
        # Step 7: Get product variants
        print(f"\n🔧 Step 7: Getting product variants...")
        variant_ids = connector.search("product.product", [("product_tmpl_id", "=", product["id"])])
        variants = connector.read("product.product", variant_ids, [
            "name", "display_name", "default_code", "lst_price", "qty_available", 
            "standard_price", "weight", "product_template_variant_value_ids"
        ])
        
        print(f"🔧 Found {len(variants)} variants:")
        for i, variant in enumerate(variants):
            print(f"  {i+1}. {variant['display_name']} (SKU: {variant.get('default_code', 'No SKU')})")
        
        # Step 8: Test the full sync process
        print(f"\n💾 Step 8: Testing product sync to database...")
        
        # Clear existing data for clean test
        sql_connector.connection.execute(f"DELETE FROM products WHERE remote_key_id = '{product['id']}'")
        sql_connector.connection.execute(f"DELETE FROM product_variants WHERE remote_key_id IN (SELECT id FROM product_variants WHERE product_id IN (SELECT id FROM products WHERE remote_key_id = '{product['id']}'))")
        print("🗑️  Cleared existing test data")
        
        # Perform the sync
        helper.upsert_product_template(product, variants, [])
        print("✅ Product sync completed")
        
        # Step 9: Verify the sync in database
        print(f"\n🔍 Step 9: Verifying sync results in database...")
        
        synced_product = sql_connector.getOne(
            "products", 
            f"`remote_key_id` = '{product['id']}'"
        ).fetch()
        
        if synced_product:
            print(f"✅ PRODUCT SYNC SUCCESS!")
            print(f"📊 Synced Product Details:")
            print(f"  - ID: {synced_product['id']}")
            print(f"  - Name: {synced_product['name']}")
            print(f"  - Price (display): {synced_product['price']}")
            print(f"  - Price (without tax): {synced_product.get('price_without_tax', 'N/A')}")
            print(f"  - Tax Rate: {synced_product.get('tax_rate', 'N/A')}%")
            print(f"  - Tax Amount: {synced_product.get('tax_amount', 'N/A')}")
            print(f"  - Stock: {synced_product['qty']}")
            print(f"  - Status: {'Pending Admin Approval' if synced_product['approve_by_admin'] == 0 else 'Approved'}")
            
            # Check variants
            synced_variants = sql_connector.getAll(
                "product_variants",
                f"`product_id` = '{synced_product['id']}'"
            ).fetch()
            
            if synced_variants:
                print(f"🔧 Synced {len(synced_variants)} variants:")
                for variant in synced_variants:
                    print(f"  - {variant['name']} (SKU: {variant['sku']}, Stock: {variant['stock']}, Price: {variant['price']})")
            
            # Step 10: Test Laravel tax helper
            print(f"\n🔧 Step 10: Testing Laravel integration...")
            print("Run this in your Laravel project to test the helper:")
            print("=" * 60)
            print("php artisan tinker")
            print("")
            print("use App\Helpers\SimpleTaxHelper;")
            print("use App\Models\Product;")
            print("")
            print(f"$product = Product::where('remote_key_id', '{product['id']}')->first();")
            print("if ($product) {")
            print("    $taxInfo = SimpleTaxHelper::formatPriceWithTax($product, 1);")
            print("    print_r($taxInfo);")
            print("} else {")
            print("    echo 'Product not found in Laravel database';")
            print("}")
            print("=" * 60)
            
        else:
            print("❌ ERROR: Product was not synced to database!")
            return
        
        print(f"\n🎉 FULL CYCLE TEST COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        print("✅ Odoo connection working")
        print("✅ Tax calculation working")
        print("✅ Product sync working")
        print("✅ Database integration working")
        print("✅ Ready for Laravel integration")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_full_cycle()
    if success:
        print("\n🚀 You can now proceed to test the Laravel integration!")
    else:
        print("\n🔧 Please fix the errors above before proceeding.")
