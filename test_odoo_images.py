#!/usr/bin/env python3
"""
Test script to find the correct Odoo image URL structure
"""
import requests
import os
from helpers.odoo_connector import OdooConnector

def test_odoo_image_urls():
    """Test various Odoo image URL patterns to find the correct one"""
    
    connector = OdooConnector()
    
    # Get a few products to test with
    print("🔍 Getting sample products from Odoo...")
    
    try:
        # Get first 3 products with images
        products = connector.search_read(
            'product.template', 
            [('image_1920', '!=', False)], 
            ['id', 'name', 'image_1920'], 
            limit=3
        )
        
        if not products:
            print("❌ No products with images found")
            return
            
        print(f"✅ Found {len(products)} products with images")
        
        base_url = os.getenv("ODOO_URL", "http://161.35.26.174:8069")
        
        # Test various URL patterns
        url_patterns = [
            # Standard Odoo patterns
            "{base}/web/image/product.template/{id}/image_1920",
            "{base}/web/image/product.template/{id}/image_1024", 
            "{base}/web/image/product.template/{id}/image_512",
            "{base}/web/image/product.template/{id}/image_256",
            
            # Public access patterns
            "{base}/web/image?model=product.template&id={id}&field=image_1920",
            "{base}/web/image?model=product.template&id={id}&field=image_1024",
            
            # Website patterns
            "{base}/web/image/product.template/{id}/image_1920/product-image",
            
            # Custom patterns that might exist
            "{base}/public/product_image/{id}/image_1920",
            "{base}/api/image/product/{id}",
        ]
        
        for product in products:
            print(f"\n{'='*60}")
            print(f"🧪 Testing product: {product['name']} (ID: {product['id']})")
            
            for pattern in url_patterns:
                url = pattern.format(base=base_url, id=product['id'])
                print(f"\n🔗 Testing: {url}")
                
                try:
                    # Test with HEAD request first (faster)
                    response = requests.head(url, timeout=10, allow_redirects=True)
                    
                    if response.status_code == 200:
                        print(f"✅ SUCCESS! Status: {response.status_code}")
                        print(f"   Content-Type: {response.headers.get('content-type', 'unknown')}")
                        print(f"   Content-Length: {response.headers.get('content-length', 'unknown')}")
                        
                        # This pattern works! Test with a few more products
                        print(f"🎉 FOUND WORKING PATTERN: {pattern}")
                        return pattern, base_url
                        
                    elif response.status_code == 404:
                        print(f"❌ Not found (404)")
                    elif response.status_code == 401:
                        print(f"🔒 Unauthorized (401) - might need authentication")
                    elif response.status_code == 403:
                        print(f"🚫 Forbidden (403)")
                    else:
                        print(f"⚠️  Status: {response.status_code}")
                        
                except requests.exceptions.Timeout:
                    print(f"⏰ Timeout")
                except requests.exceptions.ConnectionError:
                    print(f"🔌 Connection error")
                except Exception as e:
                    print(f"❌ Error: {str(e)}")
    
    except Exception as e:
        print(f"❌ Failed to test: {str(e)}")
        return None, None
    
    print("\n❌ No working image URL pattern found!")
    return None, None

def test_authenticated_access():
    """Test if we need authentication for image access"""
    print("\n🔐 Testing authenticated image access...")
    
    connector = OdooConnector()
    base_url = os.getenv("ODOO_URL", "http://161.35.26.174:8069")
    
    # Get a product
    products = connector.search_read(
        'product.template', 
        [('image_1920', '!=', False)], 
        ['id', 'name'], 
        limit=1
    )
    
    if not products:
        print("❌ No products found")
        return
    
    product = products[0]
    
    # Test with session cookies (if we can get them)
    session = requests.Session()
    
    # Try to authenticate
    auth_url = f"{base_url}/web/session/authenticate"
    auth_data = {
        'jsonrpc': '2.0',
        'method': 'call',
        'params': {
            'db': os.getenv("ODOO_DB"),
            'login': os.getenv("ODOO_USER"), 
            'password': os.getenv("ODOO_PASS")
        }
    }
    
    try:
        auth_response = session.post(auth_url, json=auth_data, timeout=10)
        print(f"🔐 Auth response: {auth_response.status_code}")
        
        if auth_response.status_code == 200:
            # Try image access with session
            image_url = f"{base_url}/web/image/product.template/{product['id']}/image_1920"
            img_response = session.head(image_url, timeout=10)
            
            print(f"🖼️  Image with session: {img_response.status_code}")
            
            if img_response.status_code == 200:
                print(f"✅ SUCCESS with authentication!")
                print(f"   Pattern: /web/image/product.template/{{id}}/image_1920")
                return f"{base_url}/web/image/product.template", True
    
    except Exception as e:
        print(f"❌ Authentication test failed: {str(e)}")
    
    return None, False

if __name__ == "__main__":
    print("🚀 Testing Odoo Image URL Patterns")
    print("="*60)
    
    # Test unauthenticated access first
    pattern, base_url = test_odoo_image_urls()
    
    if not pattern:
        # Test authenticated access
        auth_pattern, needs_auth = test_authenticated_access()
        
        if auth_pattern:
            print(f"\n🎉 Found working pattern with authentication:")
            print(f"   Base: {auth_pattern}")
            print(f"   Needs auth: {needs_auth}")
        else:
            print(f"\n❌ No working patterns found. Manual investigation needed.")
            
            print(f"\n💡 Suggestions:")
            print(f"   1. Check if Odoo website/ecommerce module is installed")
            print(f"   2. Verify product images exist in Odoo admin")
            print(f"   3. Check Odoo security settings")
            print(f"   4. Try accessing Odoo web interface manually")
    else:
        print(f"\n🎉 SUCCESS! Working pattern found:")
        print(f"   Pattern: {pattern}")
        print(f"   Base URL: {base_url}")