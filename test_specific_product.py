#!/usr/bin/env python3
"""
Test image URLs for a specific product (631) to debug the issue
"""
import requests
import sys
sys.path.insert(0, 'helpers/')

def test_product_images(product_id=631):
    """Test image URLs for specific product"""
    print(f"🧪 Testing image URLs for product {product_id}")
    print("="*60)
    
    # Test main image
    main_url = f"https://odoo.eboutiques.com/public/product_image/{product_id}/image_1920"
    print(f"\n📷 Testing main image:")
    print(f"URL: {main_url}")
    
    try:
        response = requests.head(main_url, timeout=10)
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
        if response.status_code == 200:
            print("✅ Main image URL works!")
        else:
            print("❌ Main image URL failed")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    # Test gallery images
    print(f"\n🖼️  Testing gallery images:")
    working_images = []
    
    for i in range(1, 11):
        gallery_url = f"https://odoo.eboutiques.com/public/product_image/{product_id}/image_{i}"
        print(f"\nImage {i}: {gallery_url}")
        
        try:
            response = requests.head(gallery_url, timeout=5)
            print(f"  Status: {response.status_code}")
            
            if response.status_code == 200:
                print(f"  ✅ Image {i} works!")
                working_images.append(i)
            elif response.status_code == 404:
                print(f"  ❌ Image {i} not found")
            else:
                print(f"  ⚠️  Image {i} returned: {response.status_code}")
                
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
    
    print(f"\n📊 SUMMARY:")
    print(f"Product ID: {product_id}")
    print(f"Working gallery images: {working_images}")
    print(f"Total working images: {len(working_images)}")
    
    if len(working_images) == 0:
        print(f"\n💡 TROUBLESHOOTING:")
        print(f"1. Check if product {product_id} has images in Odoo admin")
        print(f"2. Verify if https://odoo.eboutiques.com is accessible")
        print(f"3. Check if the public image endpoint is enabled in Odoo")
        print(f"4. Try accessing the URL manually in a browser")

if __name__ == "__main__":
    # Test the specific product from the logs
    test_product_images(631)
    
    # Also test the working examples you provided
    print(f"\n" + "="*60)
    print("🧪 Testing your working examples:")
    
    working_examples = [693, 701]
    for product_id in working_examples:
        print(f"\n🔍 Testing product {product_id}:")
        main_url = f"https://odoo.eboutiques.com/public/product_image/{product_id}/image_1920"
        gallery_url = f"https://odoo.eboutiques.com/public/product_image/{product_id}/image_1"
        
        try:
            main_response = requests.head(main_url, timeout=5)
            gallery_response = requests.head(gallery_url, timeout=5)
            
            print(f"  Main (image_1920): {main_response.status_code}")
            print(f"  Gallery (image_1): {gallery_response.status_code}")
            
            if main_response.status_code == 200 and gallery_response.status_code == 200:
                print(f"  ✅ Product {product_id} works as expected!")
            else:
                print(f"  ❌ Product {product_id} has issues")
                
        except Exception as e:
            print(f"  ❌ Error testing {product_id}: {str(e)}")