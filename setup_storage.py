
import os

def setup_image_storage():
    """Create necessary directories for image storage in Laravel"""
    
    # Use environment variable or default path for Laravel storage
    laravel_base_path = os.environ.get('LARAVEL_PATH', '/var/www/html/OrpitOps/Eboutiques/eboutiques_backend')
    
    directories = [
        os.path.join(laravel_base_path, "storage", "app", "public"),
        os.path.join(laravel_base_path, "storage", "app", "public", "products"),
        os.path.join(laravel_base_path, "storage", "app", "public", "variants"),
        os.path.join(laravel_base_path, "storage", "app", "public", "website_images")
    ]
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"✅ Created/verified directory: {directory}")
        except Exception as e:
            print(f"❌ Failed to create directory {directory}: {e}")
    
    # Create a .gitkeep file to keep directories in git
    for subdir in ["products", "variants"]:
        directory = os.path.join(laravel_base_path, "storage", "app", "public", subdir)
        gitkeep_path = os.path.join(directory, ".gitkeep")
        try:
            with open(gitkeep_path, 'w') as f:
                f.write("# Keep this directory in git\n")
            print(f"✅ Created .gitkeep in {directory}")
        except Exception as e:
            print(f"❌ Failed to create .gitkeep in {directory}: {e}")

if __name__ == "__main__":
    setup_image_storage()
