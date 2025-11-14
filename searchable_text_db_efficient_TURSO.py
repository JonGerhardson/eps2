    import os
    import glob
    import sys
    import libsql_client
    import boto3 # For uploading images to R2
    from typing import List, Tuple

    # --- R2/S3 Config (Set these) ---
    # Make sure these are set as environment variables for security
    R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL") # e.g., 'https://<account_id>.r2.cloudflarestorage.com'
    R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
    R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
    R2_PUBLIC_URL = os.environ.get("R2_PUBLIC_URL") # e.g., 'https://pub-<...>.r2.dev'

    # --- Turso Config ---
    # Set these as environment variables
    TURSO_DB_URL = os.environ.get("TURSO_DB_URL") # Your Turso URL
    TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN") # Your Turso token

    # --- Local Config ---
    TEXT_DIRECTORY = "/home/jon/Documents/Epstein dump nov 12/TEXT"
    IMAGE_BASE_DIRECTORY = "/home/jon/Documents/Epstein dump nov 12"

    class TextSearchDatabase:
        def __init__(self):
            if not TURSO_DB_URL or not TURSO_AUTH_TOKEN:
                raise ValueError("TURSO_DB_URL and TURSO_AUTH_TOKEN must be set as environment variables.")
            
            self.client = libsql_client.create_client(
                url=TURSO_DB_URL,
                auth_token=TURSO_AUTH_TOKEN
            )
            
            # Init S3/R2 client
            self.s3 = boto3.client(
                's3',
                endpoint_url=R2_ENDPOINT_URL,
                aws_access_key_id=R2_ACCESS_KEY_ID,
                aws_secret_access_key=R2_SECRET_ACCESS_KEY
            )
            print("Connected to Turso and R2.")

        def create_tables(self):
            """Create the database tables with full-text search support."""
            print("Creating tables if they don't exist...")
            
            # We must store the FULL content now, not just a sample.
            # We also add image_url.
            self.client.execute('''
                CREATE TABLE IF NOT EXISTS text_files (
                    id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    filepath TEXT NOT NULL UNIQUE, -- Make filepath unique
                    content TEXT NOT NULL, -- Store FULL content
                    image_url TEXT -- Store the public URL to the image
                )
            ''')

            # FTS5 table setup is the same and will work.
            self.client.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS text_files_fts
                USING fts5(id, content, filename, filepath, content='text_files', content_rowid='id')
            ''')

            # Triggers are also the same.
            self.client.execute_batch([
                '''
                CREATE TRIGGER IF NOT EXISTS text_files_ai AFTER INSERT ON text_files
                BEGIN
                    INSERT INTO text_files_fts(rowid, id, content, filename, filepath)
                    VALUES (new.id, new.id, new.content, new.filename, new.filepath);
                END;
                ''',
                '''
                CREATE TRIGGER IF NOT EXISTS text_files_ad AFTER DELETE ON text_files
                BEGIN
                    INSERT INTO text_files_fts(text_files_fts, rowid, id, content, filename, filepath)
                    VALUES('delete', old.id, old.id, old.content, old.filename, old.filepath);
                END;
                ''',
                '''
                CREATE TRIGGER IF NOT EXISTS text_files_au AFTER UPDATE ON text_files
                BEGIN
                    INSERT INTO text_files_fts(text_files_fts, rowid, id, content, filename, filepath)
                    VALUES('delete', old.id, old.id, old.content, old.filename, old.filepath);
                    INSERT INTO text_files_fts(rowid, id, content, filename, filepath)
                    VALUES (new.id, new.id, new.content, new.filename, new.filepath);
                END;
                '''
            ])
            print("Tables and triggers are ready.")

        def find_and_upload_image(self, txt_path):
            """Finds the corresponding image, uploads it to R2, and returns the public URL."""
            import re
            base_name = os.path.splitext(os.path.basename(txt_path))[0]
            image_extensions = ['.jpg', '.jpeg', '.JPG', '.JPEG', '.png', '.PNG']
            
            match = re.search(r'/TEXT/(\d{3})/', txt_path)
            if not match:
                return None
                
            sub_dir = match.group(1) # e.g., "001"
            
            for ext in image_extensions:
                image_path = os.path.join(IMAGE_BASE_DIRECTORY, sub_dir, f"{base_name}{ext}")
                if os.path.exists(image_path):
                    try:
                        # Upload the file
                        # Use a clean object key, e.g., "001/image_name.jpg"
                        object_key = f"{sub_dir}/{base_name}{ext}"
                        
                        self.s3.upload_file(
                            image_path,
                            R2_BUCKET_NAME,
                            object_key,
                            ExtraArgs={'ContentType': f'image/{ext.lstrip(".").lower()}'}
                        )
                        
                        # Return the public URL
                        public_url = f"{R2_PUBLIC_URL}/{object_key}"
                        print(f"  > Uploaded image to {public_url}")
                        return public_url
                    except Exception as e:
                        print(f"  > Error uploading {image_path}: {e}")
                        return None
            return None

        def index_text_files(self, text_directory: str, batch_size: int = 20):
            """Index all text files and upload corresponding images."""
            print(f"Indexing text files from {text_directory}...")
            txt_files = glob.glob(os.path.join(text_directory, "**", "*.txt"), recursive=True)
            total_files = len(txt_files)
            print(f"Found {total_files} text files to index...")

            for i, file_path in enumerate(txt_files):
                print(f"Processing {i+1}/{total_files}: {file_path}")
                try:
                    # 1. Read FULL content
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        full_content = f.read()
                    
                    filename = os.path.basename(file_path)

                    # 2. Find and upload the corresponding image
                    image_url = self.find_and_upload_image(file_path)
                    
                    # 3. Insert full content and image URL into Turso
                    # Use INSERT OR IGNORE to skip duplicates if re-running
                    self.client.execute(
                        "INSERT OR IGNORE INTO text_files (filename, filepath, content, image_url) VALUES (?, ?, ?, ?)",
                        (filename, file_path, full_content, image_url)
                    )

                except Exception as e:
                    print(f"  > Error processing {file_path}: {str(e)}")
                    continue
            
            print(f"Indexing complete! Processed {total_files} files.")

        def close(self):
            if self.client:
                self.client.close()

    def main():
        # Make sure you have set your environment variables!
        # export TURSO_DB_URL="..."
        # export TURSO_AUTH_TOKEN="..."
        # export R2_ENDPOINT_URL="..."
        # ...etc.
        
        if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, R2_PUBLIC_URL, TURSO_DB_URL, TURSO_AUTH_TOKEN]):
            print("Error: Missing one or more environment variables.")
            print("Please set: TURSO_DB_URL, TURSO_AUTH_TOKEN, R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, R2_PUBLIC_URL")
            sys.exit(1)

        db = TextSearchDatabase()
        db.create_tables()
        db.index_text_files(TEXT_DIRECTORY)
        db.close()
        print("Data migration to Turso is complete.")

    if __name__ == "__main__":
        main()
