"""
Cloud upload module for the Stock Market Data Collection System.

This module handles uploading collected CSV data to DigitalOcean Spaces
(S3-compatible storage) and cleaning up old local files.

Classes:
    - Upload: Manages file uploads and local cleanup

Configuration:
    Requires the following environment variables:
    - DIGITALOCEAN_KEY_ID: Access key ID
    - DIGITALOCEAN_KEY_SECRET: Secret access key
    - DIGITALOCEAN_REGION: Region (e.g., 'nyc3')
    - DIGITALOCEAN_ENDPOINT: S3 endpoint URL
    - DIGITALOCEAN_BUCKET_NAME: Bucket/Space name
"""

import os
import datetime as dt
from typing import Optional

import boto3
from dotenv import load_dotenv

import config
from utils import get_ist_now, get_ist_date, IST


class Upload:
    """
    Manages file uploads to DigitalOcean Spaces and local cleanup.
    
    Attributes:
        local_dir (str): Local directory containing data files.
        bucket (str): S3 bucket/Space name.
        client: boto3 S3 client instance.
    """

    def __init__(self, local_dir: str):
        """
        Initialize the Upload manager.
        
        Args:
            local_dir: Local directory containing data files to upload.
        """
        load_dotenv(config.ENVLOC)
        
        self.local_dir = local_dir
        
        # Load configuration
        self.key = os.getenv("DIGITALOCEAN_KEY_ID")
        self.secret = os.getenv("DIGITALOCEAN_KEY_SECRET")
        self.region = os.getenv("DIGITALOCEAN_REGION")
        self.endpoint = os.getenv("DIGITALOCEAN_ENDPOINT")
        self.bucket = os.getenv("DIGITALOCEAN_BUCKET_NAME", "kite")
        
        # Initialize S3 client
        self.client = self._create_client()

    def _create_client(self) -> Optional[boto3.client]:
        """
        Create and return a boto3 S3 client.
        
        Returns:
            boto3 S3 client, or None if credentials are missing.
        """
        if not all([self.key, self.secret, self.endpoint]):
            print("⚠️ DigitalOcean credentials not configured.", flush=True)
            return None
        
        session = boto3.session.Session()
        return session.client(
            's3',
            region_name=self.region,
            endpoint_url=self.endpoint,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret
        )

    def upload(self):
        """
        Upload today's CSV files to S3.
        
        Walks through the local directory and uploads all CSV files
        with today's date in the filename.
        """
        if not self.client:
            print("❌ Cannot upload: S3 client not initialized.", flush=True)
            return
        
        print("📤 Uploading files to S3...", flush=True)
        
        today = get_ist_date()
        uploaded_count = 0
        
        for root, dirs, files in os.walk(self.local_dir):
            for filename in files:
                # Only upload CSV files
                if not filename.endswith('.csv'):
                    continue
                
                # Only upload today's files
                file_date = filename.replace('.csv', '')
                if file_date != today:
                    print(f"⏭️  Skipping {filename} (not today's date)", flush=True)
                    continue
                
                # Build paths
                local_path = os.path.join(root, filename)
                s3_path = os.path.relpath(local_path, self.local_dir).replace("\\", "/")

                # Avoid accidental bucket-name duplication in object key
                bucket_prefix = f"{self.bucket}/"
                if s3_path.startswith(bucket_prefix):
                    s3_path = s3_path[len(bucket_prefix):]

                try:
                    print(f"📤 Uploading {local_path} → s3://{self.bucket}/{s3_path}", flush=True)
                    self.client.upload_file(local_path, self.bucket, s3_path)
                    uploaded_count += 1
                except Exception as e:
                    print(f"❌ Failed to upload {filename}: {e}", flush=True)
        
        print(f"✅ Upload complete. {uploaded_count} files uploaded.", flush=True)

    def delete_old(self, retention_days: int = 7):
        """
        Delete local CSV files older than the retention period.
        
        Args:
            retention_days: Number of days to retain files. Default is 7.
        """
        print(f"🗑️  Deleting files older than {retention_days} days...", flush=True)
        
        cutoff_date = get_ist_now() - dt.timedelta(days=retention_days)
        deleted_count = 0
        
        for root, dirs, files in os.walk(self.local_dir):
            for filename in files:
                # Only process CSV files
                if not filename.endswith('.csv'):
                    continue
                
                try:
                    # Parse date from filename
                    file_date_str = filename.replace('.csv', '')
                    file_date = dt.datetime.strptime(file_date_str, "%Y-%m-%d")
                    file_date = file_date.replace(tzinfo=IST)
                    
                    # Delete if older than cutoff
                    if file_date < cutoff_date:
                        file_path = os.path.join(root, filename)
                        os.remove(file_path)
                        deleted_count += 1
                        print(f"🗑️  Deleted {file_path}", flush=True)
                        
                except ValueError:
                    # Filename doesn't match expected date format
                    continue
                except Exception as e:
                    print(f"❌ Failed to delete {filename}: {e}", flush=True)
        
        print(f"✅ Cleanup complete. {deleted_count} files deleted.", flush=True)


def cloud_upload():
    """
    Convenience function to upload data from the default path.
    """
    uploader = Upload(config.DATA_PATH)
    uploader.upload()
    uploader.delete_old()


if __name__ == "__main__":
    # Test upload
    cloud_upload()

