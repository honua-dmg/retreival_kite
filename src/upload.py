import os
import boto3
from dotenv import load_dotenv
import datetime as dt

class Upload():
    def __init__(self,local_dir):
        load_dotenv()
        # Load from .env
        self.key = os.getenv("DIGITALOCEAN_KEY_ID")
        self.secret = os.getenv("DIGITALOCEAN_KEY_SECRET")
        self.region = os.getenv("DIGITALOCEAN_REGION")
        self.endpoint = os.getenv("DIGITALOCEAN_ENDPOINT")
        self.bucket = os.getenv("DIGITALOCEAN_BUCKET_NAME")
        self.local_dir = local_dir  # Your mounted folder

        # Initialize S3 client
        session = boto3.session.Session()
        self.client = session.client(
            's3',
            region_name=self.region,
            endpoint_url=self.endpoint,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret
        )
        self.bucket = 'kite'
    def upload(self):
        """
        Uploads the latest CSV files to S3. (digitalocean spaces)
        
        This method uploads the latest CSV files to S3. It checks for files in the local directory
        that match the current date and uploads them to S3.
        """
        print("Uploading files to S3...")
        date=dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"%Y-%m-%d")
        #date= '2025-06-06'
        for root, dirs, files in os.walk(self.local_dir):
            for filename in files:
                if filename.split('.')[1] != 'csv':
                    continue
                if filename.split('.')[0] != date:
                    print(f"Skipping {filename} as it is not from today.")
                    continue
                filepath = os.path.join(root, filename)
                s3_path = os.path.relpath(filepath, self.local_dir).replace("\\", "/")
                print(f"Uploading {filepath} → s3://{self.bucket}/{s3_path}")
                self.client.upload_file(filepath, self.bucket, s3_path)
        print("Files uploaded successfully.")
    def delete_old(self):
        """
        Deletes old CSV files from the local directory.
        
        This method deletes old CSV files from the local directory that are older than 7 days.
        """
        print("Deleting old files...")
        #cutoff_date = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5)-dt.timedelta(days=7)
        current_date = dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes=30)))
        cutoff_date = current_date - dt.timedelta(days=7)
        for root, dirs, files in os.walk(self.local_dir):
            for filename in files:
                if filename.split('.')[1] != 'csv':
                    continue
                if dt.datetime.strptime(filename.split('.')[0], "%Y-%m-%d").replace(tzinfo=dt.timezone(dt.timedelta(hours=5,minutes=30))) < cutoff_date:
                    os.remove(os.path.join(root, filename))
        print("Old files deleted successfully.")


def cloud_handler():
    
    upload = Upload("/app/data")
    upload.upload()
    upload.delete_old()
    
def local_handler(path):
    upload = Upload(path)
    upload.upload()
    
    