import os
import boto3
from dotenv import load_dotenv

load_dotenv()

# Load from .env
key = os.getenv("DO_SPACES_KEY")
secret = os.getenv("DO_SPACES_SECRET")
region = os.getenv("DO_SPACES_REGION")
endpoint = os.getenv("DO_SPACES_ENDPOINT")
bucket = os.getenv("DO_SPACES_BUCKET")
local_dir = "data"  # Your mounted folder

# Initialize S3 client
session = boto3.session.Session()
client = session.client(
    's3',
    region_name=region,
    endpoint_url=endpoint,
    aws_access_key_id=key,
    aws_secret_access_key=secret
)