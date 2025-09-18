import boto3
import pandas as pd
import json
from pathlib import Path

# Configuration
EXCEL_FILE = 'classifications.xlsx'
BUCKET_NAME = 'damage-analyzer-images'
S3_IMAGE_PREFIX = 'car-damage/'
MANIFEST_KEY = 'manifests/car-damage-manifest.jsonl'

# Initialize clients
s3 = boto3.client('s3')

# Load Excel data
df = pd.read_excel(EXCEL_FILE)

# Get images from S3
s3_response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_IMAGE_PREFIX)
s3_images = {obj['Key'].split('/')[-1] for obj in s3_response['Contents']}

# Create AWS Custom Labels manifest
manifest_lines = []
for _, row in df.iterrows():
    image_name = row['image']
    damage_class = row['classes']
    
    # Find matching image in S3
    image_file = None
    for ext in ['.jpg', '.jpeg', '.png']:
        test_name = f"{Path(image_name).stem}{ext}"
        if test_name in s3_images:
            image_file = test_name
            break
    
    if image_file:
        # AWS Custom Labels format
        manifest_entry = {
            "source-ref": f"s3://{BUCKET_NAME}/{S3_IMAGE_PREFIX}{image_file}",
            "class": damage_class,
            "class-metadata": {
                "confidence": 1,
                "job-name": "labeling-job",
                "class-name": damage_class,
                "human-annotated": "yes",
                "creation-date": "2021-01-01T00:00:00.000Z"
            }
        }
        manifest_lines.append(json.dumps(manifest_entry))

# Upload manifest to S3
manifest_content = '\n'.join(manifest_lines)
s3.put_object(
    Bucket=BUCKET_NAME,
    Key=MANIFEST_KEY,
    Body=manifest_content
)

print(f"Manifest created: s3://{BUCKET_NAME}/{MANIFEST_KEY}")
print(f"Images in manifest: {len(manifest_lines)}")
