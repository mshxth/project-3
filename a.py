import boto3
import pandas as pd
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Configuration
EXCEL_FILE = 'classifications.xlsx'  # Your Excel file path
IMAGE_DIR = 'images/'                # Your images folder path
BUCKET_NAME = 'your-s3-bucket'       # Your S3 bucket
PROJECT_NAME = 'car-damage-detection'
REGION = 'us-east-1'

# Initialize AWS clients
s3 = boto3.client('s3', region_name=REGION)
rekognition = boto3.client('rekognition', region_name=REGION)

def upload_image(image_path):
    """Upload single image to S3"""
    try:
        key = f"car-damage/{Path(image_path).name}"
        s3.upload_file(str(image_path), BUCKET_NAME, key)
        return {'success': True, 'key': key, 'path': image_path}
    except Exception as e:
        return {'success': False, 'error': str(e), 'path': image_path}

# Load classifications from Excel
print("Loading Excel file...")
df = pd.read_excel(EXCEL_FILE)
print(f"Found {len(df)} classifications")

# Match images with classifications
print("Matching images...")
image_data = []
image_dir = Path(IMAGE_DIR)

for _, row in df.iterrows():
    image_name = row['image']
    damage_class = row['classes']
    
    # Find image file (try different extensions)
    image_path = None
    for ext in ['.jpg', '.jpeg', '.png']:
        potential_path = image_dir / f"{Path(image_name).stem}{ext}"
        if potential_path.exists():
            image_path = potential_path
            break
    
    if image_path:
        image_data.append({
            'path': str(image_path),
            'class': damage_class,
            'name': image_path.name
        })

print(f"Found {len(image_data)} matching images")

# Upload images to S3
print("Uploading to S3...")
image_paths = [item['path'] for item in image_data]

with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(tqdm(
        executor.map(upload_image, image_paths),
        total=len(image_paths),
        desc="Uploading"
    ))

successful_uploads = [r for r in results if r['success']]
print(f"Uploaded {len(successful_uploads)}/{len(image_paths)} images")

# Create manifest file for Custom Labels
print("Creating manifest...")
manifest_lines = []

for item in image_data:
    image_name = item['name']
    damage_class = item['class']
    
    # Check if this image was successfully uploaded
    uploaded = any(r['key'].endswith(image_name) for r in successful_uploads if r['success'])
    if not uploaded:
        continue
    
    manifest_entry = {
        "source-ref": f"s3://{BUCKET_NAME}/car-damage/{image_name}",
        "damage-classification": damage_class,
        "damage-classification-metadata": {
            "confidence": 1.0,
            "job-name": "car-damage-labeling",
            "class-name": damage_class,
            "human-annotated": "yes"
        }
    }
    manifest_lines.append(json.dumps(manifest_entry))

manifest_content = '\n'.join(manifest_lines)

# Upload manifest to S3
manifest_key = f"manifests/{PROJECT_NAME}-manifest.jsonl"
s3.put_object(
    Bucket=BUCKET_NAME,
    Key=manifest_key,
    Body=manifest_content,
    ContentType='application/jsonl'
)

# Create Rekognition Custom Labels project
try:
    response = rekognition.create_project(ProjectName=PROJECT_NAME)
    project_arn = response['ProjectArn']
    print(f"Created project: {project_arn}")
except Exception as e:
    if 'ResourceAlreadyExists' in str(e):
        projects = rekognition.describe_projects()
        project_arn = next(p['ProjectArn'] for p in projects['ProjectDescriptions'] 
                          if p['ProjectName'] == PROJECT_NAME)
        print(f"Using existing project: {project_arn}")
    else:
        raise e

print(f"\n✅ Upload complete!")
print(f"📁 Project ARN: {project_arn}")
print(f"📄 Manifest: s3://{BUCKET_NAME}/{manifest_key}")
print(f"🖼️  Images uploaded: {len(successful_uploads)}")
print(f"\nNext: Create dataset in Rekognition console using the manifest file")
