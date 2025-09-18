import boto3

# Configuration
BUCKET_NAME = 'your-s3-bucket'
MANIFEST_KEY = 'manifests/car-damage-detection-manifest.jsonl'
PROJECT_NAME = 'car-damage-detection'
DATASET_NAME = 'car-damage-dataset'

# Initialize client
rekognition = boto3.client('rekognition')

# Create project
try:
    response = rekognition.create_project(ProjectName=PROJECT_NAME)
    project_arn = response['ProjectArn']
except:
    projects = rekognition.describe_projects()
    project_arn = next(p['ProjectArn'] for p in projects['ProjectDescriptions'] 
                      if p['ProjectName'] == PROJECT_NAME)

# Create dataset from manifest
response = rekognition.create_dataset(
    ProjectArn=project_arn,
    DatasetType='TRAIN',
    DatasetSource={
        'GroundTruthManifest': {
            'S3Object': {
                'Bucket': BUCKET_NAME,
                'Name': MANIFEST_KEY
            }
        }
    }
)

dataset_arn = response['DatasetArn']

print(f"Dataset created: {dataset_arn}")
print(f"Project: {project_arn}")
print("Ready for training!")
