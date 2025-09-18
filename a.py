import boto3
import json

s3 = boto3.client('s3')

# Download current manifest
response = s3.get_object(Bucket='damage-analyzer-images', Key='manifests/DamageAnalyzer-manifest-fixed.jsonl')
content = response['Body'].read().decode('utf-8')

# Convert to the AWS Custom Labels format
fixed_lines = []
for line in content.strip().split('\n'):
    entry = json.loads(line)
    damage_class = entry.get('damage-classification', 'unknown')
    
    # Use the exact AWS format from your screenshot
    fixed_entry = {
        "source-ref": entry['source-ref'],
        "class": damage_class,
        "class-metadata": {
            "confidence": 1,
            "job-name": "labeling-job", 
            "class-name": damage_class,
            "human-annotated": "yes",
            "creation-date": "2021-01-01T00:00:00.000Z"
        }
    }
    fixed_lines.append(json.dumps(fixed_entry))

# Upload the AWS-compatible manifest
s3.put_object(
    Bucket='damage-analyzer-images',
    Key='manifests/DamageAnalyzer-manifest-aws.jsonl',
    Body='\n'.join(fixed_lines)
)

print("AWS-compatible manifest created: manifests/DamageAnalyzer-manifest-aws.jsonl")
