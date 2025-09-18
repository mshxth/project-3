import boto3
import json

s3 = boto3.client('s3')

# Download current manifest
response = s3.get_object(Bucket='damage-analyzer-images', Key='manifests/DamageAnalyzer-manifest-fixed.jsonl')
content = response['Body'].read().decode('utf-8')

# Convert to the correct AWS format
fixed_lines = []
for line in content.strip().split('\n'):
    entry = json.loads(line)
    damage_class = entry.get('damage-classification', 'unknown')
    
    # Use the exact format from your screenshot
    fixed_entry = {
        "source-ref": entry['source-ref'],
        "image-label": 0,  # Class index (you might need to map classes to numbers)
        "image-label-metadata": {
            "class-name": damage_class,
            "confidence": 1,
            "type": "groundtruth/image-classification",
            "job-name": "labeling-job/image-label"
        }
    }
    fixed_lines.append(json.dumps(fixed_entry))

# Upload the corrected manifest
s3.put_object(
    Bucket='damage-analyzer-images',
    Key='manifests/DamageAnalyzer-manifest-correct.jsonl',
    Body='\n'.join(fixed_lines)
)

print("Correct manifest created!")
