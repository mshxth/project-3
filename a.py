import boto3
import json

# Download current manifest
s3 = boto3.client('s3')
response = s3.get_object(Bucket='damage-analyzer-images', Key='manifests/DamageAnalyzer-manifest.jsonl')
content = response['Body'].read().decode('utf-8')

# Fix the format
fixed_lines = []
for line in content.strip().split('\n'):
    entry = json.loads(line)
    
    # Extract the class name from damage-classification field
    damage_class = entry.get('damage-classification', 'unknown')
    
    # Create correct format
    fixed_entry = {
        "source-ref": entry['source-ref'],
        "damage-classification": damage_class,
        "damage-classification-metadata": {
            "confidence": 1.0,
            "job-name": "car-damage-labeling", 
            "class-name": damage_class,
            "human-annotated": "yes"
        }
    }
    fixed_lines.append(json.dumps(fixed_entry))

# Upload fixed manifest
fixed_content = '\n'.join(fixed_lines)
s3.put_object(
    Bucket='damage-analyzer-images',
    Key='manifests/DamageAnalyzer-manifest-fixed.jsonl',
    Body=fixed_content
)

print("Fixed manifest created!")
