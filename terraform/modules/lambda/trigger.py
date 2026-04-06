import json
import boto3
import os

def handler(event, context):
    print("TradeSphere ingestion trigger fired")
    print(json.dumps(event))
    return {
        "statusCode": 200,
        "body": "Trigger executed successfully"
    }