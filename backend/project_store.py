"""
DynamoDB + S3 project storage layer for CloudCanva.

Architecture Decision:
- DynamoDB stores project METADATA (user, name, dates, S3 key)
- S3 stores the actual DATA (canvas JSON, which can be large)

Why both?
- DynamoDB has a 400KB item size limit — canvas JSON with many nodes could exceed this
- S3 has no practical size limit and is cheap for file storage
- DynamoDB gives us fast queries ("get all projects for user X") that S3 can't do efficiently
- Together: DynamoDB = index/catalog, S3 = file system

Table Schema:
  Partition Key: user_email (String) — groups all projects by user
  Sort Key: project_name (String) — unique project name within a user's scope
  Attributes: created_at, updated_at, s3_key, node_count
"""

import os
import json
import boto3
from datetime import datetime, timezone
from typing import Optional
from botocore.exceptions import ClientError

# ─── Configuration (loaded from .env via main.py's load_dotenv) ────────────
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "CloudCanva-Projects")
S3_BUCKET = os.environ.get("S3_BUCKET", "cloudcanva-terraform-exports-hareniv")

# ─── AWS Clients ───────────────────────────────────────────────────────────
# boto3.resource gives us a higher-level ORM-like interface for DynamoDB
# boto3.client gives us lower-level access for S3
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)
s3_client = boto3.client("s3", region_name=AWS_REGION)


def save_project(user_email: str, project_name: str, canvas_data: dict) -> dict:
    """
    Save a project (create or update).

    Steps:
    1. Upload canvas JSON to S3 at a deterministic path
    2. Write/overwrite metadata record in DynamoDB

    The S3 key follows: users/{email}/projects/{project_name}/canvas.json
    This makes it easy to find and manage files per user.
    """
    now = datetime.now(timezone.utc).isoformat()

    # Step 1: Upload canvas data to S3
    # We use a structured key so files are organized by user
    s3_key = f"users/{user_email}/projects/{project_name}/canvas.json"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(canvas_data),
        ContentType="application/json",
    )

    # Step 2: Write metadata to DynamoDB
    # put_item does an upsert — creates if new, overwrites if exists
    node_count = len(canvas_data.get("nodes", []))

    item = {
        "user_email": user_email,       # Partition key
        "project_name": project_name,    # Sort key
        "s3_key": s3_key,                # Pointer to actual data in S3
        "node_count": node_count,        # Quick stat without loading full data
        "updated_at": now,               # Track last modification
    }

    # Only set created_at on first save (don't overwrite on updates)
    # We use a DynamoDB condition expression for this
    try:
        # Try to update existing item (preserves created_at)
        table.update_item(
            Key={
                "user_email": user_email,
                "project_name": project_name,
            },
            UpdateExpression="SET s3_key = :s3, node_count = :nc, updated_at = :ua",
            ExpressionAttributeValues={
                ":s3": s3_key,
                ":nc": node_count,
                ":ua": now,
            },
            ConditionExpression="attribute_exists(user_email)",  # Only if item exists
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # Item doesn't exist yet — create with created_at
            item["created_at"] = now
            table.put_item(Item=item)
        else:
            raise

    return {"message": f"Project '{project_name}' saved", "s3_key": s3_key}


def list_projects(user_email: str) -> list:
    """
    Get all projects for a user.

    Uses DynamoDB Query (not Scan):
    - Query = search within one partition (fast, indexed, O(log n))
    - Scan = search entire table (slow, expensive, reads everything)

    Always prefer Query when you know the partition key.
    """
    response = table.query(
        KeyConditionExpression="user_email = :email",
        ExpressionAttributeValues={":email": user_email},
        # Only return metadata, not the full item (saves read capacity)
        ProjectionExpression="project_name, created_at, updated_at, node_count",
    )

    return response.get("Items", [])


def get_project(user_email: str, project_name: str) -> Optional[dict]:
    """
    Load a specific project's full canvas data.

    Steps:
    1. Get metadata from DynamoDB (to find the S3 key)
    2. Download canvas JSON from S3
    3. Return combined result
    """
    # Step 1: Get metadata from DynamoDB
    response = table.get_item(
        Key={
            "user_email": user_email,
            "project_name": project_name,
        }
    )

    item = response.get("Item")
    if not item:
        return None

    # Step 2: Download canvas data from S3
    s3_key = item["s3_key"]
    try:
        s3_response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        canvas_data = json.loads(s3_response["Body"].read().decode("utf-8"))
    except ClientError:
        # S3 object missing (data corruption case)
        canvas_data = {"nodes": [], "edges": [], "elasticIP_association": []}

    return {
        "project_name": item["project_name"],
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "node_count": item.get("node_count", 0),
        "canvas_data": canvas_data,
    }


def delete_project(user_email: str, project_name: str) -> dict:
    """
    Delete a project (both metadata and files).

    Steps:
    1. Get the S3 key from DynamoDB
    2. Delete the S3 object
    3. Delete the DynamoDB record

    Order matters: delete S3 first, then DynamoDB.
    If DynamoDB delete fails, we can still find the S3 key to retry.
    If S3 delete fails, we have an orphaned file (less bad than orphaned metadata).
    """
    # Step 1: Get S3 key
    response = table.get_item(
        Key={"user_email": user_email, "project_name": project_name}
    )
    item = response.get("Item")
    if not item:
        return {"message": "Project not found"}

    # Step 2: Delete from S3
    s3_key = item.get("s3_key", "")
    if s3_key:
        try:
            s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        except ClientError:
            pass  # Non-critical: orphaned S3 object is acceptable

    # Step 3: Delete from DynamoDB
    table.delete_item(
        Key={"user_email": user_email, "project_name": project_name}
    )

    return {"message": f"Project '{project_name}' deleted"}
