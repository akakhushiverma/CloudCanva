from dotenv import load_dotenv
load_dotenv()

from template_registry import TemplateRegistry
from generators.orcherastor import TerraformOrchestrator
import json
import boto3
from boto3.dynamodb.conditions import Key
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from fastapi.responses import FileResponse
from auth import get_current_user

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Configuration ───────────────────────────────────────────────
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "cloudcanva-terraform-exports-hareniv")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "CloudCanva-Projects")

# ─── Multi-Region Support ─────────────────────────────────────────
REGION_CONFIG = {
    "us-east-1": {
        "ami_amazon_linux": "ami-0c02fb55956c7d316",
        "ami_ubuntu": "ami-0aa7d40eeae50c9a9",
        "azs": ["us-east-1a", "us-east-1b", "us-east-1c"],
    },
    "us-west-2": {
        "ami_amazon_linux": "ami-0ceecbb0f30a902a6",
        "ami_ubuntu": "ami-017fecd1353bcc96e",
        "azs": ["us-west-2a", "us-west-2b", "us-west-2c"],
    },
    "eu-north-1": {
        "ami_amazon_linux": "ami-09a9858973b288bdd",
        "ami_ubuntu": "ami-0014ce3e52a6bbf4d",
        "azs": ["eu-north-1a", "eu-north-1b", "eu-north-1c"],
    },
    "eu-west-1": {
        "ami_amazon_linux": "ami-0905a3c97561e0b69",
        "ami_ubuntu": "ami-0694d931cee176e7d",
        "azs": ["eu-west-1a", "eu-west-1b", "eu-west-1c"],
    },
    "ap-south-1": {
        "ami_amazon_linux": "ami-0f1dcc636b69a6438",
        "ami_ubuntu": "ami-03f4878755434977f",
        "azs": ["ap-south-1a", "ap-south-1b", "ap-south-1c"],
    },
    "ap-northeast-1": {
        "ami_amazon_linux": "ami-0d52744d6551d851e",
        "ami_ubuntu": "ami-0d52744d6551d851e",
        "azs": ["ap-northeast-1a", "ap-northeast-1c", "ap-northeast-1d"],
    },
}

DEFAULT_AMI = "ami-09a9858973b288bdd"  # Amazon Linux 2023 in eu-north-1
AVAILABILITY_ZONES = ["eu-north-1a", "eu-north-1b", "eu-north-1c"]

# ─── AWS Clients ─────────────────────────────────────────────────
s3_client = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
projects_table = dynamodb.Table(DYNAMODB_TABLE)


@app.get("/")
def read_root():
    return {"message": "CloudCanva Backend is running"}


@app.get("/regions")
def get_regions():
    """Return available regions with their AMIs and AZs."""
    return {
        "regions": {
            region: {
                "amis": {
                    "Amazon Linux": config["ami_amazon_linux"],
                    "Ubuntu": config["ami_ubuntu"],
                },
                "availability_zones": config["azs"],
            }
            for region, config in REGION_CONFIG.items()
        },
        "default_region": "eu-north-1",
    }


@app.post("/generate")
async def generate(payload: Dict[str, Any], user: dict = Depends(get_current_user)):
    try:
        project_name = payload.pop("project_name", f"untitled-{uuid.uuid4().hex[:6]}")
        selected_region = payload.pop("region", "eu-north-1")
        user_email = user.get("username", user.get("sub", "unknown"))

        # Inject region into config for template generation
        payload["_region"] = selected_region

        zip_file_path = generate_tf_code(payload, selected_region)

        # Upload to S3 under user's folder
        s3_key = f"users/{user_email}/{project_name}.zip"
        try:
            s3_client.upload_file(zip_file_path, S3_BUCKET, s3_key)
            print(f"Uploaded to S3: {s3_key}")
        except Exception as s3_err:
            print(f"S3 upload failed: {s3_err}")

        # Save metadata to DynamoDB
        try:
            projects_table.put_item(Item={
                "user_email": user_email,
                "project_name": project_name,
                "s3_key": s3_key,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "node_count": len(payload.get("connected_services", [{}])[0].get("services", [])),
            })
        except Exception as db_err:
            print(f"DynamoDB save failed: {db_err}")

        # Return file directly to user
        return FileResponse(
            path=zip_file_path,
            media_type="application/zip",
            filename=f"{project_name}.zip",
        )
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/projects")
async def list_projects(user: dict = Depends(get_current_user)):
    """List all projects for the authenticated user."""
    try:
        user_email = user.get("username", user.get("sub", "unknown"))
        response = projects_table.query(
            KeyConditionExpression=Key("user_email").eq(user_email),
        )
        projects = response.get("Items", [])
        # Sort by created_at descending (newest first)
        projects.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return {"projects": projects}
    except Exception as e:
        print(f"Error listing projects: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/projects/{project_name}/download")
async def download_project(project_name: str, user: dict = Depends(get_current_user)):
    """Generate a presigned URL to download a project's zip file."""
    try:
        user_email = user.get("username", user.get("sub", "unknown"))

        # Look up the project in DynamoDB
        response = projects_table.get_item(Key={
            "user_email": user_email,
            "project_name": project_name,
        })
        item = response.get("Item")
        if not item:
            raise HTTPException(status_code=404, detail="Project not found")

        # Generate presigned URL (valid for 10 minutes)
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": item["s3_key"]},
            ExpiresIn=600,
        )
        return {"download_url": presigned_url, "project_name": project_name}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error generating download URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/projects/{project_name}")
async def delete_project(project_name: str, user: dict = Depends(get_current_user)):
    """Delete a project (removes from DynamoDB and S3)."""
    try:
        user_email = user.get("username", user.get("sub", "unknown"))

        # Look up the project
        response = projects_table.get_item(Key={
            "user_email": user_email,
            "project_name": project_name,
        })
        item = response.get("Item")
        if not item:
            raise HTTPException(status_code=404, detail="Project not found")

        # Delete from S3
        try:
            s3_client.delete_object(Bucket=S3_BUCKET, Key=item["s3_key"])
        except Exception as s3_err:
            print(f"S3 delete failed: {s3_err}")

        # Delete from DynamoDB
        projects_table.delete_item(Key={
            "user_email": user_email,
            "project_name": project_name,
        })

        return {"message": f"Project '{project_name}' deleted"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting project: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/validate")
async def validate(payload: Dict[str, Any], user: dict = Depends(get_current_user)):
    try:
        config = payload.get("config", payload)
        errors, warnings = validate_architecture(config)
        if len(errors) == 0 and len(warnings) == 0:
            return {"success": True, "errors": [], "warnings": []}
        elif len(errors) == 0:
            return {"success": True, "errors": [], "warnings": warnings}
        else:
            return {"success": False, "errors": errors, "warnings": warnings}
    except Exception as e:
        print(f"Validation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ai/generate")
async def ai_generate(payload: Dict[str, Any], user: dict = Depends(get_current_user)):
    prompt = payload.get("prompt", "")
    region = payload.get("region", "eu-north-1")
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt is required")
    
    # Get region-specific config
    region_conf = REGION_CONFIG.get(region, REGION_CONFIG["eu-north-1"])
    
    try:
        result = generate_with_bedrock(prompt, region, region_conf)
        # Post-process to ensure refs are correct
        result = post_process_ai_response(result, region, region_conf)
        return result
    except Exception as e:
        print(f"Bedrock error: {e}, falling back to mock")
        result = mock_ai_generate(prompt)
        return result


# ─── Quota Check Endpoints ────────────────────────────────────────

# Default AWS limits (used for static warnings)
AWS_DEFAULT_LIMITS = {
    "vpc": {"limit": 5, "label": "VPCs"},
    "aws_eip": {"limit": 5, "label": "Elastic IPs"},
    "aws_instance": {"limit": 20, "label": "EC2 Instances"},
    "subnet": {"limit": 200, "label": "Subnets per VPC"},
    "security_group": {"limit": 500, "label": "Security Groups per VPC"},
    "internet_gateway": {"limit": 5, "label": "Internet Gateways"},
}


@app.post("/check-quotas")
async def check_quotas(payload: Dict[str, Any], user: dict = Depends(get_current_user)):
    """
    Static quota warnings based on known AWS default limits.
    Counts resources in the design and warns about potential limit issues.
    """
    warnings = []
    services = []

    if "connected_services" in payload:
        for infra in payload["connected_services"]:
            services.extend(infra.get("services", []))

    # Count resources by type
    resource_counts: Dict[str, int] = {}
    for service in services:
        stype = service.get("type", "")
        resource_counts[stype] = resource_counts.get(stype, 0) + 1

    # Generate warnings for known limits
    for resource_type, info in AWS_DEFAULT_LIMITS.items():
        count = resource_counts.get(resource_type, 0)
        if count > 0:
            if count >= info["limit"]:
                warnings.append({
                    "level": "error",
                    "message": f"Your design creates {count} {info['label']}. AWS default limit is {info['limit']} per region. Deployment WILL fail.",
                })
            elif count >= info["limit"] - 1:
                warnings.append({
                    "level": "warning",
                    "message": f"Your design creates {count} {info['label']}. AWS default limit is {info['limit']} per region. You're close to the limit.",
                })
            else:
                warnings.append({
                    "level": "info",
                    "message": f"Your design creates {count} {info['label']}. AWS default limit is {info['limit']} per region.",
                })

    return {"warnings": warnings, "resource_counts": resource_counts}


@app.post("/check-quotas-live")
async def check_quotas_live(payload: Dict[str, Any], user: dict = Depends(get_current_user)):
    """
    Live quota check using user's AWS credentials.
    User provides role_arn OR access_key + secret_key.
    We assume the role / use the keys to check actual usage.
    """
    credentials = payload.get("credentials", {})
    region = payload.get("region", AWS_REGION)

    role_arn = credentials.get("role_arn")
    access_key = credentials.get("access_key_id")
    secret_key = credentials.get("secret_access_key")
    session_token = credentials.get("session_token")

    try:
        # Get credentials either via AssumeRole or direct keys
        if role_arn:
            sts_client = boto3.client("sts", region_name=region)
            assumed = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName="cloudcanva-quota-check",
                DurationSeconds=900,
            )
            creds = assumed["Credentials"]
            ec2_client = boto3.client(
                "ec2",
                region_name=region,
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
            )
        elif access_key and secret_key:
            ec2_client = boto3.client(
                "ec2",
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
            )
        else:
            raise HTTPException(status_code=400, detail="Provide role_arn or access_key_id + secret_access_key")

        # Check actual usage
        vpcs = ec2_client.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["false"]}])
        vpc_count = len(vpcs.get("Vpcs", []))

        eips = ec2_client.describe_addresses()
        eip_count = len(eips.get("Addresses", []))

        instances = ec2_client.describe_instances(Filters=[{"Name": "instance-state-name", "Values": ["running", "stopped"]}])
        instance_count = sum(len(r["Instances"]) for r in instances.get("Reservations", []))

        # Count what the design will add
        services = []
        if "connected_services" in payload:
            for infra in payload["connected_services"]:
                services.extend(infra.get("services", []))

        design_vpcs = sum(1 for s in services if s.get("type") == "vpc")
        design_eips = sum(1 for s in services if s.get("type") == "aws_eip")
        design_instances = sum(1 for s in services if s.get("type") == "aws_instance")

        # Build results
        results = []

        # VPC check
        total_vpcs = vpc_count + design_vpcs
        results.append({
            "resource": "VPCs",
            "current": vpc_count,
            "adding": design_vpcs,
            "total_after": total_vpcs,
            "limit": 5,
            "status": "error" if total_vpcs > 5 else ("warning" if total_vpcs == 5 else "ok"),
            "message": f"VPCs: {vpc_count} existing + {design_vpcs} new = {total_vpcs}/5",
        })

        # EIP check
        total_eips = eip_count + design_eips
        results.append({
            "resource": "Elastic IPs",
            "current": eip_count,
            "adding": design_eips,
            "total_after": total_eips,
            "limit": 5,
            "status": "error" if total_eips > 5 else ("warning" if total_eips == 5 else "ok"),
            "message": f"Elastic IPs: {eip_count} existing + {design_eips} new = {total_eips}/5",
        })

        # Instance check
        total_instances = instance_count + design_instances
        results.append({
            "resource": "EC2 Instances",
            "current": instance_count,
            "adding": design_instances,
            "total_after": total_instances,
            "limit": 20,
            "status": "error" if total_instances > 20 else ("warning" if total_instances >= 18 else "ok"),
            "message": f"EC2 Instances: {instance_count} existing + {design_instances} new = {total_instances}/20",
        })

        return {"results": results, "region": region}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Live quota check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to check quotas: {str(e)}")


# ─── IAM Policy Generator ─────────────────────────────────────────

# Mapping: resource type → required IAM actions
RESOURCE_IAM_ACTIONS = {
    "vpc": [
        "ec2:CreateVpc", "ec2:DeleteVpc", "ec2:DescribeVpcs",
        "ec2:ModifyVpcAttribute", "ec2:CreateTags",
    ],
    "subnet": [
        "ec2:CreateSubnet", "ec2:DeleteSubnet", "ec2:DescribeSubnets",
        "ec2:ModifySubnetAttribute", "ec2:CreateTags",
    ],
    "aws_instance": [
        "ec2:RunInstances", "ec2:TerminateInstances", "ec2:DescribeInstances",
        "ec2:DescribeInstanceStatus", "ec2:CreateTags",
    ],
    "security_group": [
        "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup",
        "ec2:DescribeSecurityGroups", "ec2:CreateTags",
    ],
    "vpc_security_group_ingress_rule": [
        "ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress",
        "ec2:DescribeSecurityGroupRules",
    ],
    "vpc_security_group_egress_rule": [
        "ec2:AuthorizeSecurityGroupEgress", "ec2:RevokeSecurityGroupEgress",
        "ec2:DescribeSecurityGroupRules",
    ],
    "internet_gateway": [
        "ec2:CreateInternetGateway", "ec2:DeleteInternetGateway",
        "ec2:AttachInternetGateway", "ec2:DetachInternetGateway",
        "ec2:DescribeInternetGateways", "ec2:CreateTags",
    ],
    "route_table": [
        "ec2:CreateRouteTable", "ec2:DeleteRouteTable",
        "ec2:CreateRoute", "ec2:DeleteRoute",
        "ec2:DescribeRouteTables", "ec2:AssociateRouteTable", "ec2:CreateTags",
    ],
    "aws_eip": [
        "ec2:AllocateAddress", "ec2:ReleaseAddress",
        "ec2:AssociateAddress", "ec2:DisassociateAddress",
        "ec2:DescribeAddresses", "ec2:CreateTags",
    ],
    "eip_association": [
        "ec2:AssociateAddress", "ec2:DisassociateAddress",
    ],
    "s3_bucket": [
        "s3:CreateBucket", "s3:DeleteBucket", "s3:ListBucket",
        "s3:PutBucketTagging", "s3:GetBucketTagging",
    ],
    "load_balancer": [
        "elasticloadbalancing:CreateLoadBalancer", "elasticloadbalancing:DeleteLoadBalancer",
        "elasticloadbalancing:DescribeLoadBalancers", "elasticloadbalancing:AddTags",
    ],
    "target_group": [
        "elasticloadbalancing:CreateTargetGroup", "elasticloadbalancing:DeleteTargetGroup",
        "elasticloadbalancing:RegisterTargets", "elasticloadbalancing:DescribeTargetGroups",
    ],
}


@app.post("/generate-iam-policy")
async def generate_iam_policy(payload: Dict[str, Any], user: dict = Depends(get_current_user)):
    """Generate the minimum IAM policy required to deploy the designed infrastructure."""
    try:
        services = []
        if "connected_services" in payload:
            for infra in payload["connected_services"]:
                services.extend(infra.get("services", []))

        # Collect all required actions based on resource types
        required_actions = set()
        resource_types_found = set()

        for service in services:
            stype = service.get("type", "")
            if stype in RESOURCE_IAM_ACTIONS:
                resource_types_found.add(stype)
                for action in RESOURCE_IAM_ACTIONS[stype]:
                    required_actions.add(action)

        # Always include basic describe/tag actions for Terraform state management
        required_actions.add("ec2:DescribeAccountAttributes")
        required_actions.add("ec2:DescribeAvailabilityZones")
        required_actions.add("sts:GetCallerIdentity")

        # Build the policy document
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "CloudCanvaTerraformPolicy",
                    "Effect": "Allow",
                    "Action": sorted(list(required_actions)),
                    "Resource": "*",
                }
            ],
        }

        return {
            "policy": policy,
            "resource_types": sorted(list(resource_types_found)),
            "action_count": len(required_actions),
            "summary": f"This policy grants {len(required_actions)} permissions for {len(resource_types_found)} resource types.",
        }

    except Exception as e:
        print(f"IAM policy generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/validate-iam-permissions")
async def validate_iam_permissions(payload: Dict[str, Any], user: dict = Depends(get_current_user)):
    """
    Validate if a user's IAM role has the required permissions to deploy the designed infrastructure.
    User provides role_arn or access keys. We simulate the required actions against their role.
    """
    credentials = payload.get("credentials", {})
    region = payload.get("region", AWS_REGION)

    role_arn = credentials.get("role_arn")
    access_key = credentials.get("access_key_id")
    secret_key = credentials.get("secret_access_key")

    try:
        # First, determine what actions are needed
        services = []
        if "connected_services" in payload:
            for infra in payload["connected_services"]:
                services.extend(infra.get("services", []))

        required_actions = set()
        for service in services:
            stype = service.get("type", "")
            if stype in RESOURCE_IAM_ACTIONS:
                for action in RESOURCE_IAM_ACTIONS[stype]:
                    required_actions.add(action)

        if not required_actions:
            return {"results": [], "message": "No resources to validate."}

        # Get IAM client with user's credentials
        if role_arn:
            sts_client = boto3.client("sts", region_name=region)
            assumed = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName="cloudcanva-iam-check",
                DurationSeconds=900,
            )
            creds = assumed["Credentials"]
            iam_client = boto3.client(
                "iam",
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
            )
            principal_arn = role_arn
        elif access_key and secret_key:
            iam_client = boto3.client(
                "iam",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
            # Get the ARN of the caller
            sts_client = boto3.client(
                "sts",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
            caller = sts_client.get_caller_identity()
            principal_arn = caller["Arn"]
        else:
            raise HTTPException(status_code=400, detail="Provide role_arn or access_key_id + secret_access_key")

        # Simulate the policy — check which actions are allowed/denied
        actions_list = sorted(list(required_actions))

        # SimulatePrincipalPolicy can handle up to 50 actions at a time
        results = []
        for i in range(0, len(actions_list), 50):
            batch = actions_list[i:i+50]
            response = iam_client.simulate_principal_policy(
                PolicySourceArn=principal_arn,
                ActionNames=batch,
            )
            for result in response.get("EvaluationResults", []):
                results.append({
                    "action": result["EvalActionName"],
                    "decision": result["EvalDecision"],  # "allowed" or "implicitDeny" or "explicitDeny"
                    "status": "allowed" if result["EvalDecision"] == "allowed" else "denied",
                })

        allowed = [r for r in results if r["status"] == "allowed"]
        denied = [r for r in results if r["status"] == "denied"]

        return {
            "results": results,
            "summary": {
                "total": len(results),
                "allowed": len(allowed),
                "denied": len(denied),
                "can_deploy": len(denied) == 0,
            },
            "denied_actions": [r["action"] for r in denied],
            "message": "All permissions available. Deployment will succeed." if len(denied) == 0
                else f"{len(denied)} permission(s) missing. Deployment will fail.",
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"IAM validation error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to validate permissions: {str(e)}")

def post_process_ai_response(data: dict, region: str = "eu-north-1", region_conf: dict = None) -> dict:
    """
    Post-process AI response to ensure:
    1. All Subnets have refs.vpc pointing to a VPC node
    2. All SecurityGroups have refs.vpc
    3. All InternetGateways have refs.vpc
    4. All EC2 instances have refs.subnet
    5. AMI and AZ values are correct for the selected region
    """
    if region_conf is None:
        region_conf = REGION_CONFIG.get(region, REGION_CONFIG["eu-north-1"])
    
    default_ami = region_conf["ami_amazon_linux"]
    azs = region_conf["azs"]
    
    nodes = data.get("nodes", [])

    # Find VPC and Subnet node IDs
    vpc_id = None
    subnet_ids = []
    for node in nodes:
        label = node.get("data", {}).get("label", "")
        if label == "VPC":
            vpc_id = node["id"]
        elif label == "Subnet":
            subnet_ids.append(node["id"])

    # Fix each node
    for node in nodes:
        label = node.get("data", {}).get("label", "")
        props = node.get("data", {}).get("properties", {})

        # Ensure properties has refs object
        if "refs" not in props:
            props["refs"] = {}

        if label == "Subnet":
            # Subnet must reference a VPC
            if not props["refs"].get("vpc") and vpc_id:
                props["refs"]["vpc"] = vpc_id
            # Fix availability zone
            az = props.get("availability_zone", "")
            if az and not az.startswith(region):
                props["availability_zone"] = azs[0]
            elif not az:
                props["availability_zone"] = azs[0]

        elif label == "EC2":
            # EC2 must have correct AMI and reference a subnet
            if not props.get("ami") or not props["ami"].startswith("ami-"):
                props["ami"] = default_ami
            # Validate AMI is for our region (replace common wrong-region AMIs)
            known_wrong_amis = ["ami-0c55b159cbfafe1f0", "ami-0c02fb55956c7d316",
                                "ami-0abcdef1234567890", "ami-09a9858973b288bdd",
                                "ami-0f1dcc636b69a6438", "ami-0014ce3e52a6bbf4d"]
            if props.get("ami") in known_wrong_amis and props["ami"] != default_ami:
                props["ami"] = default_ami
            if not props.get("instance_type"):
                props["instance_type"] = "t3.micro"
            # Reference first subnet if no subnet ref
            if not props["refs"].get("subnet") and subnet_ids:
                props["refs"]["subnet"] = subnet_ids[0]

        elif label == "SecurityGroup":
            # SecurityGroup must reference a VPC
            if not props["refs"].get("vpc") and vpc_id:
                props["refs"]["vpc"] = vpc_id
            
            # Security guardrails: fix risky ingress rules
            sensitive_ports = {22: "SSH", 3306: "MySQL", 5432: "PostgreSQL", 6379: "Redis", 27017: "MongoDB"}
            for rule in props.get("ingress", []):
                cidr = rule.get("cidr_blocks", [None])[0] if rule.get("cidr_blocks") else rule.get("cidr_ipv4")
                port = rule.get("from_port", 0)
                
                if cidr == "0.0.0.0/0" and port in sensitive_ports:
                    # Auto-fix: restrict sensitive ports to VPC CIDR
                    if port == 22:
                        rule["cidr_blocks"] = ["10.0.0.0/16"]  # VPC-only SSH
                    else:
                        rule["cidr_blocks"] = ["10.0.1.0/24"]  # App subnet only for DB ports
                elif not rule.get("cidr_blocks") and not rule.get("cidr_ipv4"):
                    rule["cidr_blocks"] = ["10.0.0.0/16"]
            
            for rule in props.get("egress", []):
                if not rule.get("cidr_blocks") and not rule.get("cidr_ipv4"):
                    rule["cidr_blocks"] = ["0.0.0.0/0"]

        elif label == "InternetGateway":
            # InternetGateway must reference a VPC
            if not props["refs"].get("vpc") and vpc_id:
                props["refs"]["vpc"] = vpc_id

        elif label == "RouteTable":
            # RouteTable must reference a VPC
            if not props["refs"].get("vpc") and vpc_id:
                props["refs"]["vpc"] = vpc_id

        elif label == "LoadBalancer":
            # LoadBalancer must have subnets — use all subnet IDs if not set
            if not props.get("refs"):
                props["refs"] = {}
            if not props["refs"].get("subnets") and subnet_ids:
                props["refs"]["subnets"] = subnet_ids[:2]  # need at least 2 subnets

        elif label == "RDS":
            # RDS needs subnets in at least 2 AZs for the subnet group
            if not props.get("refs"):
                props["refs"] = {}
            if not props["refs"].get("subnets") and subnet_ids:
                props["refs"]["subnets"] = subnet_ids[:2]  # at least 2 for multi-AZ
            if not props["refs"].get("subnet") and subnet_ids:
                props["refs"]["subnet"] = subnet_ids[0]

        # Write back
        node["data"]["properties"] = props

    data["nodes"] = nodes
    return data


def generate_with_bedrock(prompt: str, region: str = "eu-north-1", region_conf: dict = None):
    if region_conf is None:
        region_conf = REGION_CONFIG.get(region, REGION_CONFIG["eu-north-1"])
    
    default_ami = region_conf["ami_amazon_linux"]
    azs = region_conf["azs"]
    
    client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    system_prompt = (
        "You are an AWS infrastructure architect. Given a user requirement, generate an AWS architecture as JSON. "
        "Return ONLY valid JSON (no markdown, no explanation, no code fences) in this exact format: "
        "{\"summary\": \"one line description\", \"nodes\": [{\"id\": \"unique-id\", \"type\": \"service\", "
        "\"position\": {\"x\": 200, \"y\": 100}, \"data\": {\"label\": \"EC2 or VPC or Subnet or RDS or S3 or "
        "SecurityGroup or InternetGateway or ElasticIP or LoadBalancer or TargetGroup or RouteTable\", "
        "\"type\": \"compute or network or database or storage or SecurityGroup or LoadBalancer or TargetGroup or "
        "InternetGateway or Subnet or RouteTable or elastic\", \"provider\": \"aws\", "
        "\"rawLabel\": \"aws_instance or vpc or subnet or rds_instance or s3_bucket or security_group or "
        "internet_gateway or aws_eip or load_balancer or target_group or route_table\", "
        "\"serviceName\": \"descriptive name\", \"properties\": {}}}], "
        "\"edges\": [{\"id\": \"edge-id\", \"source\": \"node-id\", \"target\": \"node-id\"}]}. "
        "\n\nCRITICAL RULES: "
        f"\n1. Region is {region}. Use ONLY availability zones: {', '.join(azs)}. "
        f"\n2. For EC2 instances, ALWAYS use AMI: {default_ami} (Amazon Linux 2023 in {region}). "
        "\n3. EVERY Subnet MUST have refs.vpc set to the VPC node's id. "
        "\n4. EVERY SecurityGroup MUST have refs.vpc set to the VPC node's id. "
        "\n5. EVERY InternetGateway MUST have refs.vpc set to the VPC node's id. "
        "\n6. EVERY EC2 instance MUST have refs.subnet set to a Subnet node's id. "
        "\n7. EVERY RouteTable MUST have refs.vpc set to the VPC node's id. "
        "\n8. Position nodes logically (VPC at top y=50, subnets y=200, services y=400). "
        "\n9. Space nodes horizontally (x increases by 200). "
        "\n10. Add edges for logical connections between services. "
        "\n11. Include proper properties (cidr_block for VPC/Subnet, ami+instance_type for EC2, engine for RDS). "
        "\n12. The properties object MUST always contain a 'refs' object even if empty: \"refs\": {}. "
        "\n13. Return ONLY the JSON object, nothing else."
        "\n\nSECURITY RULES (MANDATORY): "
        "\n14. NEVER set SSH (port 22) ingress cidr_blocks to 0.0.0.0/0. Use the VPC CIDR (e.g., 10.0.0.0/16) instead. "
        "\n15. NEVER expose database ports (3306, 5432, 6379) to 0.0.0.0/0. Use the app/web subnet CIDR (e.g., 10.0.1.0/24). "
        "\n16. ONLY ports 80 (HTTP) and 443 (HTTPS) should have 0.0.0.0/0 as ingress source. "
        "\n17. Database EC2 instances MUST be in private subnets (map_public_ip_on_launch = false). "
        "\n18. Always include an egress rule with protocol -1, from_port 0, to_port 0, cidr_blocks 0.0.0.0/0 for outbound. "
    )
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [{"role": "user", "content": system_prompt + "\n\nUser requirement: " + prompt}]
    })
    response = client.invoke_model(
        modelId="eu.anthropic.claude-sonnet-4-6",
        body=body,
        contentType="application/json"
    )
    result = json.loads(response["body"].read())
    ai_text = result["content"][0]["text"]
    ai_text = ai_text.strip()
    if ai_text.startswith("```"):
        ai_text = ai_text.split("\n", 1)[1]
    if ai_text.endswith("```"):
        ai_text = ai_text.rsplit("```", 1)[0]
    ai_text = ai_text.strip()
    architecture = json.loads(ai_text)
    return architecture


def generate_tf_code(config_data, region="eu-north-1"):
    templates_path = "templates"
    template_registry = TemplateRegistry(templates_path)
    orchestrator = TerraformOrchestrator(config_data, template_registry, region)
    terraform_code = orchestrator.generate()
    current_dir = os.getcwd()
    temp_base = tempfile.mkdtemp(dir=current_dir)
    output_dir = os.path.join(temp_base, "tf_output")
    os.makedirs(output_dir, exist_ok=True)
    for infra_name, file in terraform_code.items():
        infra_dir = os.path.join(output_dir, infra_name)
        os.makedirs(infra_dir, exist_ok=True)
        if "tf" in file:
            tf_file_path = os.path.join(infra_dir, "main.tf")
            with open(tf_file_path, "w") as f:
                for obj in file["tf"]:
                    f.write(obj)
                    f.write("\n")
    zip_file_path = shutil.make_archive(output_dir, "zip", output_dir)
    return zip_file_path


def validate_architecture(config):
    errors = []
    warnings = []
    services = []
    if "connected_services" in config:
        for infra in config["connected_services"]:
            services.extend(infra.get("services", []))
    
    sensitive_ports = {22: "SSH", 3306: "MySQL", 5432: "PostgreSQL", 6379: "Redis", 27017: "MongoDB"}
    
    for service in services:
        stype = service.get("type", "")
        name = service.get("name", service.get("id", "unknown"))
        if stype == "aws_instance":
            if not service.get("ami"):
                errors.append(f"{name}: Missing AMI")
            if not service.get("instance_type"):
                errors.append(f"{name}: Missing instance type")
        elif stype == "vpc":
            if not service.get("cidr_block"):
                errors.append(f"{name}: Missing CIDR block")
        elif stype == "subnet":
            if not service.get("cidr_block"):
                errors.append(f"{name}: Missing CIDR block")
            if not service.get("refs", {}).get("vpc"):
                errors.append(f"{name}: Subnet not linked to a VPC")
        elif stype == "security_group":
            if not service.get("refs", {}).get("vpc"):
                errors.append(f"{name}: Security group not linked to a VPC")
        elif stype == "internet_gateway":
            if not service.get("refs", {}).get("vpc"):
                errors.append(f"{name}: Internet gateway not attached to VPC")
        elif stype == "vpc_security_group_ingress_rule":
            # Security guardrails
            port = service.get("from_port", 0)
            cidr_list = service.get("cidr_blocks", [])
            cidr = cidr_list[0] if cidr_list else service.get("cidr_ipv4", "")
            
            if cidr == "0.0.0.0/0" and port in sensitive_ports:
                warnings.append(
                    f"⚠️ Security Risk: {sensitive_ports[port]} (port {port}) is open to 0.0.0.0/0 (entire internet). "
                    f"Restrict to VPC CIDR or specific IP."
                )
            elif cidr == "0.0.0.0/0" and port == 0:
                warnings.append(
                    f"🔴 Critical: All ports open to 0.0.0.0/0. This exposes everything."
                )
    
    return errors, warnings


def mock_ai_generate(prompt):
    """Fallback mock generator with proper refs for eu-north-1."""
    p = prompt.lower()

    if "ecommerce" in p or "web app" in p or "three-tier" in p:
        return {
            "summary": "Scalable three-tier architecture with VPC, ALB, EC2, and RDS.",
            "nodes": [
                {"id": "ai-vpc-1", "type": "service", "position": {"x": 250, "y": 50}, "data": {"label": "VPC", "type": "network", "provider": "aws", "rawLabel": "vpc", "serviceName": "Main VPC", "properties": {"name": "main-vpc", "cidr_block": "10.0.0.0/16", "enable_dns_support": True, "enable_dns_hostname": True, "refs": {}, "tags": {}}}},
                {"id": "ai-subnet-1", "type": "service", "position": {"x": 100, "y": 200}, "data": {"label": "Subnet", "type": "Subnet", "provider": "aws", "rawLabel": "subnet", "serviceName": "Public Subnet", "properties": {"name": "public-subnet", "cidr_block": "10.0.1.0/24", "availability_zone": "eu-north-1a", "map_public_ip_on_launch": True, "refs": {"vpc": "ai-vpc-1"}, "tags": {}}}},
                {"id": "ai-subnet-2", "type": "service", "position": {"x": 400, "y": 200}, "data": {"label": "Subnet", "type": "Subnet", "provider": "aws", "rawLabel": "subnet", "serviceName": "Private Subnet", "properties": {"name": "private-subnet", "cidr_block": "10.0.2.0/24", "availability_zone": "eu-north-1b", "map_public_ip_on_launch": False, "refs": {"vpc": "ai-vpc-1"}, "tags": {}}}},
                {"id": "ai-sg-1", "type": "service", "position": {"x": 550, "y": 200}, "data": {"label": "SecurityGroup", "type": "SecurityGroup", "provider": "aws", "rawLabel": "security_group", "serviceName": "Web SG", "properties": {"name": "web-sg", "description": "Allow HTTP and HTTPS", "refs": {"vpc": "ai-vpc-1"}, "ingress": [{"protocol": "tcp", "from_port": 80, "to_port": 80, "cidr_blocks": ["0.0.0.0/0"], "description": "HTTP"}, {"protocol": "tcp", "from_port": 443, "to_port": 443, "cidr_blocks": ["0.0.0.0/0"], "description": "HTTPS"}], "egress": [{"protocol": "-1", "from_port": 0, "to_port": 0, "cidr_blocks": ["0.0.0.0/0"], "description": "All outbound"}], "tags": {}}}},
                {"id": "ai-ec2-1", "type": "service", "position": {"x": 250, "y": 400}, "data": {"label": "EC2", "type": "compute", "provider": "aws", "rawLabel": "aws_instance", "serviceName": "Web Server", "properties": {"name": "web-server", "ami": DEFAULT_AMI, "instance_type": "t3.medium", "refs": {"subnet": "ai-subnet-1", "security_group": "ai-sg-1"}, "tags": {}}}},
                {"id": "ai-rds-1", "type": "service", "position": {"x": 450, "y": 400}, "data": {"label": "RDS", "type": "database", "provider": "aws", "rawLabel": "rds_instance", "serviceName": "App DB", "properties": {"name": "app-db", "engine": "postgresql", "instance_class": "db.t3.medium", "allocated_storage": 20, "refs": {}, "tags": {}}}},
            ],
            "edges": [
                {"id": "ai-e1", "source": "ai-ec2-1", "target": "ai-rds-1"},
                {"id": "ai-e2", "source": "ai-ec2-1", "target": "ai-sg-1"},
            ],
        }

    if "vpc" in p or "network" in p or "subnet" in p:
        return {
            "summary": "VPC with public/private subnets and internet gateway.",
            "nodes": [
                {"id": "ai-vpc-1", "type": "service", "position": {"x": 250, "y": 50}, "data": {"label": "VPC", "type": "network", "provider": "aws", "rawLabel": "vpc", "serviceName": "Main VPC", "properties": {"name": "main-vpc", "cidr_block": "10.0.0.0/16", "enable_dns_support": True, "enable_dns_hostname": True, "refs": {}, "tags": {}}}},
                {"id": "ai-igw-1", "type": "service", "position": {"x": 100, "y": 200}, "data": {"label": "InternetGateway", "type": "InternetGateway", "provider": "aws", "rawLabel": "internet_gateway", "serviceName": "Main IGW", "properties": {"name": "main-igw", "refs": {"vpc": "ai-vpc-1"}, "tags": {}}}},
                {"id": "ai-subnet-1", "type": "service", "position": {"x": 250, "y": 350}, "data": {"label": "Subnet", "type": "Subnet", "provider": "aws", "rawLabel": "subnet", "serviceName": "Public Subnet", "properties": {"name": "public-subnet", "cidr_block": "10.0.1.0/24", "availability_zone": "eu-north-1a", "map_public_ip_on_launch": True, "refs": {"vpc": "ai-vpc-1"}, "tags": {}}}},
                {"id": "ai-subnet-2", "type": "service", "position": {"x": 450, "y": 350}, "data": {"label": "Subnet", "type": "Subnet", "provider": "aws", "rawLabel": "subnet", "serviceName": "Private Subnet", "properties": {"name": "private-subnet", "cidr_block": "10.0.2.0/24", "availability_zone": "eu-north-1b", "map_public_ip_on_launch": False, "refs": {"vpc": "ai-vpc-1"}, "tags": {}}}},
            ],
            "edges": [
                {"id": "ai-e1", "source": "ai-igw-1", "target": "ai-vpc-1"},
            ],
        }

    # Default: simple EC2 setup
    return {
        "summary": "Basic EC2 setup with VPC, subnet, and security group.",
        "nodes": [
            {"id": "ai-vpc-1", "type": "service", "position": {"x": 200, "y": 50}, "data": {"label": "VPC", "type": "network", "provider": "aws", "rawLabel": "vpc", "serviceName": "Main VPC", "properties": {"name": "main-vpc", "cidr_block": "10.0.0.0/16", "enable_dns_support": True, "enable_dns_hostname": True, "refs": {}, "tags": {}}}},
            {"id": "ai-subnet-1", "type": "service", "position": {"x": 200, "y": 200}, "data": {"label": "Subnet", "type": "Subnet", "provider": "aws", "rawLabel": "subnet", "serviceName": "Public Subnet", "properties": {"name": "public-subnet", "cidr_block": "10.0.1.0/24", "availability_zone": "eu-north-1a", "map_public_ip_on_launch": True, "refs": {"vpc": "ai-vpc-1"}, "tags": {}}}},
            {"id": "ai-sg-1", "type": "service", "position": {"x": 400, "y": 200}, "data": {"label": "SecurityGroup", "type": "SecurityGroup", "provider": "aws", "rawLabel": "security_group", "serviceName": "App SG", "properties": {"name": "app-sg", "description": "Allow SSH and HTTP", "refs": {"vpc": "ai-vpc-1"}, "ingress": [{"protocol": "tcp", "from_port": 22, "to_port": 22, "cidr_blocks": ["0.0.0.0/0"], "description": "SSH"}, {"protocol": "tcp", "from_port": 80, "to_port": 80, "cidr_blocks": ["0.0.0.0/0"], "description": "HTTP"}], "egress": [{"protocol": "-1", "from_port": 0, "to_port": 0, "cidr_blocks": ["0.0.0.0/0"], "description": "All outbound"}], "tags": {}}}},
            {"id": "ai-ec2-1", "type": "service", "position": {"x": 200, "y": 400}, "data": {"label": "EC2", "type": "compute", "provider": "aws", "rawLabel": "aws_instance", "serviceName": "App Server", "properties": {"name": "app-server", "ami": DEFAULT_AMI, "instance_type": "t3.micro", "refs": {"subnet": "ai-subnet-1", "security_group": "ai-sg-1"}, "tags": {}}}},
        ],
        "edges": [
            {"id": "ai-e1", "source": "ai-ec2-1", "target": "ai-sg-1"},
        ],
    }
