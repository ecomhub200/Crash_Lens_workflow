#!/usr/bin/env python3
"""
Deploy Chronos-2 model to Amazon SageMaker for crash prediction.

This script creates a SageMaker endpoint running the Chronos-2 time series
forecasting model. The endpoint accepts JSON payloads with historical crash
time series and returns probabilistic forecasts.

Supports two deployment modes:
  - **serverless** (default): Scales to zero when idle — no charges when not
    in use. Pay only per-request. Recommended for batch/periodic forecast runs.
  - **realtime**: Always-on instance — charges ~$0.23/hr even when idle.
    Use only when continuous low-latency inference is needed.

Usage:
    python scripts/deploy_chronos_endpoint.py --action deploy                    # serverless (default)
    python scripts/deploy_chronos_endpoint.py --action deploy --mode realtime    # always-on instance
    python scripts/deploy_chronos_endpoint.py --action status
    python scripts/deploy_chronos_endpoint.py --action delete

Environment Variables (or GitHub Secrets):
    AWS_ACCESS_KEY_ID       - IAM user access key
    AWS_SECRET_ACCESS_KEY   - IAM user secret key
    AWS_REGION              - AWS region (default: us-east-1)
"""

import argparse
import json
import os
import sys
import time

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("ERROR: boto3 is required. Install with: pip install boto3")
    sys.exit(1)

# ============================================================
# Configuration
# ============================================================

ENDPOINT_NAME = "crashlens-chronos2-endpoint"
MODEL_NAME = "crashlens-chronos2-model"
ENDPOINT_CONFIG_NAME = "crashlens-chronos2-config"

# Instance types to try, in order of preference.
# New AWS accounts often have 0 quota for many instance types.
# We try multiple types to find one that works.
INSTANCE_TYPES = [
    "ml.m5.xlarge",   # 4 vCPU, 16 GB — best for Chronos-2
    "ml.m5.large",    # 2 vCPU, 8 GB — sufficient, cheaper
    "ml.c5.xlarge",   # 4 vCPU, 8 GB — compute-optimized
    "ml.c5.large",    # 2 vCPU, 4 GB — minimal compute
    "ml.m4.xlarge",   # 4 vCPU, 16 GB — older generation
]
INSTANCE_TYPE = os.environ.get("SAGEMAKER_INSTANCE_TYPE", INSTANCE_TYPES[0])
INITIAL_INSTANCE_COUNT = 1

# Chronos-2 model from HuggingFace via SageMaker JumpStart
MODEL_ID = "pytorch-forecasting-chronos-2"

# Serverless inference configuration (scales to zero when idle)
SERVERLESS_MEMORY_SIZE_MB = 4096  # 4 GB — sufficient for Chronos-2
SERVERLESS_MAX_CONCURRENCY = 5    # Max concurrent invocations

# Timeout settings
DEPLOY_WAIT_MINUTES = 15
POLL_INTERVAL_SECONDS = 30


def get_boto_session():
    """Create boto3 session from environment variables."""
    region = os.environ.get("AWS_REGION", "us-east-1")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        print("ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set.")
        print("Set them as environment variables or GitHub repository secrets.")
        sys.exit(1)

    return boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def deploy_endpoint(session, mode="serverless"):
    """Deploy Chronos-2 model to SageMaker endpoint.

    Args:
        session: boto3 Session
        mode: 'serverless' (scales to zero, pay-per-request) or
              'realtime' (always-on instance, charges ~$0.23/hr idle)
    """
    region = session.region_name
    sm_client = session.client("sagemaker")
    mode_label = "SERVERLESS (scales to zero)" if mode == "serverless" else f"REALTIME ({INSTANCE_TYPE})"
    print(f"\n{'='*60}")
    print(f"  Deploying Chronos-2 to SageMaker")
    print(f"  Region: {region}")
    print(f"  Endpoint: {ENDPOINT_NAME}")
    print(f"  Mode: {mode_label}")
    print(f"{'='*60}\n")

    # Check if endpoint already exists
    try:
        status = sm_client.describe_endpoint(EndpointName=ENDPOINT_NAME)
        current_status = status["EndpointStatus"]
        if current_status == "InService":
            print(f"Endpoint '{ENDPOINT_NAME}' is already InService.")
            print("Use --action delete first if you want to redeploy.")
            return True
        elif current_status in ("Creating", "Updating"):
            print(f"Endpoint is currently {current_status}. Wait for it to finish.")
            return wait_for_endpoint(sm_client)
        else:
            print(f"Endpoint exists with status: {current_status}. Cleaning up...")
            delete_endpoint(session)
            time.sleep(10)
    except ClientError as e:
        if "Could not find endpoint" in str(e):
            print("No existing endpoint found. Creating new one...")
        else:
            raise

    # Clean up any orphaned endpoint configs/models from previous failed deploys.
    # JumpStart uses the endpoint name as the config name, so clean both variants.
    _cleanup_stale_resources(sm_client)

    # Try JumpStart deployment
    try:
        from sagemaker.jumpstart.model import JumpStartModel
        from sagemaker import Session as SageMakerSession
    except ImportError as e:
        print(f"ERROR: sagemaker JumpStart SDK not available: {e}")
        print("Install sagemaker v2: pip install \"sagemaker>=2.200,<3\"")
        return False

    sm_session = SageMakerSession(boto_session=session)

    # Get or create IAM execution role for SageMaker
    # (IAM users can't be used as execution roles — must be an IAM role)
    iam_client = session.client("iam")
    role_arn = get_or_create_sagemaker_role(iam_client)
    print(f"  Using execution role: {role_arn}")

    # ── Serverless deployment (scales to zero — no idle charges) ──
    if mode == "serverless":
        return _deploy_serverless(sm_client, sm_session, role_arn)

    # ── Realtime deployment (always-on instance) ──
    return _deploy_realtime(sm_client, sm_session, role_arn)


def _deploy_serverless(sm_client, sm_session, role_arn):
    """Deploy Chronos-2 as a serverless endpoint (scales to zero)."""
    from sagemaker.jumpstart.model import JumpStartModel
    from sagemaker.serverless import ServerlessInferenceConfig

    print(f"\n[1/3] Loading JumpStart model: {MODEL_ID} (serverless)...")
    print(f"  Memory: {SERVERLESS_MEMORY_SIZE_MB} MB")
    print(f"  Max concurrency: {SERVERLESS_MAX_CONCURRENCY}")

    serverless_config = ServerlessInferenceConfig(
        memory_size_in_mb=SERVERLESS_MEMORY_SIZE_MB,
        max_concurrency=SERVERLESS_MAX_CONCURRENCY,
    )

    try:
        model = JumpStartModel(
            model_id=MODEL_ID,
            role=role_arn,
            sagemaker_session=sm_session,
        )

        print(f"[2/3] Deploying serverless endpoint: {ENDPOINT_NAME}...")
        predictor = model.deploy(
            endpoint_name=ENDPOINT_NAME,
            serverless_inference_config=serverless_config,
            wait=False,
        )

        print(f"[3/3] Serverless deployment initiated. Waiting for InService...")
        return wait_for_endpoint(sm_client)

    except Exception as e:
        error_msg = str(e)
        if "Cannot create already existing" in error_msg:
            print(f"  Stale resource detected. Cleaning up and retrying...")
            _cleanup_partial_deploy(sm_client)
            time.sleep(5)
            try:
                model = JumpStartModel(
                    model_id=MODEL_ID,
                    role=role_arn,
                    sagemaker_session=sm_session,
                )
                predictor = model.deploy(
                    endpoint_name=ENDPOINT_NAME,
                    serverless_inference_config=serverless_config,
                    wait=False,
                )
                print(f"[3/3] Serverless deployment initiated. Waiting for InService...")
                return wait_for_endpoint(sm_client)
            except Exception as retry_e:
                print(f"  Retry also failed: {retry_e}")
                return False
        else:
            print(f"  Serverless deployment failed: {e}")
            return False


def _deploy_realtime(sm_client, sm_session, role_arn):
    """Deploy Chronos-2 as a realtime endpoint (always-on instance)."""
    from sagemaker.jumpstart.model import JumpStartModel

    # Build list of instance types to try
    if os.environ.get("SAGEMAKER_INSTANCE_TYPE"):
        instance_types_to_try = [os.environ["SAGEMAKER_INSTANCE_TYPE"]]
    else:
        instance_types_to_try = INSTANCE_TYPES

    for idx, instance_type in enumerate(instance_types_to_try):
        print(f"\n[1/3] Loading JumpStart model: {MODEL_ID} (instance: {instance_type})...")

        try:
            model = JumpStartModel(
                model_id=MODEL_ID,
                role=role_arn,
                instance_type=instance_type,
                sagemaker_session=sm_session,
            )

            print(f"[2/3] Deploying to endpoint: {ENDPOINT_NAME}...")
            predictor = model.deploy(
                endpoint_name=ENDPOINT_NAME,
                initial_instance_count=INITIAL_INSTANCE_COUNT,
                wait=False,
            )

            print(f"[3/3] Deployment initiated with {instance_type}. Waiting for InService...")
            return wait_for_endpoint(sm_client)

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            error_msg = str(e)

            if "ResourceLimitExceeded" in error_msg or error_code == "ResourceLimitExceeded":
                print(f"  QUOTA LIMIT: {instance_type} has 0 quota in this account/region.")
                if idx < len(instance_types_to_try) - 1:
                    print(f"  Trying next instance type...")
                    _cleanup_partial_deploy(sm_client)
                    continue
                else:
                    print(f"\n  ERROR: All instance types exhausted. No quota available.")
                    print(f"  You must request a quota increase in AWS Service Quotas:")
                    print(f"    1. Go to https://console.aws.amazon.com/servicequotas/")
                    print(f"    2. Search for 'Amazon SageMaker'")
                    print(f"    3. Request increase for 'ml.m5.xlarge for endpoint usage'")
                    print(f"    4. Request at least 1 instance")
                    return False
            elif "Cannot create already existing" in error_msg:
                print(f"  Stale resource detected. Cleaning up and retrying...")
                _cleanup_partial_deploy(sm_client)
                time.sleep(5)
                try:
                    model = JumpStartModel(
                        model_id=MODEL_ID,
                        role=role_arn,
                        instance_type=instance_type,
                        sagemaker_session=sm_session,
                    )
                    predictor = model.deploy(
                        endpoint_name=ENDPOINT_NAME,
                        initial_instance_count=INITIAL_INSTANCE_COUNT,
                        wait=False,
                    )
                    print(f"[3/3] Deployment initiated with {instance_type}. Waiting for InService...")
                    return wait_for_endpoint(sm_client)
                except Exception as retry_e:
                    print(f"  Retry also failed: {retry_e}")
                    return False
            else:
                print(f"  Deployment failed: {e}")
                return False
        except Exception as e:
            error_msg = str(e)
            if "Cannot create already existing" in error_msg:
                print(f"  Stale resource detected. Cleaning up and retrying...")
                _cleanup_partial_deploy(sm_client)
                time.sleep(5)
                try:
                    model = JumpStartModel(
                        model_id=MODEL_ID,
                        role=role_arn,
                        instance_type=instance_type,
                        sagemaker_session=sm_session,
                    )
                    predictor = model.deploy(
                        endpoint_name=ENDPOINT_NAME,
                        initial_instance_count=INITIAL_INSTANCE_COUNT,
                        wait=False,
                    )
                    print(f"[3/3] Deployment initiated with {instance_type}. Waiting for InService...")
                    return wait_for_endpoint(sm_client)
                except Exception as retry_e:
                    print(f"  Retry also failed: {retry_e}")
                    return False
            else:
                print(f"  Deployment failed: {e}")
                return False

    return False


def _cleanup_stale_resources(sm_client):
    """Clean up orphaned endpoint configs and models from previous failed deploys.

    JumpStart uses the endpoint name as the endpoint config name, which differs
    from our ENDPOINT_CONFIG_NAME constant. We must clean up both variants.
    """
    # Config names that JumpStart or our script may have created
    config_names = list(set([ENDPOINT_CONFIG_NAME, ENDPOINT_NAME]))
    for config_name in config_names:
        try:
            sm_client.describe_endpoint_config(EndpointConfigName=config_name)
            print(f"  Deleting orphaned endpoint config: {config_name}")
            sm_client.delete_endpoint_config(EndpointConfigName=config_name)
        except ClientError:
            pass  # Doesn't exist, that's fine

    # Also clean up any stale model objects (JumpStart auto-names may vary)
    try:
        sm_client.describe_model(ModelName=MODEL_NAME)
        print(f"  Deleting orphaned model: {MODEL_NAME}")
        sm_client.delete_model(ModelName=MODEL_NAME)
    except ClientError:
        pass

    # JumpStart may also create models with auto-generated names containing the endpoint name
    try:
        models = sm_client.list_models(NameContains="crashlens-chronos2", MaxResults=10)
        for m in models.get("Models", []):
            name = m["ModelName"]
            print(f"  Deleting stale model: {name}")
            sm_client.delete_model(ModelName=name)
    except ClientError:
        pass

    time.sleep(2)


def _cleanup_partial_deploy(sm_client):
    """Clean up partially created model/config resources before retrying with a different instance."""
    # Delete endpoint
    try:
        sm_client.delete_endpoint(EndpointName=ENDPOINT_NAME)
    except ClientError:
        pass

    # Delete endpoint configs (both our name and JumpStart's name which uses the endpoint name)
    for config_name in set([ENDPOINT_CONFIG_NAME, ENDPOINT_NAME]):
        try:
            sm_client.delete_endpoint_config(EndpointConfigName=config_name)
        except ClientError:
            pass

    # Delete any models matching our prefix
    try:
        models = sm_client.list_models(NameContains="crashlens-chronos2", MaxResults=10)
        for m in models.get("Models", []):
            try:
                sm_client.delete_model(ModelName=m["ModelName"])
            except ClientError:
                pass
    except ClientError:
        pass

    time.sleep(2)


def deploy_with_boto3(session):
    """Fallback deployment using raw boto3 (no SageMaker SDK).

    NOTE: Chronos-2 deployment requires the SageMaker JumpStart SDK (sagemaker v2)
    which automatically resolves container images and model artifacts. The boto3-only
    fallback cannot deploy Chronos-2 because the model artifacts and container URIs
    are managed internally by JumpStart. Install sagemaker v2:
        pip install "sagemaker>=2.200,<3"
    """
    print("ERROR: boto3-only deployment is not supported for Chronos-2.")
    print("Chronos-2 requires the SageMaker JumpStart SDK (sagemaker v2) which")
    print("automatically resolves container images and model artifact locations.")
    print("")
    print("Fix: Install sagemaker SDK v2:")
    print('  pip install "sagemaker>=2.200,<3"')
    print("")
    print("Then re-run with --action deploy.")
    return False


def get_or_create_sagemaker_role(iam_client):
    """Get or create a SageMaker execution role."""
    role_name = "CrashLensSageMakerRole"
    try:
        role = iam_client.get_role(RoleName=role_name)
        print(f"  Using existing role: {role_name}")
        return role["Role"]["Arn"]
    except ClientError:
        pass

    print(f"  Creating IAM role: {role_name}")
    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "sagemaker.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    })

    role = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=trust_policy,
        Description="Execution role for CrashLens Chronos-2 endpoint",
    )

    # Attach required policies
    for policy_arn in [
        "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess",
        "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    ]:
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

    # Wait for role propagation
    time.sleep(10)
    return role["Role"]["Arn"]


def wait_for_endpoint(sm_client):
    """Poll endpoint status until InService or failure."""
    max_polls = (DEPLOY_WAIT_MINUTES * 60) // POLL_INTERVAL_SECONDS
    for i in range(max_polls):
        try:
            resp = sm_client.describe_endpoint(EndpointName=ENDPOINT_NAME)
            status = resp["EndpointStatus"]
            elapsed = (i + 1) * POLL_INTERVAL_SECONDS
            print(f"  [{elapsed}s] Status: {status}")

            if status == "InService":
                print(f"\n  Endpoint is LIVE and ready for predictions!")
                print(f"  Endpoint Name: {ENDPOINT_NAME}")
                return True
            elif status in ("Failed", "RollbackComplete"):
                reason = resp.get("FailureReason", "Unknown")
                print(f"\n  Deployment FAILED: {reason}")
                return False
        except ClientError as e:
            print(f"  Poll error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)

    print(f"\n  Timed out after {DEPLOY_WAIT_MINUTES} minutes.")
    return False


def check_status(session):
    """Check the current status of the endpoint."""
    sm_client = session.client("sagemaker")
    try:
        resp = sm_client.describe_endpoint(EndpointName=ENDPOINT_NAME)
        status = resp["EndpointStatus"]
        creation = resp.get("CreationTime", "N/A")
        last_mod = resp.get("LastModifiedTime", "N/A")

        print(f"\n{'='*60}")
        print(f"  Endpoint: {ENDPOINT_NAME}")
        print(f"  Status:   {status}")
        print(f"  Created:  {creation}")
        print(f"  Modified: {last_mod}")
        print(f"{'='*60}\n")

        if status == "InService":
            # Test with a minimal payload
            print("Testing endpoint with sample data...")
            runtime = session.client("sagemaker-runtime")
            test_payload = {
                "inputs": [{"target": [100, 110, 105, 115, 108, 120, 112, 125]}],
                "parameters": {
                    "prediction_length": 3,
                    "quantile_levels": [0.1, 0.5, 0.9],
                },
            }
            try:
                response = runtime.invoke_endpoint(
                    EndpointName=ENDPOINT_NAME,
                    ContentType="application/json",
                    Body=json.dumps(test_payload),
                )
                result = json.loads(response["Body"].read().decode("utf-8"))
                print(f"  Test prediction successful!")
                print(f"  Sample output keys: {list(result.get('predictions', {}).keys())}")
            except Exception as e:
                print(f"  Test prediction failed: {e}")

        return status
    except ClientError as e:
        if "Could not find endpoint" in str(e):
            print(f"\nEndpoint '{ENDPOINT_NAME}' does not exist.")
            print("Run with --action deploy to create it.")
            return None
        raise


def delete_endpoint(session):
    """Delete the endpoint, config, and model."""
    sm_client = session.client("sagemaker")

    print(f"\nDeleting endpoint resources...")

    # Delete endpoint
    try:
        sm_client.delete_endpoint(EndpointName=ENDPOINT_NAME)
        print(f"  Deleted Endpoint: {ENDPOINT_NAME}")
    except ClientError as e:
        if "Could not find" in str(e) or "does not exist" in str(e):
            print(f"  Endpoint '{ENDPOINT_NAME}' not found (already deleted).")
        else:
            print(f"  Error deleting Endpoint: {e}")

    # Delete endpoint configs (both our name and JumpStart's auto-name)
    for config_name in set([ENDPOINT_CONFIG_NAME, ENDPOINT_NAME]):
        try:
            sm_client.delete_endpoint_config(EndpointConfigName=config_name)
            print(f"  Deleted Config: {config_name}")
        except ClientError as e:
            if "Could not find" in str(e) or "does not exist" in str(e):
                print(f"  Config '{config_name}' not found (already deleted).")
            else:
                print(f"  Error deleting Config: {e}")

    # Delete all models matching our prefix
    try:
        models = sm_client.list_models(NameContains="crashlens-chronos2", MaxResults=10)
        for m in models.get("Models", []):
            try:
                sm_client.delete_model(ModelName=m["ModelName"])
                print(f"  Deleted Model: {m['ModelName']}")
            except ClientError as e:
                print(f"  Error deleting Model {m['ModelName']}: {e}")
        if not models.get("Models"):
            print(f"  No models found matching 'crashlens-chronos2'.")
    except ClientError as e:
        print(f"  Error listing models: {e}")

    print("Cleanup complete.")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy Chronos-2 to SageMaker for crash prediction"
    )
    parser.add_argument(
        "--action",
        choices=["deploy", "status", "delete"],
        required=True,
        help="Action to perform",
    )
    parser.add_argument(
        "--mode",
        choices=["serverless", "realtime"],
        default="serverless",
        help="Deployment mode: 'serverless' scales to zero (no idle charges), "
             "'realtime' keeps an always-on instance (default: serverless)",
    )
    args = parser.parse_args()

    session = get_boto_session()

    if args.action == "deploy":
        success = deploy_endpoint(session, mode=args.mode)
        sys.exit(0 if success else 1)
    elif args.action == "status":
        check_status(session)
    elif args.action == "delete":
        delete_endpoint(session)


if __name__ == "__main__":
    main()
