# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
"""Bootstraps the resources required to run the MWAA Serverless integration tests.

Creates:
- IAM execution role (trusted by the MWAA Serverless service principal(s)) that
  the workflow assumes at execution time.
- S3 bucket (versioning enabled) holding the workflow definition YAML, which is
  uploaded after bucket creation (the generic Bucket bootstrapper can only
  pre-create zero-byte objects, which would fail CreateWorkflow validation).

MWAA Serverless is serverless, so no VPC bootstrap is needed (unlike classic
MWAA).
"""

import json
import logging

import boto3

from acktest.bootstrapping import Resources, BootstrapFailureException
from acktest.bootstrapping.iam import Role, UserPolicies
from acktest.bootstrapping.s3 import Bucket
from e2e import bootstrap_directory
from e2e.bootstrap_resources import BootstrapResources

# S3 object key under which the workflow definition YAML is uploaded.
DEFINITION_OBJECT_KEY = "workflows/example.yaml"

# Minimal MWAA Serverless workflow definition (a dag-factory-style YAML DAG).
#
# MWAA Serverless workflow definitions are YAML DAGs (NOT Python) in the
# dag-factory format: a top-level <dag_id> key mapping to a `tasks` mapping
# (task name -> operator/dependencies). This body was validated against the
# live MWAA Serverless CreateWorkflow API.
WORKFLOW_DEFINITION_BODY = """\
ack_example_workflow:
  default_args: { owner: airflow }
  schedule_interval: null
  catchup: false
  tasks:
    start:  { operator: airflow.operators.empty.EmptyOperator }
    finish: { operator: airflow.operators.empty.EmptyOperator, dependencies: [start] }
"""


def service_bootstrap() -> Resources:
    logging.getLogger().setLevel(logging.INFO)

    # Execution policy: per the MWAA Serverless "Execution roles" user guide the
    # role needs S3 access to the definition bucket, CloudWatch Logs write
    # access (logs:CreateLogStream, logs:PutLogEvents), and KMS access
    # (Encrypt/Decrypt/GenerateDataKey/DescribeKey). The previously included
    # `airflow:PublishMetrics` / `airflow-serverless:PublishMetrics` actions
    # were guessed and are not documented MWAA Serverless execution-role
    # actions, so they have been removed.
    execution_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject*",
                    "s3:GetBucket*",
                    "s3:List*",
                ],
                "Resource": "*",
            },
            {
                # Documented CloudWatch Logs actions for the execution role.
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "*",
            },
            {
                # Documented KMS actions for the execution role.
                "Effect": "Allow",
                "Action": [
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:GenerateDataKey",
                    "kms:DescribeKey",
                ],
                "Resource": "*",
            },
        ],
    })

    # The MWAA Serverless execution-role trust policy requires exactly ONE
    # service principal: `airflow-serverless.amazonaws.com` (per the MWAA
    # Serverless "Execution roles" user guide). The classic MWAA principals
    # (airflow.amazonaws.com / airflow-env.amazonaws.com) do NOT apply here, so
    # no additional/fallback principals are added.
    resources = BootstrapResources(
        ExecutionRole=Role(
            name_prefix="ack-mwaaserverless-execution-role",
            principal_service="airflow-serverless.amazonaws.com",
            user_policies=UserPolicies(
                "ack-mwaaserverless-execution-policy", [execution_policy],
            ),
        ),
        # Versioning is enabled so the definition object has a versionID that
        # can be referenced by spec.definitionS3Location.versionID if needed.
        DefinitionBucket=Bucket(
            name_prefix="ack-mwaaserverless-definitions",
            enable_versioning=True,
        ),
    )

    try:
        resources.bootstrap()
    except BootstrapFailureException:
        exit(254)

    # Upload the actual workflow definition body. `empty_objects` only creates
    # zero-byte objects, so we write a real, non-empty definition here so that
    # CreateWorkflow's definition validation can succeed.
    s3_client = boto3.client("s3", region_name=resources.DefinitionBucket.region)
    s3_client.put_object(
        Bucket=resources.DefinitionBucket.name,
        Key=DEFINITION_OBJECT_KEY,
        Body=WORKFLOW_DEFINITION_BODY.encode("utf-8"),
    )
    logging.info(
        f"Uploaded workflow definition to "
        f"s3://{resources.DefinitionBucket.name}/{DEFINITION_OBJECT_KEY}"
    )

    return resources


if __name__ == "__main__":
    config = service_bootstrap()
    # Write config to current directory by default
    config.serialize(bootstrap_directory)
