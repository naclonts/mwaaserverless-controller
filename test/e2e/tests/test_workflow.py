# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the MWAA Serverless Workflow resource.

MWAA Serverless workflow create/update/delete are fast (the full lifecycle
runs in ~4 min in e2e), so a single modest timeout ceiling covers every op.

Each test provisions a uniquely-named workflow and cleans it up, so they are
safe to run concurrently under pytest-xdist (`-n auto`).

Marked @pytest.mark.canary so the kind-e2e canary runs them.
"""

import datetime
import logging
import time

import pytest

from acktest.k8s import resource as k8s
from acktest.k8s import condition
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_mwaaserverless_resource
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "workflows"

# A single ceiling for every workflow lifecycle op (create/update/delete). The
# full create->update->delete cycle completes in ~4 min in e2e, so 8 min gives
# generous headroom for any one op without masking a genuinely stuck reconcile.
TIMEOUT_SECONDS = 60 * 8

POLL_INTERVAL_SECONDS = 30

# Time to let the controller reconcile after a k8s operation
RECONCILE_WAIT_SECONDS = 30

# READY is the synced/available state for an MWAA Serverless workflow; DELETING
# is the transient state while it is being torn down.
WORKFLOW_STATUS_READY = "READY"


def wait_for_cr_status(ref, target_status, timeout_seconds):
    """Poll the CR until status.status matches target_status."""
    deadline = datetime.datetime.now() + datetime.timedelta(seconds=timeout_seconds)
    while datetime.datetime.now() < deadline:
        try:
            cr = k8s.get_resource(ref)
            status = cr.get("status", {}).get("status")
            logging.info(f"CR {ref.name} status: {status}")
            if status == target_status:
                return cr
        except Exception as e:
            logging.warning(f"Transient error getting CR {ref.name}: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"Timed out waiting for CR status to reach {target_status}")


def get_workflow_arn(ref):
    """Read the server-assigned WorkflowArn from the CR's ackResourceMetadata.

    GetWorkflow/ListTagsForResource are keyed by the WorkflowArn (not the
    workflow Name), so the ARN must be read from
    status.ackResourceMetadata.arn.
    """
    cr = k8s.get_resource(ref)
    arn = (cr or {}).get("status", {}).get("ackResourceMetadata", {}).get("arn")
    assert arn, "status.ackResourceMetadata.arn should be set on the CR"
    return arn


def wait_for_workflow_status(mwaaserverless_client, workflow_arn, target_status,
                             timeout_seconds, cr_ref=None):
    """Poll the MWAA Serverless API until the workflow reaches target_status.

    If ``cr_ref`` is provided, also inspect the CR's conditions on each
    iteration and fast-fail if the controller has set ACK.Terminal=True.
    Without this, a terminal error surfaced only on the CR (e.g. a
    ValidationException that prevents the CreateWorkflow call from ever
    reaching AWS) would be invisible to this poll loop. Pass ``cr_ref=None``
    when polling for deletion because the CR is intentionally being torn down.
    """
    deadline = datetime.datetime.now() + datetime.timedelta(seconds=timeout_seconds)
    while datetime.datetime.now() < deadline:
        # Fail fast if the controller marked the CR terminal.
        if cr_ref is not None:
            try:
                cr = k8s.get_resource(cr_ref)
                for cond in (cr or {}).get("status", {}).get("conditions", []) or []:
                    if (cond.get("type") == condition.CONDITION_TYPE_TERMINAL
                            and cond.get("status") == "True"):
                        pytest.fail(
                            f"Controller set {condition.CONDITION_TYPE_TERMINAL} on "
                            f"{cr_ref.name}: {cond.get('message')}"
                        )
            except Exception as e:
                # CR read failure is transient; keep polling.
                logging.warning(f"Transient error reading CR {cr_ref.name}: {e}")

        try:
            resp = mwaaserverless_client.get_workflow(WorkflowArn=workflow_arn)
            # GetWorkflow returns the status under "WorkflowStatus", not "Status"
            # (the GetWorkflowResponse member is workflow_status/WorkflowStatus).
            status = resp.get("WorkflowStatus")
            logging.info(f"Workflow {workflow_arn}: {status}")
            if status == target_status:
                return status
        except mwaaserverless_client.exceptions.ResourceNotFoundException:
            if target_status == "DELETED":
                return "DELETED"
            # Controller may not have issued CreateWorkflow yet, or the API is
            # eventually consistent. Keep polling until timeout.
            logging.info(
                f"Workflow {workflow_arn} not yet visible in AWS; continuing to poll"
            )
        time.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"Timed out waiting for workflow {workflow_arn} to reach {target_status}")


@service_marker
@pytest.mark.canary
def test_workflow_lifecycle(mwaaserverless_client):
    """End-to-end create -> update -> delete for an MWAA Serverless Workflow."""
    workflow_name = random_suffix_name("ack-mwaaserverless", 32)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["WORKFLOW_NAME"] = workflow_name

    resource_data = load_mwaaserverless_resource(
        "workflow",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        workflow_name, namespace="default",
    )

    # --- Create ---
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)
    assert cr is not None
    assert k8s.get_resource_exists(ref)

    try:
        # Wait for READY on the CR first; the ARN is only available once the
        # controller has created the resource and populated status.
        cr = wait_for_cr_status(ref, WORKFLOW_STATUS_READY, TIMEOUT_SECONDS)

        workflow_arn = get_workflow_arn(ref)

        # Verify the workflow is READY in AWS, keyed by ARN.
        wait_for_workflow_status(
            mwaaserverless_client, workflow_arn, WORKFLOW_STATUS_READY,
            TIMEOUT_SECONDS, cr_ref=ref,
        )

        # Verify Synced condition.
        assert k8s.wait_on_condition(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True", wait_periods=10,
        )

        # Verify status fields populated on the CR.
        assert "arn" in cr["status"]["ackResourceMetadata"], \
            "ackResourceMetadata.arn should be set after READY"
        assert "version" in cr["status"], \
            "version should be set after READY"
        assert "createdAt" in cr["status"], \
            "createdAt should be set after READY"

        # --- No-op stability ---
        # With no spec change, the controller must NOT issue an UpdateWorkflow.
        # A read-path that writes a server-defaulted/omitted field back onto a
        # nil Spec field would create phantom drift, and because UpdateWorkflow
        # mints a new WorkflowVersion, that would show up as the version
        # advancing on every resync with no user change. Capture the version
        # after the initial READY, let the controller run several resync
        # periods, and assert it is unchanged.
        initial_version = cr["status"]["version"]
        time.sleep(RECONCILE_WAIT_SECONDS * 3)
        stable_cr = k8s.get_resource(ref)
        assert stable_cr["status"]["version"] == initial_version, (
            "version advanced with no spec change: "
            f"{initial_version} -> {stable_cr['status']['version']}; "
            "the read path is manufacturing phantom drift and churning "
            "UpdateWorkflow"
        )

        # --- Update ---
        # Patch the description and tags to exercise both the update path and
        # the tag-sync path (TagResource/UntagResource).
        new_description = "ACK e2e test workflow (updated)"
        new_tags = {"ack-e2e": "updated"}
        updates = {
            "spec": {
                "description": new_description,
                "tags": new_tags,
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(RECONCILE_WAIT_SECONDS)

        # Wait for the workflow to settle back to READY after the update.
        wait_for_workflow_status(
            mwaaserverless_client, workflow_arn, WORKFLOW_STATUS_READY,
            TIMEOUT_SECONDS, cr_ref=ref,
        )
        wait_for_cr_status(ref, WORKFLOW_STATUS_READY, TIMEOUT_SECONDS)

        # Verify Synced after update.
        assert k8s.wait_on_condition(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True", wait_periods=10,
        )

        # Verify the description changed in AWS (GetWorkflow is ARN-keyed).
        # GetWorkflow returns the description at resp["Description"].
        aws_res = mwaaserverless_client.get_workflow(WorkflowArn=workflow_arn)
        assert aws_res.get("Description") == new_description

        # Verify the tags changed in AWS. GetWorkflow does NOT return tags
        # inline, so list_tags_for_resource (ARN-keyed) is used; it returns the
        # tags at resp["Tags"] as a flat key->value map.
        tags_res = mwaaserverless_client.list_tags_for_resource(ResourceArn=workflow_arn)
        assert tags_res.get("Tags", {}).get("ack-e2e") == "updated"

    finally:
        # --- Delete (teardown) ---
        # Best-effort: don't let a teardown error mask a test-body failure, but
        # still log it and wait for the workflow to actually disappear so we
        # don't leak AWS resources.
        try:
            workflow_arn = get_workflow_arn(ref)
        except Exception:
            workflow_arn = None
        try:
            _, deleted = k8s.delete_custom_resource(ref, 3, 10)
            if not deleted:
                logging.warning(
                    f"delete_custom_resource returned False for {workflow_name}"
                )
            if workflow_arn is not None:
                # Verify the workflow is gone from AWS (GetWorkflow eventually
                # raises ResourceNotFoundException).
                wait_for_workflow_status(
                    mwaaserverless_client, workflow_arn, "DELETED",
                    TIMEOUT_SECONDS,
                )
        except Exception as e:
            logging.error(f"Teardown failed for {workflow_name}: {e}")
