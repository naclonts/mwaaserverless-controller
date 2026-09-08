// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package workflow

import (
	"context"

	svcsdktags "github.com/aws-controllers-k8s/mwaaserverless-controller/pkg/tags"
)

// getTags retrieves the tags for the Workflow identified by the supplied ARN.
//
// GetWorkflow does not return tags inline, and ACK does not automatically call
// ListTagsForResource, so the sdk_read_one_post_set_output hook invokes this to
// populate Spec.Tags. The MWAA Serverless API models tags as a flat
// map[string]string; the generated Spec.Tags field is map[string]*string, so we
// convert here. The ListTagsForResource call itself lives in pkg/tags so it can
// be reused if additional resources are added.
func (rm *resourceManager) getTags(
	ctx context.Context,
	resourceARN string,
) (map[string]*string, error) {
	tags, err := svcsdktags.GetTags(ctx, rm.sdkapi, rm.metrics, resourceARN)
	if err != nil {
		return nil, err
	}
	if len(tags) == 0 {
		return nil, nil
	}
	out := make(map[string]*string, len(tags))
	for k, v := range tags {
		v := v
		out[k] = &v
	}
	return out, nil
}

// syncTags keeps the AWS resource's tags in sync with Spec.Tags.
//
// Tags on a Workflow are managed exclusively via TagResource/UntagResource;
// UpdateWorkflow has no Tags member. The sdk_update_pre_build_request hook
// invokes this when delta.DifferentAt("Spec.Tags") so that tag drift is
// reconciled rather than looping forever. Spec.Tags is map[string]*string on
// the CRD while the SDK uses map[string]string, so we convert before delegating
// the Tag/Untag reconciliation to pkg/tags.
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) error {
	// The ARN is required for both Tag/Untag calls and is only available once
	// the resource exists.
	if latest.ko.Status.ACKResourceMetadata == nil ||
		latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return nil
	}
	resourceARN := string(*latest.ko.Status.ACKResourceMetadata.ARN)

	return svcsdktags.SyncTags(
		ctx,
		rm.sdkapi,
		rm.metrics,
		resourceARN,
		flattenTags(desired.ko.Spec.Tags),
		flattenTags(latest.ko.Spec.Tags),
	)
}

// flattenTags converts a CRD-style map[string]*string tag map into the flat
// map[string]string the MWAA Serverless SDK expects, dropping nil values.
func flattenTags(tags map[string]*string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		if v == nil {
			continue
		}
		out[k] = *v
	}
	return out
}
