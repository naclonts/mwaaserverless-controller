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

// Package tags contains reusable helpers for keeping a Workflow's AWS tags in
// sync with its desired state. The MWAA Serverless API exposes tags only via
// the separate TagResource/UntagResource/ListTagsForResource operations (there
// is no inline Tags member on Create/Get/Update), so tag reconciliation is
// implemented here rather than handled declaratively by the code-generator.
package tags

import (
	"context"

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"

	svcsdk "github.com/aws/aws-sdk-go-v2/service/mwaaserverless"
)

type metricsRecorder interface {
	RecordAPICall(opType string, opID string, err error)
}

type tagsClient interface {
	TagResource(context.Context, *svcsdk.TagResourceInput, ...func(*svcsdk.Options)) (*svcsdk.TagResourceOutput, error)
	UntagResource(context.Context, *svcsdk.UntagResourceInput, ...func(*svcsdk.Options)) (*svcsdk.UntagResourceOutput, error)
	ListTagsForResource(context.Context, *svcsdk.ListTagsForResourceInput, ...func(*svcsdk.Options)) (*svcsdk.ListTagsForResourceOutput, error)
}

// GetTags returns the tags associated with the resource identified by the
// supplied ARN. GetWorkflow does not return tags inline, so the read path
// calls this to populate Spec.Tags.
func GetTags(
	ctx context.Context,
	client tagsClient,
	mr metricsRecorder,
	resourceARN string,
) (tags map[string]string, err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("tags.GetTags")
	defer func() { exit(err) }()

	input := &svcsdk.ListTagsForResourceInput{
		ResourceArn: &resourceARN,
	}
	resp, err := client.ListTagsForResource(ctx, input)
	mr.RecordAPICall("GET", "ListTagsForResource", err)
	if err != nil {
		return nil, err
	}
	return resp.Tags, nil
}

// SyncTags calls TagResource and UntagResource to ensure the set of associated
// tags stays in sync with the desired state.
func SyncTags(
	ctx context.Context,
	client tagsClient,
	mr metricsRecorder,
	resourceARN string,
	desiredTags map[string]string,
	existingTags map[string]string,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("tags.SyncTags")
	defer func() { exit(err) }()

	toAdd := map[string]string{}
	toDelete := []string{}

	for k, v := range desiredTags {
		if ev, found := existingTags[k]; !found || ev != v {
			toAdd[k] = v
		}
	}
	for k := range existingTags {
		if _, found := desiredTags[k]; !found {
			toDelete = append(toDelete, k)
		}
	}

	if len(toDelete) > 0 {
		input := &svcsdk.UntagResourceInput{
			ResourceArn: &resourceARN,
			TagKeys:     toDelete,
		}
		_, err = client.UntagResource(ctx, input)
		mr.RecordAPICall("UPDATE", "UntagResource", err)
		if err != nil {
			return err
		}
	}

	if len(toAdd) > 0 {
		input := &svcsdk.TagResourceInput{
			ResourceArn: &resourceARN,
			Tags:        toAdd,
		}
		_, err = client.TagResource(ctx, input)
		mr.RecordAPICall("UPDATE", "TagResource", err)
		if err != nil {
			return err
		}
	}

	return nil
}
