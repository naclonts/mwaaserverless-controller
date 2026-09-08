	// Tags are managed via TagResource/UntagResource, not UpdateWorkflow. Sync
	// any tag drift here so the resource doesn't reconcile forever.
	if delta.DifferentAt("Spec.Tags") {
		if err = rm.syncTags(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	// If ONLY tags changed there is nothing for UpdateWorkflow to do, so
	// short-circuit and return the desired state.
	if !delta.DifferentExcept("Spec.Tags") {
		return desired, nil
	}
