	// DeleteWorkflow deletes only the specified version when WorkflowVersion is
	// set; leaving it unset deletes the workflow entirely. ACK owns the whole
	// workflow, so never scope the delete to a single version -- otherwise the
	// API returns success, ACK removes the finalizer and reports the CR
	// deleted, but the real workflow stays READY in AWS (resource leak).
	input.WorkflowVersion = nil
