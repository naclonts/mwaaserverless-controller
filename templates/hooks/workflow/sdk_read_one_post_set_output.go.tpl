	if ko.Status.ACKResourceMetadata != nil && ko.Status.ACKResourceMetadata.ARN != nil {
		tags, err := rm.getTags(ctx, string(*ko.Status.ACKResourceMetadata.ARN))
		if err != nil {
			return nil, err
		}
		ko.Spec.Tags = tags
	}
