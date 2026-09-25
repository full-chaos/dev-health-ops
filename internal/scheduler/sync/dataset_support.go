package sync

// SupportsDataset is sync/datasets.py: get_dataset_spec(provider, key) is not
// None: whether the provider's dataset registry holds the key. The api's
// integration dataset admin route creates a missing dataset row only for a
// registered key.
func SupportsDataset(provider, dataset string) bool {
	_, ok := datasetSpecification(provider, dataset)
	return ok
}
