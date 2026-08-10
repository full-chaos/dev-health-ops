package providersync

// isDerivedWorkItemProvider is the bounded provider set that may use the
// provider-neutral work-item derivation builders. Collection and raw effect
// routes remain provider-owned; this helper only widens the already shared
// derived semantics to Linear.
func isDerivedWorkItemProvider(provider string) bool {
	return provider == "github" || provider == "linear"
}
