package providersync

// isDerivedWorkItemProvider is the bounded provider set that may use the
// provider-neutral work-item derivation builders and their shared ClickHouse
// validation seams. Collection and raw effect routes remain provider-owned;
// callers of the builders still pass an exact expected provider.
func isDerivedWorkItemProvider(provider string) bool {
	return provider == "github" || provider == "gitlab" || provider == "jira" || provider == "linear"
}
