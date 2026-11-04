package identity

// projectedLogAttrs are the attributes of the "legacy external identity projected" Info line.
func projectedLogAttrs(key ExternalKey, protocol string) []any {
	return []any{"provider", providerLabel(key.Provider)}
}
