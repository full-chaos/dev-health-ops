package workitemmetrics

// Helpers of the scope-view tests: they name the scope set the tests read.

func scopeViewRule(versions []ItemVersion, scopes []string) []int {
	for index := range versions {
		versions[index].Scope = scopes[index]
	}
	kept, _ := OncePerProviderScopeAndID(versions)
	return kept
}

func inScopeSet(item StoredItem, scope string) StoredItem {
	item.Scope = scope
	return item
}

func scopeAttributionKey(scope, workItemID string) string { return AttributionKey(scope, workItemID) }
