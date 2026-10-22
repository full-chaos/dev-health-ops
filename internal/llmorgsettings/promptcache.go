package llmorgsettings

// KeyPromptCache is the llm settings row an org admin writes to opt the org's
// saved BYO configuration in to the Anthropic prompt cache ("true"). No row, or
// any other value, reads as off.
const KeyPromptCache = "prompt_cache"

// promptCacheOptIn is whether the org opted in AND its effective provider is
// anthropic: the cache marker exists on that provider only.
func promptCacheOptIn(settings orgSettings, provider string) bool {
	return settings[KeyPromptCache] == "true" && provider == "anthropic"
}
