package fixture

func providerName(add func(string, string)) {
	add("git", "commit")
	_ = map[string]string{"git": "commit"}
}
