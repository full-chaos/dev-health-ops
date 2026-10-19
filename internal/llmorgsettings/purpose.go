package llmorgsettings

// Purpose names one use of an org's BYO LLM. The org's unprefixed llm rows
// are the default; a purpose may carry its own provider/model/api_key/base_url
// override in the same settings category, under the key
// "purpose:<purpose>:<field>" (CHAOS-10039).
type Purpose string

const (
	PurposeInvestigate Purpose = "investigate"
	PurposeExplain     Purpose = "explain"
	PurposeCategorize  Purpose = "categorize"
)

// Purposes is the fixed list, in the order the admin API reports it.
var Purposes = []Purpose{PurposeInvestigate, PurposeExplain, PurposeCategorize}

// ParsePurpose accepts exactly the fixed names.
func ParsePurpose(name string) (Purpose, bool) {
	for _, purpose := range Purposes {
		if string(purpose) == name {
			return purpose, true
		}
	}
	return "", false
}

// Fields are the four stored fields of one BYO config, default or override.
var Fields = []string{keyProvider, keyModel, keyAPIKey, keyBaseURL}

// PurposeKey is the settings key of one override field.
func PurposeKey(purpose Purpose, field string) string {
	return "purpose:" + string(purpose) + ":" + field
}

// PurposeKeys are the stored keys of one purpose override.
func PurposeKeys(purpose Purpose) []string {
	keys := make([]string, len(Fields))
	for i, field := range Fields {
		keys[i] = PurposeKey(purpose, field)
	}
	return keys
}

// ForPurpose returns a Store whose every reader resolves for purpose: the
// purpose override when one is stored, else the default row. A Store with no
// purpose reads the default row only.
func (s Store) ForPurpose(purpose Purpose) Store {
	s.purpose = purpose
	return s
}

// overrideStored reports whether a purpose override is stored, judged by its
// provider key: an override is whole or absent, it never borrows a field
// (the api_key above all) from the default row.
func overrideStored(stored func(key string) bool, purpose Purpose) bool {
	return purpose != "" && stored(PurposeKey(purpose, keyProvider))
}

// inheritsDefaultKey: an override for the same provider as the default row
// and with no api_key of its own uses the default row's key, so one provider
// key serves several models. A different provider never inherits it.
func inheritsDefaultKey(stored func(key string) bool, value func(key string) string, purpose Purpose) bool {
	return overrideStored(stored, purpose) &&
		!stored(PurposeKey(purpose, keyAPIKey)) &&
		stored(keyAPIKey) &&
		normalizeProvider(value(keyProvider)) == normalizeProvider(value(PurposeKey(purpose, keyProvider)))
}

// sourceKeys are the stored keys the effective config of purpose reads.
func sourceKeys(stored func(key string) bool, value func(key string) string, purpose Purpose) []string {
	if !overrideStored(stored, purpose) {
		return Fields
	}
	keys := PurposeKeys(purpose)
	if inheritsDefaultKey(stored, value, purpose) {
		keys = append(keys, keyProvider, keyAPIKey)
	}
	return keys
}

// project maps the effective source of purpose onto the default key names, so
// every reader below it is unchanged.
func project(settings orgSettings, purpose Purpose) orgSettings {
	if purpose == "" {
		return settings
	}
	stored := func(key string) bool { return settings[key] != "" }
	value := func(key string) string { return settings[key] }
	if !overrideStored(stored, purpose) {
		return settings
	}
	out := orgSettings{}
	for _, field := range Fields {
		if v := settings[PurposeKey(purpose, field)]; v != "" {
			out[field] = v
		}
	}
	if inheritsDefaultKey(stored, value, purpose) {
		out[keyAPIKey] = settings[keyAPIKey]
	}
	return out
}
