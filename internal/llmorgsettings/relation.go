package llmorgsettings

// SettingsRelation is where a reader reads an org's settings rows from. It is a
// closed set: the SQL text of each member is a constant, never caller input.
type SettingsRelation int

const (
	// SettingsTable is the settings table itself (the api's roles hold SELECT on it).
	SettingsTable SettingsRelation = iota
	// CategorizeBYODefiner is the definer function of pgmigrate 0158: the heavy
	// worker's domain role holds EXECUTE on it and nothing on settings. It returns
	// the CategorizeBYODefinerKeys rows of an opted-in org and the budget limit row.
	CategorizeBYODefiner
)

// categorizeBYOSettingsFrom reads the function as a relation named settings, so
// every reader's column names and WHERE clauses stay as they are. Each reader
// binds the org id as $1.
const categorizeBYOSettingsFrom = "public.categorize_byo_settings($1) AS settings"

// From is the FROM item of the relation.
func (relation SettingsRelation) From() string {
	if relation == CategorizeBYODefiner {
		return categorizeBYOSettingsFrom
	}
	return "settings"
}

// CategorizeBYODefinerKeys are the llm keys the categorize Build path reads: the
// default and the categorize override configuration (byoKeys, PurposeKeys), the
// provider row of each purpose (HasSavedBYO), the own-model toggle and the
// prompt-cache opt-in (OrgConfig). The definer function returns these and no
// other llm key; a test pins its list to this one in both directions.
func CategorizeBYODefinerKeys() []string {
	keys := append([]string{}, Fields...)
	keys = append(keys, PurposeKeys(PurposeCategorize)...)
	for _, key := range savedProviderKeys() {
		if key != keyProvider {
			keys = append(keys, key)
		}
	}
	return append(keys, KeyOwnModelEnabled, KeyPromptCache)
}
