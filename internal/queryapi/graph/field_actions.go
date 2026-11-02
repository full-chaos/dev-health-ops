package graph

// fieldActions maps every served GraphQL root field ("Query.name",
// "Mutation.name") to the catalog action a decision request for it names.
var fieldActions = map[string]string{}

// FieldActionTable returns a copy of the field action table.
func FieldActionTable() map[string]string {
	out := make(map[string]string, len(fieldActions))
	for key, action := range fieldActions {
		out[key] = action
	}
	return out
}
