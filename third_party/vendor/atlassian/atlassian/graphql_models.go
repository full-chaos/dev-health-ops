package atlassian

type GraphQLRequest struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables,omitempty"`
	OperationName string         `json:"operationName,omitempty"`
}

type GraphQLError struct {
	Message    string           `json:"message"`
	Path       []any            `json:"path,omitempty"`
	Extensions map[string]any   `json:"extensions,omitempty"`
	Locations  []map[string]any `json:"locations,omitempty"`
}

type Result struct {
	Data       map[string]any `json:"data"`
	Errors     []GraphQLError `json:"errors"`
	Extensions map[string]any `json:"extensions"`
}
