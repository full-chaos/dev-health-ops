package shadowvenue

const statesHeader = "row\tcaller\tk\tstate"

const statePlaceholder = "{state}"

// States: skeleton.
type States struct{ byCell map[string]string }

// ParseStates: skeleton, no checks yet.
func ParseStates(raw []byte) (States, error) { return States{}, nil }
