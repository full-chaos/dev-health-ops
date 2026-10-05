package goapiproof

import (
	"errors"
	"fmt"
)

// ErrDocumentOperationNotRouted refuses a routing verb that names a catalog (document) operation. query-api serves
// every registered operation (routeswitch.NewCatalogSwitch) and no row decides it, so there is nothing to enable,
// disable, seed or repoint. Only MCP class roots (mcp:<root>) have a decision, in go_api_class_decision.
var ErrDocumentOperationNotRouted = errors.New("goapiproof: a catalog operation has no routing state")

func refuseDocumentOperations(verb string, document []string) error {
	return fmt.Errorf("%w: %s applies to MCP class roots (mcp:<root>) only, and query-api serves every registered catalog operation without a row; not a class root: %v",
		ErrDocumentOperationNotRouted, verb, document)
}
