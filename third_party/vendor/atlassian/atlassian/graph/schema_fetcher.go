package graph

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"log/slog"

	"atlassian/atlassian"
)

const introspectionQuery = `
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types {
      ...FullType
    }
    directives {
      name
      description
      locations
      args {
        ...InputValue
      }
    }
  }
}

fragment FullType on __Type {
  kind
  name
  description
  fields(includeDeprecated: true) {
    name
    description
    args {
      ...InputValue
    }
    type {
      ...TypeRef
    }
    isDeprecated
    deprecationReason
  }
  inputFields {
    ...InputValue
  }
  interfaces {
    ...TypeRef
  }
  enumValues(includeDeprecated: true) {
    name
    description
    isDeprecated
    deprecationReason
  }
  possibleTypes {
    ...TypeRef
  }
}

fragment InputValue on __InputValue {
  name
  description
  type { ...TypeRef }
  defaultValue
}

fragment TypeRef on __Type {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
            }
          }
        }
      }
    }
  }
}
`

type SchemaFetchResult struct {
	IntrospectionJSONPath string
	SDLPath               string
}

type SchemaFetchOptions struct {
	OutputDir         string
	ExperimentalAPIs  []string
	Timeout           time.Duration
	Logger            *slog.Logger
	HTTPClient        *http.Client
}

func FetchSchemaIntrospection(
	ctx context.Context,
	baseURL string,
	auth atlassian.AuthProvider,
	opts SchemaFetchOptions,
) (*SchemaFetchResult, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, errors.New("baseURL is required")
	}
	if auth == nil {
		return nil, errors.New("auth is required")
	}

	outputDir := strings.TrimSpace(opts.OutputDir)
	if outputDir == "" {
		outputDir = "graphql"
	}

	clientTimeout := opts.Timeout
	if clientTimeout <= 0 {
		clientTimeout = defaultTimeout
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: clientTimeout}
	} else if httpClient.Timeout == 0 {
		copied := *httpClient
		copied.Timeout = clientTimeout
		httpClient = &copied
	}

	client := Client{
		BaseURL:       baseURL,
		Auth:          auth,
		HTTPClient:    httpClient,
		Strict:        true,
		MaxRetries429: defaultRetries429,
		MaxWait:       defaultMaxWait,
		Logger:        opts.Logger,
	}

	result, err := client.Execute(ctx, introspectionQuery, nil, "IntrospectionQuery", opts.ExperimentalAPIs, 1)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Data == nil {
		return nil, errors.New("introspection response missing data")
	}
	if _, ok := result.Data["__schema"]; !ok {
		return nil, errors.New("introspection response missing data.__schema")
	}

	envelope := map[string]any{
		"data": result.Data,
	}
	if len(result.Errors) > 0 {
		envelope["errors"] = result.Errors
	}
	if result.Extensions != nil {
		envelope["extensions"] = result.Extensions
	}

	introspectionBytes, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return nil, err
	}
	introspectionBytes = append(introspectionBytes, '\n')

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, err
	}
	introspectionPath := filepath.Join(outputDir, "schema.introspection.json")
	if err := os.WriteFile(introspectionPath, introspectionBytes, 0o644); err != nil {
		return nil, err
	}

	return &SchemaFetchResult{
		IntrospectionJSONPath: introspectionPath,
		SDLPath:               "",
	}, nil
}
