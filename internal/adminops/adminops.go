// Package adminops is the operator verbs of the `admin` group of dho that work on
// application data: users, organizations, BYO LLM settings, licenses, feature
// bundles and billing plans. They live apart from package admincli (the feature
// seed the migrate Job runs) so that binary does not link the admin route code the
// verbs share (internal/apiservice/admin).
package adminops

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/full-chaos/dev-health-ops/internal/admincli"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Command is the `admin` group: the feature seed of package admincli and the
// verbs of this package.
func Command() cli.Command {
	command := admincli.Command()
	command.Children = append([]cli.Command{usersGroup(), orgsGroup(), llmSettingsGroup()}, command.Children...)
	return command
}

func writeError(stderr io.Writer, code, detail string) int {
	_ = json.NewEncoder(stderr).Encode(map[string]any{"error": map[string]string{"code": code, "detail": detail}})
	return cli.ExitFailure
}

var _ = fmt.Sprint
