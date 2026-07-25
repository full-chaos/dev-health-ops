// Package version exposes build metadata for commands, logs, and metrics.
package version

import (
	"encoding/json"
	"io"
	"log/slog"
	"runtime"
	"runtime/debug"
)

// These values may be set with -ldflags. Safe development defaults keep local
// builds useful without pretending they are release artifacts.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

type Info struct {
	Service   string `json:"service"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
	Modified  bool   `json:"modified"`
}

func Current(service string) Info {
	info := Info{
		Service:   service,
		Version:   Version,
		Commit:    Commit,
		BuildTime: BuildTime,
		GoVersion: runtime.Version(),
	}
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				if info.Commit == "unknown" {
					info.Commit = setting.Value
				}
			case "vcs.time":
				if info.BuildTime == "unknown" {
					info.BuildTime = setting.Value
				}
			case "vcs.modified":
				info.Modified = setting.Value == "true"
			}
		}
	}
	return info
}

func (i Info) Attrs() []slog.Attr {
	return []slog.Attr{
		slog.String("version", i.Version),
		slog.String("commit", i.Commit),
		slog.String("build_time", i.BuildTime),
		slog.String("go_version", i.GoVersion),
		slog.Bool("modified", i.Modified),
	}
}

func (i Info) WriteJSON(output io.Writer) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(i)
}
