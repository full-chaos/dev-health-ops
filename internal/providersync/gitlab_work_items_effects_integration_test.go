//go:build integration

package providersync

import (
	"time"
)

func gitlabFloatPtr(value float64) *float64    { return &value }
func gitlabTimePtr(value time.Time) *time.Time { return &value }
