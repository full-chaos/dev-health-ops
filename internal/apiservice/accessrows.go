package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
)

const goAPIAccessService = accessrouting.Service

var (
	goAPIAccessRows = accessrouting.Rows
	parseAccessRows = accessrouting.ParseRows
)

type routedAccessVerify = accessrouting.Adapter
