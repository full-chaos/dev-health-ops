package authapi

import (
	"net/http"

	"github.com/google/uuid"
)

type socialSessions struct{ h handlers }

func (s socialSessions) WriteSession(w http.ResponseWriter, _ *http.Request, _ uuid.UUID, _ bool) {
	notBuilt(w)
}
