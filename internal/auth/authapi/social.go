package authapi

import (
	"net/http"

	"github.com/google/uuid"
)

type socialSessions struct{ h compatHandlers }

func (s socialSessions) WriteSession(w http.ResponseWriter, r *http.Request, _ uuid.UUID, _ bool) {
	notBuilt(w, r)
}
