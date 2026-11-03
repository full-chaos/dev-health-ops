package customerpush

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	reconcileTokenColumns = ``
	reconcileTokenPageSQL = ``
	reconcileTokenIDsSQL  = ``
)

type TxBeginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

type PagePair struct {
	Tokens    []pushreconcile.Token
	Projected []pushreconcile.Projected
	Next      string
	Last      bool
}

func ReadDomainPage(ctx context.Context, db TxBeginner, schema authschema.ValidatedIdentifier, after uuid.UUID, batch int) (PagePair, error) {
	return PagePair{}, errors.New("customerpush: not implemented")
}

func ReadProjectionPage(ctx context.Context, db TxBeginner, schema authschema.ValidatedIdentifier, after string, batch int) (PagePair, error) {
	return PagePair{}, errors.New("customerpush: not implemented")
}
