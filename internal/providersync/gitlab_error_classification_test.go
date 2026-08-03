package providersync

import (
	"errors"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabTypedNilSingleUnwrapper struct{ cause error }

func (err *gitLabTypedNilSingleUnwrapper) Error() string { return "typed nil single" }
func (err *gitLabTypedNilSingleUnwrapper) Unwrap() error { return err.cause }

type gitLabTypedNilMultiUnwrapper struct{ causes []error }

func (err *gitLabTypedNilMultiUnwrapper) Error() string   { return "typed nil multi" }
func (err *gitLabTypedNilMultiUnwrapper) Unwrap() []error { return err.causes }

func TestGitLabErrorTreeOnlyProviderClasses(t *testing.T) {
	t.Parallel()
	soft := []providerfoundation.ErrorClass{
		providerfoundation.ErrorNotFound,
		providerfoundation.ErrorConflict,
		providerfoundation.ErrorTransient,
		providerfoundation.ErrorPermanent,
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "direct allowed provider leaf",
			err:  &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient},
			want: true,
		},
		{
			name: "wrapped allowed provider leaf",
			err: fmt.Errorf(
				"detail: %w",
				&providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent},
			),
			want: true,
		},
		{
			name: "joined allowed provider leaves",
			err: errors.Join(
				&providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound},
				&providerfoundation.ProviderError{Class: providerfoundation.ErrorConflict},
			),
			want: true,
		},
		{
			name: "nested allowed provider leaves",
			err: fmt.Errorf(
				"detail: %w",
				errors.Join(
					&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient},
					fmt.Errorf(
						"release: %w",
						&providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent},
					),
				),
			),
			want: true,
		},
		{
			name: "joined budget leaf",
			err: errors.Join(
				&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient},
				providerfoundation.ErrBudgetUnavailable,
			),
		},
		{
			name: "nested lease leaf",
			err: fmt.Errorf(
				"detail: %w",
				errors.Join(
					&providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound},
					fmt.Errorf("release: %w", providerfoundation.ErrLeaseLost),
				),
			),
		},
		{
			name: "authentication leaf",
			err:  &providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication},
		},
		{
			name: "rate-limit leaf",
			err:  &providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited},
		},
		{
			name: "cancelled leaf",
			err:  &providerfoundation.ProviderError{Class: providerfoundation.ErrorCancelled},
		},
		{
			name: "unknown provider leaf",
			err:  &providerfoundation.ProviderError{Class: "future-control-plane-class"},
		},
		{name: "nil provider leaf", err: (*providerfoundation.ProviderError)(nil)},
		{name: "typed nil single unwrapper", err: (*gitLabTypedNilSingleUnwrapper)(nil)},
		{name: "typed nil multi unwrapper", err: (*gitLabTypedNilMultiUnwrapper)(nil)},
		{name: "non-provider leaf", err: errors.New("transport failed")},
		{name: "nil error", err: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := gitLabErrorTreeOnlyProviderClasses(test.err, soft...); got != test.want {
				t.Fatalf("soft=%t want %t for %v", got, test.want, test.err)
			}
		})
	}
}
