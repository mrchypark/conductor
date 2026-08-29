package connectors

import (
	"errors"
	"testing"

	"github.com/amp-labs/connectors/common"
)

func TestConnectorErrorPreservesAmpClassification(t *testing.T) {
	checks := []struct {
		err       error
		class     string
		retryable bool
	}{
		{common.ErrAccessToken, "auth_expired", false},
		{common.ErrForbidden, "permission_denied", false},
		{common.ErrLimitExceeded, "rate_limited", true},
		{common.ErrNotFound, "not_found", false},
		{common.ErrConflict, "conflict", false},
		{common.ErrServer, "retryable", true},
	}
	for _, check := range checks {
		got := connectorError(check.err)
		if got.Class != check.class || got.Retryable != check.retryable {
			t.Errorf("connectorError(%v) = %+v, want class=%q retryable=%t", check.err, got, check.class, check.retryable)
		}
	}
	wrapped := connectorError(errors.New("provider failure"))
	if wrapped.Class != "connector_error" || wrapped.Retryable {
		t.Fatalf("unexpected unknown error classification: %+v", wrapped)
	}
}
