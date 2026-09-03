package persistence

import (
	"errors"
	"testing"
)

func TestRetryableMutationError(t *testing.T) {
	savepoint := errors.New("sqlite3: SQL logic error: no such savepoint: rhiza_command")
	if !retryableMutationError(savepoint, 0) {
		t.Fatal("Rhiza savepoint loss must retry the identical mutation")
	}
	if retryableMutationError(savepoint, 1) {
		t.Fatal("persistent savepoint loss must return after one retry")
	}
	if retryableMutationError(errors.New("sqlite3: constraint failed"), 0) {
		t.Fatal("deterministic SQL failures must not retry")
	}
}
