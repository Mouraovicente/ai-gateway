package core

import (
	"errors"
	"testing"
)

func TestBackendError_ErrorMessageWrapsUnderlying(t *testing.T) {
	underlying := errors.New("connection refused")
	be := &BackendError{Class: Transient, Status: 0, Err: underlying}

	if got := be.Error(); got != "backend error (transient): connection refused" {
		t.Fatalf("unexpected Error() output: %q", got)
	}
	if !errors.Is(be, underlying) {
		t.Fatalf("expected errors.Is to unwrap to underlying error")
	}
}

func TestBackendError_ClassifiesTransientVsPermanent(t *testing.T) {
	transient := &BackendError{Class: Transient, Status: 503}
	permanent := &BackendError{Class: Permanent, Status: 400}

	if !transient.IsTransient() {
		t.Fatalf("expected 503 BackendError to be transient")
	}
	if permanent.IsTransient() {
		t.Fatalf("expected 400 BackendError to be permanent")
	}
}
