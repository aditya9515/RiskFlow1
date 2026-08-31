package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type fakePinger struct {
	err   error
	calls atomic.Int32
}

func (f *fakePinger) Ping(context.Context) error {
	f.calls.Add(1)
	return f.err
}

func TestHealthReturnsOKWithoutDatabasePing(t *testing.T) {
	t.Parallel()

	pinger := &fakePinger{}
	handler := NewHandler(pinger, nil, nil, nil, nil, time.Second, 3*time.Second, 3*time.Second, 5*time.Second, discardLogger())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if pinger.calls.Load() != 0 {
		t.Fatalf("database ping calls = %d, want 0", pinger.calls.Load())
	}

	var body statusResponse
	decodeJSON(t, recorder, &body)
	if body.Status != "ok" {
		t.Fatalf("status body = %q, want ok", body.Status)
	}
}

func TestReadyReturnsOKWhenDatabasePingSucceeds(t *testing.T) {
	t.Parallel()

	pinger := &fakePinger{}
	handler := NewHandler(pinger, nil, nil, nil, nil, time.Second, 3*time.Second, 3*time.Second, 5*time.Second, discardLogger())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if pinger.calls.Load() != 1 {
		t.Fatalf("database ping calls = %d, want 1", pinger.calls.Load())
	}

	var body statusResponse
	decodeJSON(t, recorder, &body)
	if body.Status != "ready" {
		t.Fatalf("status body = %q, want ready", body.Status)
	}
}

func TestReadyReturnsTypedErrorWhenDatabasePingFails(t *testing.T) {
	t.Parallel()

	pinger := &fakePinger{err: errors.New("database offline")}
	handler := NewHandler(pinger, nil, nil, nil, nil, time.Second, 3*time.Second, 3*time.Second, 5*time.Second, discardLogger())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if recorder.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", recorder.Header().Get("Content-Type"))
	}

	var body errorResponse
	decodeJSON(t, recorder, &body)
	if body.Error.Code != "database_unavailable" {
		t.Fatalf("error code = %q, want database_unavailable", body.Error.Code)
	}
	if body.Error.Message != "database unavailable" {
		t.Fatalf("error message = %q, want database unavailable", body.Error.Message)
	}
}

func decodeJSON(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()

	if err := json.NewDecoder(recorder.Body).Decode(target); err != nil {
		t.Fatalf("decode JSON response: %v", err)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
