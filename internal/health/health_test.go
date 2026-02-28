package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

type failingPinger struct{ err error }

func (p *failingPinger) Ping(context.Context) error { return p.err }

type okPinger struct{}

func (p *okPinger) Ping(context.Context) error { return nil }

func TestChecker_Liveness(t *testing.T) {
	c := NewChecker(nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /health: got status %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type: got %s", rec.Header().Get("Content-Type"))
	}
}

func TestChecker_LiveAlias(t *testing.T) {
	c := NewChecker(nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/live", nil)
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /live: got status %d", rec.Code)
	}
}

func TestChecker_Readiness_AllNil(t *testing.T) {
	c := NewChecker(nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /ready with no deps: got status %d", rec.Code)
	}
}

func TestChecker_Readiness_AllOk(t *testing.T) {
	c := NewChecker(&okPinger{}, &okPinger{}, &okPinger{})
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /ready all ok: got status %d", rec.Code)
	}
}

func TestChecker_Readiness_StoreFails(t *testing.T) {
	c := NewChecker(&failingPinger{err: context.DeadlineExceeded}, &okPinger{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("GET /ready store fail: got status %d", rec.Code)
	}
}

func TestChecker_Readiness_NotFound(t *testing.T) {
	c := NewChecker(nil, nil, nil)
	for _, path := range []string{"/", "/other", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		c.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s: got status %d", path, rec.Code)
		}
	}
}

func TestChecker_MethodNotAllowed(t *testing.T) {
	c := NewChecker(nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("POST /health: got status %d", rec.Code)
	}
}
