package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIsAuthorized(t *testing.T) {
	keys, err := parseAuthKeys("0123456789abcdef,fedcba9876543210")
	if err != nil {
		t.Fatal(err)
	}
	authKeys = keys
	defer func() { authKeys = nil }()

	cases := map[string]bool{
		"0123456789abcdef":  true,
		"fedcba9876543210":  true,
		"":                  false,
		"0123456789abcde":   false,
		"0123456789abcdefg": false,
		"FEDCBA9876543210":  false,
	}
	for token, want := range cases {
		if got := isAuthorized(token); got != want {
			t.Errorf("isAuthorized(%q) = %v, want %v", token, got, want)
		}
	}
}

func TestIsAuthorizedWithoutConfiguredKeys(t *testing.T) {
	authKeys = nil
	if isAuthorized("0123456789abcdef") {
		t.Fatal("must deny everything when no key is configured")
	}
}

func TestParseRequestRejectsEmptyBody(t *testing.T) {
	keys, _ := parseAuthKeys("0123456789abcdef")
	authKeys = keys
	defer func() { authKeys = nil }()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/s2s/event", http.NoBody)
	req.Header.Set("x-auth-token", "0123456789abcdef")
	rec := httptest.NewRecorder()
	parseRequest(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d", rec.Code)
	}
}

func TestParseRequestRejectsMalformedJSON(t *testing.T) {
	keys, _ := parseAuthKeys("0123456789abcdef")
	authKeys = keys
	defer func() { authKeys = nil }()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/s2s/event", strings.NewReader(`{"a":`))
	req.Header.Set("x-auth-token", "0123456789abcdef")
	rec := httptest.NewRecorder()
	parseRequest(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d", rec.Code)
	}
}

func TestProbe(t *testing.T) {
	rec := httptest.NewRecorder()
	probe(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestNewHTTPServerSetsTimeoutsAndLimits(t *testing.T) {
	srv := newHTTPServer(":0", http.NewServeMux())
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Fatalf("timeouts must be set: %+v", srv)
	}
	if srv.ReadHeaderTimeout > srv.ReadTimeout {
		t.Fatal("ReadHeaderTimeout must not exceed ReadTimeout")
	}
	if srv.MaxHeaderBytes <= 0 || srv.MaxHeaderBytes > 1<<20 {
		t.Fatalf("MaxHeaderBytes = %d", srv.MaxHeaderBytes)
	}
	if srv.IdleTimeout < time.Hour {
		t.Fatalf("IdleTimeout %s is below the gateway upstream keep-alive", srv.IdleTimeout)
	}
}

func TestSampleFixturesAreValid(t *testing.T) {
	for _, file := range []string{"tests/single_json.txt", "tests/many_jsons.txt"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		docs, err := BodyToJSON(raw)
		if err != nil || len(docs) == 0 {
			t.Fatalf("%s: %v", file, err)
		}
		for _, doc := range docs {
			data, err := createMapFromJSON(doc)
			if err != nil {
				t.Fatalf("%s: %v", file, err)
			}
			payload, ok := data.(map[string]interface{})
			if !ok {
				t.Fatalf("%s: unexpected type %T", file, data)
			}
			if _, err := validatePayload(payload); err != nil {
				t.Fatalf("%s: %v", file, err)
			}
		}
	}
}

func TestCreateMapFromJSON(t *testing.T) {
	data, err := createMapFromJSON(`{"a":"x","b":1}`)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := data.(map[string]interface{})
	if !ok || m["a"] != "x" || m["b"] != 1.0 {
		t.Fatalf("got %#v", data)
	}
	for _, bad := range []string{``, `[1]`, `"x"`, `{"a":`} {
		if _, err := createMapFromJSON(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestTruncateForLog(t *testing.T) {
	if got := truncateForLog("short"); got != "short" {
		t.Fatalf("got %q", got)
	}
	long := strings.Repeat("a", 100)
	if got := truncateForLog(long); len(got) != 67 || !strings.HasSuffix(got, "...") {
		t.Fatalf("got %q", got)
	}
}
