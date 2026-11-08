package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidatePayloadRejectsUnsafeNames(t *testing.T) {
	cases := map[string]map[string]interface{}{
		"nil map":                  nil,
		"event_type injection":     {"event_type": "x ENGINE=Memory AS SELECT * FROM system.users --"},
		"event_type with dot":      {"event_type": "other_db.table"},
		"event_type with tick":     {"event_type": "a`b"},
		"empty event_type":         {"event_type": ""},
		"numeric event_type":       {"event_type": 1.0},
		"object event_type":        {"event_type": map[string]interface{}{}},
		"column injection":         {"event_type": "ok", "a String) ENGINE=Memory --": "x"},
		"column with space":        {"event_type": "ok", "a b": "x"},
		"column starts with digit": {"event_type": "ok", "1a": "x"},
		"overlong column":          {"event_type": "ok", strings.Repeat("a", maxIdentifierLength+1): "x"},
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := validatePayload(payload); !errors.Is(err, errInvalidPayload) {
				t.Fatalf("expected errInvalidPayload, got %v", err)
			}
		})
	}
}

func TestValidatePayloadFieldLimit(t *testing.T) {
	payload := map[string]interface{}{}
	for i := 0; i <= *maxColumns; i++ {
		payload[fmt.Sprintf("c%d", i)] = "v"
	}
	if _, err := validatePayload(payload); !errors.Is(err, errInvalidPayload) {
		t.Fatalf("expected field limit error, got %v", err)
	}
}

func TestValidatePayloadAcceptsPlainIdentifiers(t *testing.T) {
	tableName, err := validatePayload(map[string]interface{}{"event_type": "response_time_base_api", "user_id": "1", "_ts": 1.0})
	if err != nil || tableName != "response_time_base_api" {
		t.Fatalf("got %q, %v", tableName, err)
	}
	tableName, err = validatePayload(map[string]interface{}{"user_id": "1"})
	if err != nil || tableName != "default" {
		t.Fatalf("got %q, %v", tableName, err)
	}
}

func TestQuoteIdentifierEscapesBackticks(t *testing.T) {
	if got := quoteIdentifier("a`b\\"); got != "`a\\`b\\\\`" {
		t.Fatalf("got %s", got)
	}
	if got := quoteString(`it's\`); got != `'it\'s\\'` {
		t.Fatalf("got %s", got)
	}
}

func TestBuildCreateTableQueriesQuotesIdentifiers(t *testing.T) {
	defer setFlag(clickhouseClusterName, "")()
	defer setFlag(clickhouseDatabaseName, "clickhouse_connector")()

	create, dist := buildCreateTableQueries("events", map[string]interface{}{"user_id": "1"})
	if dist != "" {
		t.Fatalf("unexpected distributed DDL in single instance mode: %s", dist)
	}
	if !strings.Contains(create, "CREATE TABLE IF NOT EXISTS `clickhouse_connector`.`events`  (`user_id` String)") {
		t.Fatalf("unexpected DDL: %s", create)
	}

	defer setFlag(clickhouseClusterName, "cluster_a")()
	_, dist = buildCreateTableQueries("events", map[string]interface{}{"user_id": "1"})
	want := "CREATE TABLE IF NOT EXISTS `clickhouse_connector`.`dist_events` ON CLUSTER `cluster_a` (`user_id` String) ENGINE = Distributed('cluster_a', 'clickhouse_connector', 'events', rand())"
	if dist != want {
		t.Fatalf("got  %s\nwant %s", dist, want)
	}
}

func TestBodyToJSON(t *testing.T) {
	docs, err := BodyToJSON([]byte(` [{"b":1,"a":"x"},{"c":true}] `))
	if err != nil || len(docs) != 2 || docs[0] != `{"a":"x","b":1}` {
		t.Fatalf("got %v, %v", docs, err)
	}
	docs, err = BodyToJSON([]byte(`{"a":"x"}`))
	if err != nil || len(docs) != 1 || docs[0] != `{"a":"x"}` {
		t.Fatalf("got %v, %v", docs, err)
	}
	for _, body := range []string{`123`, `"x"`, `null`, `[null]`, `[1,2]`, `{"a":`, ``} {
		if _, err := BodyToJSON([]byte(body)); err == nil {
			t.Fatalf("expected error for body %q", body)
		}
	}
}

func TestParseAuthKeys(t *testing.T) {
	if _, err := parseAuthKeys(""); err == nil {
		t.Fatal("expected error for empty auth key")
	}
	if _, err := parseAuthKeys("short"); err == nil {
		t.Fatal("expected error for short auth key")
	}
	keys, err := parseAuthKeys(" 0123456789abcdef , fedcba9876543210 ,")
	if err != nil || len(keys) != 2 {
		t.Fatalf("got %d keys, %v", len(keys), err)
	}
}

func TestParseRequestRejectsUnauthorizedAndOversized(t *testing.T) {
	keys, _ := parseAuthKeys("0123456789abcdef,fedcba9876543210")
	authKeys = keys
	defer func() { authKeys = nil }()
	limit := int64(16)
	defer setInt64Flag(maxBodyBytes, limit)()

	cases := []struct {
		name  string
		token string
		body  string
		want  int
	}{
		{"missing token", "", `{"a":1}`, http.StatusUnauthorized},
		{"wrong token", "0123456789abcdeX", `{"a":1}`, http.StatusUnauthorized},
		{"oversized body", "fedcba9876543210", `{"a":"` + strings.Repeat("x", 64) + `"}`, http.StatusRequestEntityTooLarge},
		{"scalar body", "0123456789abcdef", `123`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/s2s/event", strings.NewReader(tc.body))
			if tc.token != "" {
				req.Header.Set("x-auth-token", tc.token)
			}
			rec := httptest.NewRecorder()
			parseRequest(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("got status %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

func setFlag(target *string, value string) func() {
	previous := *target
	*target = value
	return func() { *target = previous }
}

func setInt64Flag(target *int64, value int64) func() {
	previous := *target
	*target = value
	return func() { *target = previous }
}
