package main

import (
	"strings"
	"testing"
	"time"
)

func TestOnClusterQuery(t *testing.T) {
	defer setFlag(clickhouseClusterName, "")()
	if got := onClusterQuery(); got != "" {
		t.Fatalf("got %q", got)
	}
	*clickhouseClusterName = "my`cluster"
	if got := onClusterQuery(); got != "ON CLUSTER `my\\`cluster`" {
		t.Fatalf("got %q", got)
	}
}

func TestPhysicalTableName(t *testing.T) {
	defer setFlag(clickhouseClusterName, "")()
	defer setBoolFlag(clickhouseLegacyDistPrefix, false)()

	if got := physicalTableName("events"); got != "events" {
		t.Fatalf("got %s", got)
	}

	*clickhouseLegacyDistPrefix = true
	if got := physicalTableName("events"); got != "dist_events" {
		t.Fatalf("single instance with legacy prefix: got %s", got)
	}

	*clickhouseClusterName = "cluster_a"
	if useLegacyDistPrefix() {
		t.Fatal("legacy prefix must have no effect in cluster mode")
	}
	if got := physicalTableName("events"); got != "events" {
		t.Fatalf("cluster mode: got %s", got)
	}
}

func TestBuildCreateTableQueriesEngines(t *testing.T) {
	defer setFlag(clickhouseDatabaseName, "db")()
	defer setFlag(clickhouseClusterName, "")()
	defer setBoolFlag(clickhouseLegacyDistPrefix, false)()
	payload := map[string]interface{}{"b": 1.5, "a": "x"}

	create, dist := buildCreateTableQueries("events", payload)
	if dist != "" {
		t.Fatalf("unexpected distributed DDL: %s", dist)
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `db`.`events`",
		"(`a` String,`b` Float64)",
		"ENGINE = ReplacingMergeTree()",
		"PRIMARY KEY eventn_ctx_event_id",
	} {
		if !strings.Contains(create, want) {
			t.Fatalf("missing %q in %s", want, create)
		}
	}

	*clickhouseLegacyDistPrefix = true
	create, dist = buildCreateTableQueries("events", payload)
	if dist != "" || !strings.Contains(create, "`db`.`dist_events`") || !strings.Contains(create, "ENGINE = ReplacingMergeTree()") {
		t.Fatalf("legacy prefix DDL: %s / %s", create, dist)
	}

	*clickhouseLegacyDistPrefix = false
	*clickhouseClusterName = "cluster_a"
	create, dist = buildCreateTableQueries("events", payload)
	if !strings.Contains(create, "ON CLUSTER `cluster_a`") || !strings.Contains(create, "ENGINE = ReplicatedReplacingMergeTree()") {
		t.Fatalf("cluster base DDL: %s", create)
	}
	if !strings.Contains(dist, "`db`.`dist_events`") || !strings.Contains(dist, "Distributed('cluster_a', 'db', 'events', rand())") {
		t.Fatalf("cluster distributed DDL: %s", dist)
	}
}

func TestBuildCreateTableQueriesEscapesClusterAndDatabaseLiterals(t *testing.T) {
	defer setFlag(clickhouseDatabaseName, "d'b")()
	defer setFlag(clickhouseClusterName, "c'l")()

	_, dist := buildCreateTableQueries("events", map[string]interface{}{"a": "x"})
	if !strings.Contains(dist, `Distributed('c\'l', 'd\'b', 'events', rand())`) {
		t.Fatalf("string literals not escaped: %s", dist)
	}
}

func TestBuildAlterTableQuery(t *testing.T) {
	defer setFlag(clickhouseDatabaseName, "db")()
	got := buildAlterTableQuery("events", "ON CLUSTER `c`", "new_col", "Int64", "AFTER `prev`")
	want := "ALTER TABLE `db`.`events` ON CLUSTER `c` ADD COLUMN `new_col` Int64 AFTER `prev`"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestQualifiedTableAndQuoteIdentifiers(t *testing.T) {
	defer setFlag(clickhouseDatabaseName, "db")()
	if got := qualifiedTable("t"); got != "`db`.`t`" {
		t.Fatalf("got %s", got)
	}
	got := quoteIdentifiers([]string{"a", "b`c"})
	if len(got) != 2 || got[0] != "`a`" || got[1] != "`b\\`c`" {
		t.Fatalf("got %v", got)
	}
}

func TestAddServiceColumnsOverridesClientValues(t *testing.T) {
	payload := map[string]interface{}{
		"api_key":             "client-supplied",
		"_timestamp":          "client-supplied",
		"eventn_ctx_event_id": "client-supplied",
		"other":               "kept",
	}
	before := time.Now()
	addServiceColumns(payload)

	if payload["api_key"] != "" {
		t.Fatalf("api_key = %v", payload["api_key"])
	}
	if ts, ok := payload["_timestamp"].(time.Time); !ok || ts.Before(before) {
		t.Fatalf("_timestamp = %v", payload["_timestamp"])
	}
	id, ok := payload["eventn_ctx_event_id"].(string)
	if !ok || len(id) != 36 || id == "client-supplied" {
		t.Fatalf("eventn_ctx_event_id = %v", payload["eventn_ctx_event_id"])
	}
	if payload["other"] != "kept" {
		t.Fatalf("unrelated field changed: %v", payload["other"])
	}

	second := map[string]interface{}{}
	addServiceColumns(second)
	if second["eventn_ctx_event_id"] == id {
		t.Fatal("event ids must be unique")
	}
}

func TestHashIsStable(t *testing.T) {
	if hash("events") != hash("events") || hash("a") == hash("b") {
		t.Fatal("hash must be deterministic and distinguish inputs")
	}
}

func setBoolFlag(target *bool, value bool) func() {
	previous := *target
	*target = value
	return func() { *target = previous }
}
