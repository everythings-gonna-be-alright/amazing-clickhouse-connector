package main

import (
	"reflect"
	"testing"
	"time"
)

func withColumnOverrides(t *testing.T, overrides map[string]string) {
	t.Helper()
	previous := columnTypeOverrides
	columnTypeOverrides = overrides
	t.Cleanup(func() { columnTypeOverrides = previous })
}

func TestGetColumnTypeWithName(t *testing.T) {
	withColumnOverrides(t, map[string]string{"ts": "String"})

	cases := []struct {
		name   string
		value  interface{}
		column string
		want   string
	}{
		{"plain string", "hello", "a", "String"},
		{"datetime string", "2024-05-01 10:20:30.123", "a", "DateTime64"},
		{"date only string", "2024-05-01", "a", "String"},
		{"int", 1, "a", "Int64"},
		{"int32", int32(1), "a", "Int64"},
		{"int64", int64(1), "a", "Int64"},
		{"float32", float32(1.5), "a", "Float64"},
		{"float64", 1.5, "a", "Float64"},
		{"time", time.Now(), "a", "DateTime64"},
		{"bool falls back to string", true, "a", "String"},
		{"nil falls back to string", nil, "a", "String"},
		{"override wins over value type", "2024-05-01 10:20:30.123", "ts", "String"},
		{"empty column name ignores overrides", 1.5, "", "Float64"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := getColumnTypeWithName(tc.value, tc.column); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestGetColumnTypeWithNameWithoutOverrides(t *testing.T) {
	withColumnOverrides(t, nil)
	if got := getColumnTypeWithName(1.5, "ts"); got != "Float64" {
		t.Fatalf("got %s", got)
	}
}

func TestConvertValueToConfiguredType(t *testing.T) {
	withColumnOverrides(t, map[string]string{
		"s":  "String",
		"i":  "Int64",
		"f":  "Float64",
		"d":  "DateTime64",
		"u":  "UInt64",
		"bl": "Boolean",
	})
	parsed := time.Date(2024, 5, 1, 10, 20, 30, 0, time.UTC)

	cases := []struct {
		name    string
		column  string
		value   interface{}
		want    interface{}
		wantErr bool
	}{
		{"string from number", "s", 12.0, "12", false},
		{"string from bool", "s", true, "true", false},
		{"int64 from int", "i", 7, int64(7), false},
		{"int64 from int32", "i", int32(7), int64(7), false},
		{"int64 from int64", "i", int64(7), int64(7), false},
		{"int64 from float64", "i", 7.0, int64(7), false},
		{"int64 from float32", "i", float32(7), int64(7), false},
		{"int64 from numeric string", "i", "42", int64(42), false},
		{"int64 from bad string", "i", "abc", nil, true},
		{"int64 from bool", "i", true, nil, true},
		{"float64 from float64", "f", 1.5, 1.5, false},
		{"float64 from float32", "f", float32(1.5), 1.5, false},
		{"float64 from int", "f", 2, 2.0, false},
		{"float64 from int32", "f", int32(2), 2.0, false},
		{"float64 from int64", "f", int64(2), 2.0, false},
		{"float64 from numeric string", "f", "2.5", 2.5, false},
		{"float64 from bad string", "f", "x", nil, true},
		{"float64 from bool", "f", false, nil, true},
		{"datetime from time", "d", parsed, parsed, false},
		{"datetime from millis string", "d", "2024-05-01 10:20:30.000", parsed, false},
		{"datetime from rfc3339 millis", "d", "2024-05-01T10:20:30.000Z", parsed, false},
		{"datetime from rfc3339", "d", "2024-05-01T10:20:30Z", parsed, false},
		{"datetime from seconds string", "d", "2024-05-01 10:20:30", parsed, false},
		{"datetime from bad string", "d", "yesterday", nil, true},
		{"datetime from number", "d", 1.0, nil, true},
		{"unhandled configured type passes value through", "u", 5.0, 5.0, false},
		{"boolean passes value through", "bl", true, true, false},
		{"column without override passes value through", "other", 5.0, 5.0, false},
		{"empty column passes value through", "", 5.0, 5.0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertValueToConfiguredType(tc.value, tc.column)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestConvertValueWithoutOverrides(t *testing.T) {
	withColumnOverrides(t, nil)
	got, err := convertValueToConfiguredType("x", "a")
	if err != nil || got != "x" {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestSortMapOrdersKeysAndConvertsValues(t *testing.T) {
	withColumnOverrides(t, map[string]string{"n": "Int64", "bad": "Int64"})

	ordered, names, values := sortMap(map[string]interface{}{
		"z":   "last",
		"n":   "5",
		"a":   1.5,
		"bad": "not-a-number",
	})

	if want := []string{"a", "bad", "n", "z"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
	if want := []interface{}{1.5, "not-a-number", int64(5), "last"}; !reflect.DeepEqual(values, want) {
		t.Fatalf("values = %#v, want %#v", values, want)
	}
	if ordered["n"] != int64(5) || ordered["bad"] != "not-a-number" {
		t.Fatalf("ordered map = %#v", ordered)
	}
}

func TestInitializeColumnTypeOverrides(t *testing.T) {
	previousConfig := *columnTypesConfig
	previousOverrides := columnTypeOverrides
	t.Cleanup(func() {
		*columnTypesConfig = previousConfig
		columnTypeOverrides = previousOverrides
	})

	cases := []struct {
		name    string
		config  string
		want    map[string]string
		wantErr bool
	}{
		{"empty config", "", map[string]string{}, false},
		{"valid config", `{"user_id":"String","ts":"DateTime64","n":"Int64"}`, map[string]string{"user_id": "String", "ts": "DateTime64", "n": "Int64"}, false},
		{"unsupported type", `{"user_id":"Array(String)"}`, nil, true},
		{"malformed json", `{"user_id":`, nil, true},
		{"non string type", `{"user_id":1}`, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			*columnTypesConfig = tc.config
			err := initializeColumnTypeOverrides()
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && !reflect.DeepEqual(columnTypeOverrides, tc.want) {
				t.Fatalf("got %v, want %v", columnTypeOverrides, tc.want)
			}
		})
	}
}
