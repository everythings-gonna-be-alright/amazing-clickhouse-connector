package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const maxIdentifierLength = 200

var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// errInvalidPayload marks events that can never be inserted, so they are dropped instead of rejected for retry.
var errInvalidPayload = errors.New("invalid payload")

var identifierEscaper = strings.NewReplacer(`\`, `\\`, "`", "\\`")
var stringLiteralEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

func validateIdentifier(name string) error {
	if len(name) > maxIdentifierLength || !identifierRe.MatchString(name) {
		return fmt.Errorf("%w: illegal identifier %q", errInvalidPayload, truncateForLog(name))
	}
	return nil
}

func quoteIdentifier(name string) string {
	return "`" + identifierEscaper.Replace(name) + "`"
}

func quoteString(value string) string {
	return "'" + stringLiteralEscaper.Replace(value) + "'"
}

func qualifiedTable(tableName string) string {
	return quoteIdentifier(*clickhouseDatabaseName) + "." + quoteIdentifier(tableName)
}

func quoteIdentifiers(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteIdentifier(name)
	}
	return quoted
}

// validatePayload enforces that every client-controlled name used in DDL/DML is a plain identifier.
func validatePayload(payloadMap map[string]interface{}) (string, error) {
	if payloadMap == nil {
		return "", fmt.Errorf("%w: event is not a JSON object", errInvalidPayload)
	}
	if len(payloadMap) > *maxColumns {
		return "", fmt.Errorf("%w: %d fields exceed the limit of %d", errInvalidPayload, len(payloadMap), *maxColumns)
	}
	for key := range payloadMap {
		if err := validateIdentifier(key); err != nil {
			return "", err
		}
	}

	tableName := "default"
	if value, ok := payloadMap["event_type"]; ok && value != nil {
		eventType, isString := value.(string)
		if !isString {
			return "", fmt.Errorf("%w: event_type must be a string, got %T", errInvalidPayload, value)
		}
		tableName = eventType
	}
	if err := validateIdentifier(tableName); err != nil {
		return "", err
	}
	return tableName, nil
}

func truncateForLog(s string) string {
	const limit = 64
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
