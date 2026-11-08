package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// clickhouseConnect function for establishing connection with clickhouse database
func clickhouseConnect() (clickhouse.Conn, error) {
	var dialCount int

	// Build a TLS config when TLS is enabled (required by ClickHouse Cloud, port 9440).
	// Certificates are verified by default; verification can only be disabled explicitly for testing.
	var tlsConfig *tls.Config
	if *clickhouseTLS {
		serverName := *clickhouseServer
		if host, _, splitErr := net.SplitHostPort(*clickhouseServer); splitErr == nil {
			serverName = host
		}
		tlsConfig = &tls.Config{
			ServerName:         serverName,
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: *clickhouseTLSSkipVerify,
		}
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*clickhouseServer},
		Auth: clickhouse.Auth{
			Database: *clickhouseDatabaseName,
			Username: *clickhouseUsername,
			Password: *clickhousePassword,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			var d net.Dialer
			rawConn, dialErr := d.DialContext(ctx, "tcp", addr)
			if dialErr != nil {
				return nil, dialErr
			}
			// A custom DialContext bypasses the library's built-in TLS handling,
			// so the TLS handshake must be performed here when TLS is enabled.
			if tlsConfig == nil {
				return rawConn, nil
			}
			tlsConn := tls.Client(rawConn, tlsConfig)
			if handshakeErr := tlsConn.HandshakeContext(ctx); handshakeErr != nil {
				rawConn.Close()
				return nil, handshakeErr
			}
			return tlsConn, nil
		},
		Debug: false,
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 600,
			"receive_timeout":    600,
			"send_timeout":       600,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     5000,
		MaxIdleConns:     500,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func insertIntoClickhouseAsync(conn clickhouse.Conn, payloadMap map[string]interface{}, consumerName string) error {
	ctx := context.Background()
	// Build slices with column names and values for batch
	var columnsNames []string
	var columnsValues []interface{}

	// Set table name from event_type json value ( each event - new table with own structure )
	tableName, err := validatePayload(payloadMap)
	if err != nil {
		return err
	}

	addServiceColumns(payloadMap)
	payloadMap, columnsNames, columnsValues = sortMap(payloadMap)

	clickhouseMutex.Lock()

	// Create ReplicatedReplacingMergeTree and Distributed tables in the clickhouse database, based on json keys
	if clickhouseAsyncSize == 0 {
		err = createTable(conn, ctx, tableName, payloadMap)
		if err != nil {
			log.Printf("Table creating %v error", tableName)
			monitoringTableCreatingErrors.WithLabelValues(consumerName, "async_mode", tableName).Inc()
			clickhouseMutex.Unlock()
			return err
		}

		monitoringTableCreateQueries.WithLabelValues(consumerName, "async_mode", tableName).Inc()
	}
	clickhouseMutex.Unlock()

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(columnsValues)), ",")
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qualifiedTable(physicalTableName(tableName)), strings.Join(quoteIdentifiers(columnsNames), ","), placeholders)
	if err := conn.AsyncInsert(ctx, insertQuery, false, columnsValues...); err != nil {
		if strings.Contains(err.Error(), "code: 16, message: No such column") {
			// Try to exec ALTER TABLE
			alterErr := alterTable(ctx, conn, tableName, payloadMap, consumerName)
			if alterErr != nil {
				monitoringTableAlteringErrors.WithLabelValues(consumerName, "async_mode", tableName).Inc()
				return alterErr
			}
			monitoringTableAltered.WithLabelValues(consumerName, "async_mode", tableName).Inc()
			// Retry after alter
			return err
		}
		return err
	}

	clickhouseAsyncSize++
	if clickhouseAsyncSize > 50000 {
		log.Printf("[%s] Inserted 50000 async queries", consumerName)
		clickhouseAsyncSize = 0
	}

	return nil
}

// insertIntoClickhouse function for create bulks and send them to clickhouse database
func insertIntoClickhouse(conn clickhouse.Conn, payloadMap map[string]interface{}, consumerName string) error {
	ctx := context.Background()
	// Build slices with column names and values for batch
	var columnsNames []string
	var columnsValues []interface{}

	// Set table name from event_type json value ( each event - new table with own structure )
	tableName, err := validatePayload(payloadMap)
	if err != nil {
		return err
	}

	addServiceColumns(payloadMap)
	payloadMap, columnsNames, columnsValues = sortMap(payloadMap)

	// Create batch for this table and field names set
	batchName := fmt.Sprintf("%s_%s", tableName, fmt.Sprint(hash(strings.Join(columnsNames, "_"))))

	clickhouseMutex.Lock()
	clickhouseBatchesSizeMutex.Lock()

	// Check if batch exist or init it
	_, ok := clickhouseBatchesMap[batchName]
	if ok && clickhouseBatchesSizeMap[batchName] > 0 {
		clickhouseBatchesSizeMutex.Unlock()
		clickhouseMutex.Unlock()
	} else {
		// Create ReplicatedReplacingMergeTree and Distributed tables in the clickhouse database, based on json keys
		err := createTable(conn, ctx, tableName, payloadMap)
		if err != nil {
			log.Printf("Table creating %v error", tableName)
			monitoringTableCreatingErrors.WithLabelValues(consumerName, batchName, tableName).Inc()
			clickhouseBatchesSizeMutex.Unlock()
			clickhouseMutex.Unlock()
			return err
		}

		monitoringTableCreateQueries.WithLabelValues(consumerName, batchName, tableName).Inc()

		batchQuery := fmt.Sprintf("INSERT INTO %s (%s)", qualifiedTable(physicalTableName(tableName)), strings.Join(quoteIdentifiers(columnsNames), ","))
		clickhouseBatchesMap[batchName], err = conn.PrepareBatch(ctx, batchQuery)
		if err != nil {
			if strings.Contains(err.Error(), "code: 16, message: No such column") {
				// Try to exec ALTER TABLE
				alterErr := alterTable(ctx, conn, tableName, payloadMap, consumerName)
				if alterErr != nil {
					clickhouseBatchesSizeMutex.Unlock()
					clickhouseMutex.Unlock()
					monitoringTableAlteringErrors.WithLabelValues(consumerName, batchName, tableName).Inc()
					return alterErr
				}
				monitoringTableAltered.WithLabelValues(consumerName, batchName, tableName).Inc()
				// Retry after alter
				_, ok = clickhouseBatchesMap[batchName]
				if ok {
					delete(clickhouseBatchesMap, batchName)
					log.Printf("batch %s deleted", batchName)
					var retryErr error
					clickhouseBatchesMap[batchName], retryErr = conn.PrepareBatch(ctx, batchQuery)
					if retryErr != nil {
						clickhouseBatchesSizeMutex.Unlock()
						clickhouseMutex.Unlock()
						return retryErr
					}
				}
			} else {
				clickhouseBatchesSizeMutex.Unlock()
				clickhouseMutex.Unlock()
				return err
			}
		}
		log.Printf("[%s] batch %s created\n", consumerName, batchName)
		// Track batch processing time
		clickhouseBatchesTimeMap[batchName] = time.Now()
		clickhouseBatchesSizeMutex.Unlock()
		clickhouseMutex.Unlock()
	}

	// Try to add new payloadMap to batch. Sometimes code can’t do it because of concurrency and maybe some bugs
	// So try it 3 times
	clickhouseMutex.Lock()
	_, ok = clickhouseBatchesMap[batchName]
	if ok {
		err = clickhouseBatchesMap[batchName].Append(columnsValues...)
		clickhouseMutex.Unlock()
		if err != nil {
			monitoringBatchAppendingErrors.WithLabelValues(consumerName, batchName, tableName).Inc()
			return err
		}
	} else {
		err = errors.New("batch not exist")
		clickhouseMutex.Unlock()
		return err
	}

	monitoringBatchAppended.WithLabelValues(consumerName, batchName, tableName).Inc()

	clickhouseMutex.Lock()
	clickhouseBatchesSizeMutex.Lock()
	// Add information about one more element in batch
	clickhouseBatchesSizeMap[batchName]++
	monitoringBatchesSize.WithLabelValues(consumerName, batchName).Inc()

	// If number of elements in our batch reach max amount or batch timeout run out - try to send it to clickhouse database
	// Else - just add information about one more element in batch

	if (clickhouseBatchesSizeMap[batchName] >= *clickhouseBatchSize || int(time.Since(clickhouseBatchesTimeMap[batchName]).Seconds()) >= *clickhouseBatchTimeout) && clickhouseBatchesSendMap[batchName] != true {
		clickhouseBatchesSendMap[batchName] = true
		// Batch timeout debug
		// log.Printf("[%s] Timeout debug. Batch: %s Time: %v Timeout: %v", consumerName, batchName, int(time.Since(clickhouseBatchesTimeMap[batchName]).Seconds()), *clickhouseBatchTimeout)

		_, ok := clickhouseBatchesMap[batchName]
		if ok {
			// Sending to Clickhouse database
			err = clickhouseBatchesMap[batchName].Send()
			if err != nil {
				log.Printf("[%s] batch sending error: %s %s", consumerName, batchName, err)
				// Monitoring
				monitoringBatchesSendingErrors.WithLabelValues(consumerName, batchName, tableName).Inc()
			} else {
				endTime := time.Since(clickhouseBatchesTimeMap[batchName])
				log.Printf("[%s] batch %s sended (elements %v) (time %s)\n", consumerName, batchName, clickhouseBatchesSizeMap[batchName], endTime)
				// Monitoring
				monitoringBatchesSent.WithLabelValues(consumerName, batchName, tableName).Inc()
			}
			// Remove batch
			delete(clickhouseBatchesSizeMap, batchName)
			delete(clickhouseBatchesMap, batchName)
			// Hotfix because you got `00-00-0000 00:00:00` if try to delete it. And finally it can break batch sending
			clickhouseBatchesTimeMap[batchName] = time.Now()
			log.Printf("[%s] batch %s deleted\n", consumerName, batchName)
			clickhouseBatchesSendMap[batchName] = false
			clickhouseMutex.Unlock()
			clickhouseBatchesSizeMutex.Unlock()
			return nil
		}

		clickhouseMutex.Unlock()
		clickhouseBatchesSizeMutex.Unlock()
		// Try to catch some weird cases when batch doesn't exist
		log.Printf("[%s] batch (%s) doesn't not exist anymore (sending phase)", consumerName, batchName)
		return err
	}

	clickhouseMutex.Unlock()
	clickhouseBatchesSizeMutex.Unlock()
	return nil
}

// getColumnTypeWithName returns the ClickHouse data type for a value, checking for column name overrides first
func getColumnTypeWithName(value interface{}, columnName string) string {
	// Check if we have a configured override for this column name
	if columnName != "" && columnTypeOverrides != nil {
		if overrideType, exists := columnTypeOverrides[columnName]; exists {
			// For debug
			// log.Printf("Using configured type override for column '%s': %s", columnName, overrideType)
			return overrideType
		}
	}

	// For debug
	//log.Print(reflect.ValueOf(value))
	//log.Print(reflect.TypeOf(value))
	switch value.(type) {
	case string:
		// Try to extract date from string
		_, err := time.Parse("2006-01-02 15:04:05.000", fmt.Sprint(value))
		if err == nil {
			return "DateTime64"
		}
		return "String"

	case int, int32, int64:
		return "Int64"
	case float32, float64:
		return "Float64"
	case time.Time:
		return "DateTime64"
	default:
		// Fallback to string if the data type is unknown
		return "String"
	}
}

// convertValueToConfiguredType converts a value to match the configured column type
func convertValueToConfiguredType(value interface{}, columnName string) (interface{}, error) {
	if columnName == "" || columnTypeOverrides == nil {
		return value, nil
	}

	configuredType, exists := columnTypeOverrides[columnName]
	if !exists {
		return value, nil
	}

	// Convert value to match configured type
	switch configuredType {
	case "String":
		return fmt.Sprintf("%v", value), nil
	case "Int64":
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case float32:
			return int64(v), nil
		case float64:
			return int64(v), nil
		case string:
			return strconv.ParseInt(v, 10, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to Int64 for column %s", value, columnName)
		}
	case "Float64":
		switch v := value.(type) {
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case int32:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case string:
			return strconv.ParseFloat(v, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to Float64 for column %s", value, columnName)
		}
	case "DateTime64":
		switch v := value.(type) {
		case time.Time:
			return v, nil
		case string:
			// Try multiple datetime formats
			for _, format := range []string{
				"2006-01-02 15:04:05.000",
				"2006-01-02T15:04:05.000Z",
				"2006-01-02T15:04:05Z",
				"2006-01-02 15:04:05",
			} {
				if t, err := time.Parse(format, v); err == nil {
					return t, nil
				}
			}
			return nil, fmt.Errorf("cannot parse string '%s' as DateTime64 for column %s", v, columnName)
		default:
			return nil, fmt.Errorf("cannot convert %T to DateTime64 for column %s", value, columnName)
		}
	default:
		return value, nil
	}
}

// sortSlice just sort slice
func sortSlice(strings []string) {
	sort.Strings(strings)
}

// sortMap sort map and return sorted map + sorted slices with column names and values
// Now includes value conversion based on configured column types
func sortMap(myMap map[string]interface{}) (map[string]interface{}, []string, []interface{}) {

	// Get the keys of the map
	keys := make([]string, 0, len(myMap))
	for k := range myMap {
		keys = append(keys, k)
	}

	// sortSlice the keys
	sort.Strings(keys)

	// Create a new ordered map with the sorted keys
	var orderedFieldsNames []string
	var orderedFieldsValues []interface{}

	orderedMap := make(map[string]interface{})
	for _, k := range keys {
		// Convert value if there's a configured type override
		convertedValue, err := convertValueToConfiguredType(myMap[k], k)
		if err != nil {
			log.Printf("Warning: Failed to convert value for column '%s': %v. Using original value.", k, err)
			convertedValue = myMap[k]
		}

		orderedFieldsNames = append(orderedFieldsNames, k)
		orderedFieldsValues = append(orderedFieldsValues, convertedValue)
		orderedMap[k] = convertedValue
	}

	return orderedMap, orderedFieldsNames, orderedFieldsValues
}

// alterTable function do alter table in clickhouse database when json have column which doesn't exit in table structure
func alterTable(ctx context.Context, conn clickhouse.Conn, tableName string, data map[string]interface{}, consumerName string) error {
	log.Printf("[%s] Try to make alter for table: %v", consumerName, tableName)
	clusterQueryPart := onClusterQuery()
	var result []struct {
		Name string
		Type string
	}

	if err := conn.Select(ctx, &result, "SELECT name AS Name, type AS Type FROM system.columns WHERE table = ? AND database = ?", physicalTableName(tableName), *clickhouseDatabaseName); err != nil {
		log.Printf("SELECT column metadata error")
		return err
	}

	// Create a map to store the Name strings for each Type string
	resultMap := make(map[string]string)

	// Iterate over the result slice and add each element to the map
	for _, r := range result {
		resultMap[r.Name] = r.Type
	}

	var previous string
	i := 0
	isCluster := len(*clickhouseClusterName) > 0
	for key := range data {
		_, ok := resultMap[key]
		if !ok {
			alterTableQuery := buildAlterTableQuery(physicalTableName(tableName), clusterQueryPart, key, getColumnTypeWithName(data[key], key), previous)
			println(alterTableQuery)

			// Only alter Distributed table if we're using a cluster
			if isCluster {
				alterDistTableQuery := buildAlterTableQuery(distTablePrefix+tableName, clusterQueryPart, key, getColumnTypeWithName(data[key], key), previous)
				err := conn.Exec(ctx, alterDistTableQuery)
				if err != nil {
					return err
				}
			}

			err := conn.Exec(ctx, alterTableQuery)
			if err != nil {
				return err
			}
			log.Printf("[%s] Table %s altered because of new column: %s %s", consumerName, tableName, key, resultMap[key])
		}
		if i > 0 {
			previous = "AFTER " + quoteIdentifier(key)
		}
		i++
	}
	return nil
}

func buildAlterTableQuery(tableName, clusterQueryPart, column, columnType, position string) string {
	return fmt.Sprintf("ALTER TABLE %s %s ADD COLUMN %s %s %s", qualifiedTable(tableName), clusterQueryPart, quoteIdentifier(column), columnType, position)
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Create a new table in clickhouse database
func createTable(conn clickhouse.Conn, ctx context.Context, tableName string, payloadMap map[string]interface{}) error {
	createTableQuery, createDistTableQuery := buildCreateTableQueries(tableName, payloadMap)
	err := conn.Exec(ctx, createTableQuery)
	if err != nil {
		return err
	}

	if createDistTableQuery != "" {
		err = conn.Exec(ctx, createDistTableQuery)
		if err != nil {
			return err
		}
	}
	return nil
}

// buildCreateTableQueries returns the base table DDL and, in cluster mode, the Distributed table DDL.
func buildCreateTableQueries(tableName string, payloadMap map[string]interface{}) (string, string) {
	clusterQueryPart := onClusterQuery()
	var createTableColumns []string
	for key, value := range payloadMap {
		createTableColumns = append(createTableColumns, fmt.Sprintf("%s %s", quoteIdentifier(key), getColumnTypeWithName(value, key)))
	}
	sortSlice(createTableColumns)
	columns := strings.Join(createTableColumns, ",")

	isCluster := len(*clickhouseClusterName) > 0

	engine := "ReplacingMergeTree()"
	if isCluster {
		engine = "ReplicatedReplacingMergeTree()"
	}

	createTableQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s (%s) ENGINE = %s PARTITION BY toYYYYMM(_timestamp) PRIMARY KEY eventn_ctx_event_id ORDER BY eventn_ctx_event_id SETTINGS index_granularity = 8192", qualifiedTable(physicalTableName(tableName)), clusterQueryPart, columns, engine)
	if !isCluster {
		return createTableQuery, ""
	}

	createDistTableQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s (%s) ENGINE = Distributed(%s, %s, %s, rand())", qualifiedTable(distTablePrefix+tableName), clusterQueryPart, columns, quoteString(*clickhouseClusterName), quoteString(*clickhouseDatabaseName), quoteString(tableName))
	return createTableQuery, createDistTableQuery
}

// addServiceColumns sets the connector-owned columns, overriding any client-supplied values.
func addServiceColumns(payloadMap map[string]interface{}) {
	payloadMap["api_key"] = ""
	payloadMap["_timestamp"] = time.Now()
	payloadMap["eventn_ctx_event_id"] = uuid.New().String()
}

// distTablePrefix is the historical prefix used for Distributed tables in cluster deployments.
const distTablePrefix = "dist_"

func useLegacyDistPrefix() bool {
	return *clickhouseLegacyDistPrefix && len(*clickhouseClusterName) == 0
}

func physicalTableName(tableName string) string {
	if useLegacyDistPrefix() {
		return distTablePrefix + tableName
	}
	return tableName
}

func onClusterQuery() string {
	// Add `ON CLUSTER ...` suffix to query ( for clickhouse cluster )
	clusterQueryPart := ""
	if len(*clickhouseClusterName) > 0 {
		clusterQueryPart = fmt.Sprintf("ON CLUSTER %s", quoteIdentifier(*clickhouseClusterName))
	}
	return clusterQueryPart
}
