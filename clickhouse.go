package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"hash/fnv"
	"log"
	"net"
	"sort"
	"strings"
	"time"
)

// clickhouseConnect function for establishing connection with clickhouse database
func clickhouseConnect() (clickhouse.Conn, error) {
	var dialCount int
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
			return d.DialContext(ctx, "tcp", addr)
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
	var err error

	// Set table name from event_type json value ( each event - new table with own structure )
	tableName := getTableName(payloadMap)

	// Add timestamp, uuid and api_key createTableColumns ( additional columns )
	apiKey := authKey
	currentTime := time.Now()
	eventUuid := uuid.New()
	payloadMap["api_key"] = apiKey
	payloadMap["_timestamp"] = currentTime
	payloadMap["eventn_ctx_event_id"] = eventUuid.String()
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

	if err := conn.AsyncInsert(ctx, fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", *clickhouseDatabaseName, tableName, strings.Join(columnsNames, ","), strings.Join(convertToStringSlice(columnsValues), ",")), false); err != nil {
		if strings.Contains(err.Error(), "code: 16, message: No such column") {
			// Try to exec ALTER TABLE
			alterErr := alterTable(ctx, conn, tableName, payloadMap, consumerName)
			if alterErr != nil {
				clickhouseBatchesSizeMutex.Unlock()
				clickhouseMutex.Unlock()
				monitoringTableAlteringErrors.WithLabelValues(consumerName, "async_mode", tableName).Inc()
				return alterErr
			}
			monitoringTableAltered.WithLabelValues(consumerName, "async_mode", tableName).Inc()
			// Retry after alter
			return err
		} else {
			return err
		}
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
	var err error

	// Set table name from event_type json value ( each event - new table with own structure )
	tableName := getTableName(payloadMap)

	// Add timestamp, uuid and api_key createTableColumns ( additional columns )
	apiKey := authKey
	currentTime := time.Now()
	eventUuid := uuid.New()
	payloadMap["api_key"] = apiKey
	payloadMap["_timestamp"] = currentTime
	payloadMap["eventn_ctx_event_id"] = eventUuid.String()
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

		clickhouseBatchesMap[batchName], err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s (%s)", *clickhouseDatabaseName, tableName, strings.Join(columnsNames, ",")))
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
					clickhouseBatchesMap[batchName], retryErr = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s (%s)", *clickhouseDatabaseName, tableName, strings.Join(columnsNames, ",")))
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
			// Monitoring
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
		} else {
			clickhouseMutex.Unlock()
			clickhouseBatchesSizeMutex.Unlock()
			// Try to catch some weird cases when batch doesn't exist
			log.Printf("[%s] batch (%s) doesn't not exist anymore (sending phase)", consumerName, batchName)
			return err
		}
	}

	clickhouseMutex.Unlock()
	clickhouseBatchesSizeMutex.Unlock()
	return nil
}

// getColumnType returns the ClickHouse data type string for a given value
func getColumnType(value interface{}) string {
	//log.Print(reflect.ValueOf(value))
	//log.Print(reflect.TypeOf(value))
	switch value.(type) {
	case string:
		// Try to extract date from string
		_, err := time.Parse("2006-01-02 15:04:05.000", fmt.Sprint(value))
		if err == nil {
			return "DateTime64"
		} else {
			return "String"
		}
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

// sortSlice just sort slice
func sortSlice(strings []string) {
	sort.Strings(strings)
}

// sortMap sort map and return sorted map + sorted slices with column names and values
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
		orderedFieldsNames = append(orderedFieldsNames, k)
		orderedFieldsValues = append(orderedFieldsValues, myMap[k])
		orderedMap[k] = myMap[k]
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

	if err := conn.Select(ctx, &result, fmt.Sprintf("SELECT name AS Name, type AS Type FROM system.columns WHERE table='%s' AND database='%s'", tableName, *clickhouseDatabaseName)); err != nil {
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
	for key, _ := range data {
		_, ok := resultMap[key]
		if !ok {

			alterTableQuery := fmt.Sprintf("ALTER TABLE %s.%s %s ADD COLUMN %s %s %s", *clickhouseDatabaseName, tableName, clusterQueryPart, key, getColumnType(data[key]), previous)
			println(alterTableQuery)
			alterDistTableQuery := fmt.Sprintf("ALTER TABLE %s.dist_%s %s ADD COLUMN %s %s %s", *clickhouseDatabaseName, tableName, clusterQueryPart, key, getColumnType(data[key]), previous)

			err := conn.Exec(ctx, alterDistTableQuery)
			err = conn.Exec(ctx, alterTableQuery)
			if err != nil {
				return err
			}
			log.Printf("[%s] Table %s altered because of new column: %s %s", consumerName, tableName, key, resultMap[key])
		}
		if i > 0 {
			previous = "AFTER " + key
		}
		i++
	}
	return nil
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func createTable(conn clickhouse.Conn, ctx context.Context, tableName string, payloadMap map[string]interface{}) error {
	// Generate the SQL statement to create the table
	clusterQueryPart := onClusterQuery()
	var createTableColumns []string
	for key, value := range payloadMap {
		columnType := getColumnType(value)
		// DateTime64 Debug
		//if columnType == "DateTime64" {
		//	log.Printf("Found DateTime64 column: %s table: %s", key, tableName)
		//}
		createTableColumns = append(createTableColumns, fmt.Sprintf("%s %s", key, columnType))
	}
	sortSlice(createTableColumns)

	createTableQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s %s (%s) ENGINE = ReplicatedReplacingMergeTree() PARTITION BY toYYYYMM(_timestamp) PRIMARY KEY eventn_ctx_event_id ORDER BY eventn_ctx_event_id SETTINGS index_granularity = 8192", *clickhouseDatabaseName, tableName, clusterQueryPart, strings.Join(createTableColumns, ","))
	createDistTableQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.dist_%s %s (%s) ENGINE = Distributed('%s', '%s', '%s', rand())", *clickhouseDatabaseName, tableName, clusterQueryPart, strings.Join(createTableColumns, ","), *clickhouseClusterName, *clickhouseDatabaseName, tableName)
	err := conn.Exec(ctx, createTableQuery)
	if err != nil {
		return err
	}
	err = conn.Exec(ctx, createDistTableQuery)
	if err != nil {
		return err
	}
	return nil
}

func onClusterQuery() string {
	// Add `ON CLUSTER ...` suffix to query ( for clickhouse cluster )
	clusterQueryPart := ""
	if len(*clickhouseClusterName) > 0 {
		clusterQueryPart = fmt.Sprintf("ON CLUSTER %s", *clickhouseClusterName)
	}
	return clusterQueryPart
}

func getTableName(payloadMap map[string]interface{}) string {
	tableName := "default"
	if payloadMap["event_type"] != nil {
		tableName = payloadMap["event_type"].(string)
	}
	return tableName
}

func convertToStringSlice(interfaces []interface{}) []string {
	strings := make([]string, len(interfaces))
	for i, v := range interfaces {
		switch v.(type) {
		case time.Time:
			strings[i] = fmt.Sprintf("'%s'", v.(time.Time).Format("2006-01-02 15:04:05.000"))
		case float64:
			strings[i] = fmt.Sprintf("'%f'", v.(float64))
		default:
			// Fallback to string if the data type is unknown
			if str, ok := v.(*string); ok && str != nil {
				strings[i] = fmt.Sprintf("'%s'", *str)
			} else {
				strings[i] = fmt.Sprintf("'%s'", v.(string))
			}
		}
	}
	return strings
}
