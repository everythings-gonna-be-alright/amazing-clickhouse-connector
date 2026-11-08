package main

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/adjust/rmq/v5"
	"github.com/avast/retry-go"
	"github.com/fatih/color"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpmertics "github.com/slok/go-http-metrics/metrics/prometheus"
	"github.com/slok/go-http-metrics/middleware"
	"github.com/slok/go-http-metrics/middleware/std"

	"os"
	"os/signal"

	"sync"
	"syscall"
	"time"
)

var addr = flag.String("listen-address", ":6000", "The address to listen on for HTTP requests.")
var monitoringAddr = flag.String("prometheus-listen-address", ":6001", "The address to listen on for monitoring HTTP requests.")
var mode = flag.String("mode", "", "Mode can be server or writer")
var clickhouseServer = flag.String("clickhouseServer", "127.0.0.1:9000", "Clickhouse server IP addr")
var clickhouseUsername = flag.String("clickhouseUsername", "default", "Clickhouse username")
var clickhousePassword = flag.String("clickhousePassword", os.Getenv("CLICKHOUSE_PASSWORD"), "Clickhouse password (prefer the CLICKHOUSE_PASSWORD env var)")
var clickhouseTLS = flag.Bool("clickhouseTLS", false, "Enable TLS for the ClickHouse connection (required for ClickHouse Cloud / port 9440)")
var clickhouseTLSSkipVerify = flag.Bool("clickhouseTLSSkipVerify", false, "Skip TLS certificate verification (insecure, use only for testing)")
var clickhouseClusterName = flag.String("clickhouseClusterName", "test_cluster_two_shards", "Clickhouse cluster name")
var clickhouseLegacyDistPrefix = flag.Bool("clickhouseLegacyDistPrefix", false, "Keep the legacy 'dist_' table prefix in single instance mode (e.g. after migrating a cluster to ClickHouse Cloud) so existing queries against dist_<table> keep working")
var clickhouseDatabaseName = flag.String("clickhouseDatabaseName", "default", "Clickhouse database name")
var clickhouseBatchSize = flag.Int("clickhouseBatchSize", 1000, "Clickhouse batch size")
var clickhouseBatchTimeout = flag.Int("clickhouseBatchTimeout", 300, "Clickhouse batch timeout sec")
var clickhouseAsyncMode = flag.Bool("clickhouseAsyncMode", false, "Enable Clickhouse async insert mode instead batches")
var threads = flag.Int("threads", 10, "Number of threads")
var redisServer = flag.String("redisServer", "127.0.0.1:6379", "Redis queue server IP addr")
var redisPassword = flag.String("redisPassword", os.Getenv("REDIS_PASSWORD"), "Redis password (prefer the REDIS_PASSWORD env var)")
var redisConnection rmq.Connection
var clickhouseConnection clickhouse.Conn
var authKey = flag.String("authKey", os.Getenv("AUTH_KEY"), "Comma-separated list of accepted x-auth-token values (prefer the AUTH_KEY env var)")
var maxBodyBytes = flag.Int64("maxBodyBytes", 1<<20, "Maximum accepted request body size in bytes")
var maxColumns = flag.Int("maxColumns", 1000, "Maximum number of fields per event; larger events are dropped")
var columnTypesConfig = flag.String("columnTypesConfig", "", "JSON config for manual column type mapping (e.g., '{\"user_id\":\"String\",\"timestamp\":\"DateTime64\"}')")

// Monitoring
var monitoringDocumentsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_documents_processed", Help: "Number of processed documents"}, []string{"consumer_name"})

// var monitoringClickhouseConnectErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_clickhouse_connecting_errors", Help: "Number of clickhouse connecting errors"}, []string{"consumer_name"})
var monitoringRedisConnectErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_redis_connecting_errors", Help: "Number of redis connecting errors"}, []string{"server_name"})
var monitoringTableCreatingErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_tables_creating_queries_errors", Help: "Number of tables creating errors"}, []string{"consumer_name", "batch_name", "table_name"})
var monitoringTableCreateQueries = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_tables_creating_queries", Help: "Number of tables creating queries"}, []string{"consumer_name", "batch_name", "table_name"})
var monitoringTableAltered = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_tables_altered", Help: "Number of tables altering succeed operations"}, []string{"consumer_name", "batch_name", "table_name"})
var monitoringTableAlteringErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_tables_altering_errors", Help: "Number of tables altering errors"}, []string{"consumer_name", "batch_name", "table_name"})
var monitoringBatchAppended = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_batches_appended", Help: "Number of batches append errors"}, []string{"consumer_name", "batch_name", "table_name"})
var monitoringBatchAppendingErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_batches_appending_errors", Help: "Number of batches appending errors"}, []string{"consumer_name", "batch_name", "table_name"})
var monitoringBatchesSent = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_batches_sent", Help: "Number of sent batches"}, []string{"consumer_name", "batch_name", "table_name"})
var monitoringBatchesSendingErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_batches_sending_errors", Help: "Number of batches sending errors"}, []string{"consumer_name", "batch_name", "table_name"})
var monitoringBatchesSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_batch_size", Help: "Number of elements in batch"}, []string{"consumer_name", "batch_name"})

// Maps for batching
var clickhouseBatchesMap = map[string]driver.Batch{}
var clickhouseBatchesSizeMap = map[string]int{}
var clickhouseBatchesTimeMap = map[string]time.Time{}
var clickhouseBatchesSendMap = map[string]bool{}

// Counter for async insters
var clickhouseAsyncSize int

// Global column type overrides map
var columnTypeOverrides map[string]string

// Mutexes for batching parallelism
var clickhouseMutex = sync.RWMutex{}
var clickhouseBatchesSizeMutex = sync.RWMutex{}

var taskQueue rmq.Queue

var redisErrChan = make(chan error, 10)

var hostName, _ = os.Hostname()

var authKeys [][]byte

const minAuthKeyLength = 16

var errNotJSONObject = errors.New("body must be a JSON object or an array of JSON objects")

// BodyToJSON splits a request body into one compact JSON document per event
func BodyToJSON(body []byte) ([]string, error) {
	trimmed := bytes.TrimSpace(body)
	var documents []map[string]interface{}
	if len(trimmed) > 0 && trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &documents); err != nil {
			return nil, err
		}
	} else {
		var document map[string]interface{}
		if err := json.Unmarshal(trimmed, &document); err != nil {
			return nil, err
		}
		documents = append(documents, document)
	}

	jsonArray := make([]string, 0, len(documents))
	for _, document := range documents {
		if document == nil {
			return nil, errNotJSONObject
		}
		jsonElement, err := json.Marshal(document)
		if err != nil {
			return nil, err
		}
		jsonArray = append(jsonArray, string(jsonElement))
	}
	return jsonArray, nil
}

func parseAuthKeys(raw string) ([][]byte, error) {
	var keys [][]byte
	for _, key := range strings.Split(raw, ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if len(key) < minAuthKeyLength {
			return nil, fmt.Errorf("auth key is shorter than %d characters", minAuthKeyLength)
		}
		keys = append(keys, []byte(key))
	}
	if len(keys) == 0 {
		return nil, errors.New("no auth key configured, set AUTH_KEY")
	}
	return keys, nil
}

func isAuthorized(token string) bool {
	if token == "" {
		return false
	}
	authorized := 0
	for _, key := range authKeys {
		authorized |= subtle.ConstantTimeCompare([]byte(token), key)
	}
	return authorized == 1
}

func parseRequest(rw http.ResponseWriter, req *http.Request) {
	if !isAuthorized(req.Header.Get("x-auth-token")) {
		http.Error(rw, "Access denied", http.StatusUnauthorized)
		log.Printf("Access denied")
		return
	}
	if req.ContentLength == 0 {
		http.Error(rw, "Empty request body", http.StatusBadRequest)
		log.Printf("Empty request body")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(rw, req.Body, *maxBodyBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(rw, "Request body too large", http.StatusRequestEntityTooLarge)
			log.Printf("Request body exceeds %d bytes", *maxBodyBytes)
			return
		}
		http.Error(rw, "Body reading error", http.StatusBadRequest)
		log.Printf("Body reading error: %s", err)
		return
	}
	jsonArray, err := BodyToJSON(body)
	if err != nil {
		http.Error(rw, "Invalid JSON", http.StatusBadRequest)
		log.Printf("Invalid JSON (%d bytes): %v", len(body), err)
		return
	}
	for _, v := range jsonArray {
		err := rmqWrite(taskQueue, v)
		if err != nil {
			log.Printf("Write to Redis error: %s", err)
		}
	}
	rw.Header().Set("Content-Type", "text/plain")
	rw.WriteHeader(http.StatusOK)
	fmt.Fprintf(rw, "ok\n")
}

func probe(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "text/plain")
	rw.WriteHeader(http.StatusOK)
	fmt.Fprintf(rw, "ok\n")
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		// Must exceed the gateway's upstream keep-alive (Envoy default 1h) to avoid reset races on reused connections.
		IdleTimeout:    75 * time.Minute,
		MaxHeaderBytes: 64 << 10,
	}
}

func startMonitoringServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Printf("Started monitoring web server at %s\n", *monitoringAddr)
		if err := newHTTPServer(*monitoringAddr, mux).ListenAndServe(); err != nil {
			log.Printf("http.ListenAndServer: %v\n", err)
		}
	}()
}

func recordMetrics() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
		}
	}()
}

// initializeColumnTypeOverrides parses the column type configuration
func initializeColumnTypeOverrides() error {
	columnTypeOverrides = make(map[string]string)

	if *columnTypesConfig == "" {
		log.Println("No column type overrides configured")
		return nil
	}

	// Parse JSON configuration
	err := json.Unmarshal([]byte(*columnTypesConfig), &columnTypeOverrides)
	if err != nil {
		return fmt.Errorf("failed to parse column types config: %v", err)
	}

	// Validate supported ClickHouse types
	supportedTypes := map[string]bool{
		"String":     true,
		"Int64":      true,
		"Float64":    true,
		"DateTime64": true,
		"UInt64":     true,
		"Int32":      true,
		"UInt32":     true,
		"Boolean":    true,
	}

	for column, columnType := range columnTypeOverrides {
		if !supportedTypes[columnType] {
			return fmt.Errorf("unsupported column type '%s' for column '%s'. Supported types: %v",
				columnType, column, getSupportedTypesSlice(supportedTypes))
		}
		log.Printf("Column type override: %s -> %s", column, columnType)
	}

	log.Printf("Loaded %d column type overrides", len(columnTypeOverrides))
	return nil
}

// getSupportedTypesSlice returns a slice of supported types for error messages
func getSupportedTypesSlice(supportedTypes map[string]bool) []string {
	var types []string
	for t := range supportedTypes {
		types = append(types, t)
	}
	return types
}

func main() {
	flag.Parse()

	// Initialize column type overrides
	if err := initializeColumnTypeOverrides(); err != nil {
		log.Fatalf("Failed to initialize column type overrides: %v", err)
	}

	recordMetrics()
	d := color.New(color.FgCyan, color.Bold)
	d.Println("AMAZING CLICKHOUSE CONNECTOR WELCOMES YOU 💖")
	fmt.Println("Application mode:", *mode)
	var err error
	if *mode == "consumer" {
		fmt.Println("Clickhouse batch size:", *clickhouseBatchSize)
		fmt.Println("Number of internal threads:", *threads)
		// Connect to Redis
		rmqConnect()
		// Monitoring for redis queues
		RecordRmqMetrics(redisConnection)
		// connect to Clickhouse + retry
		err = retry.Do(
			func() error {
				clickhouseConnection, err = clickhouseConnect()
				if err != nil {
					log.Printf("Clickhouse redisConnection error: %v\n", err)
					return err
				}
				return nil
			},
			retry.MaxDelay(10*time.Second),
		)

		log.Printf("Connected to clickhouse: %s", *clickhouseServer)
		// Open redis queue and start consuming messages + retry

		startMonitoringServer()

		// Start consume queue, process messages and send them to DB
		err = retry.Do(
			func() error {
				taskQueue, err := redisConnection.OpenQueue("clickhouse_connector")
				if err != nil {
					log.Printf("Redis queue connecting error: %v\n", err)
					return err
				}

				// Ticker which try to return 10000 elements from rejected queue
				rmqReturnRejected()

				// Ticker which keep queues in clean state
				rmqCleaner()

				err = taskQueue.StartConsuming(prefetchLimit, pollDuration)
				if err != nil {
					log.Printf("Redis consuming error: %v", err)
					return err
				}

				for i := 0; i < *threads; i++ {
					name := fmt.Sprintf("%s-%d", hostName, i)
					if _, err := taskQueue.AddConsumer(name, NewConsumer(i)); err != nil {
						log.Printf("Redis add consumer error: %v\n", err)
						return err
					}
				}

				signals := make(chan os.Signal, 1)
				signal.Notify(signals, syscall.SIGTERM)
				defer signal.Stop(signals)

				<-signals // wait for signal
				go func() {
					<-signals // hard exit on second signal (in case shutdown gets stuck)
					os.Exit(1)
				}()

				<-redisConnection.StopAllConsuming() // wait for all Consume() calls to finish
				if err != nil {
					log.Printf("Redis consume error: %v\n", err)
					return err
				}
				return nil
			},
			retry.MaxDelay(10*time.Second),
		)

	}
	if *mode == "server" {
		authKeys, err = parseAuthKeys(*authKey)
		if err != nil {
			log.Fatalf("Invalid auth key configuration: %v", err)
		}
		// Connect to redis
		rmqConnect()
		taskQueue, err = redisConnection.OpenQueue("clickhouse_connector")
		if err != nil {
			log.Fatalf("Redis queue opening error: %v", err)
		}

		// Create our middleware.
		httpMonitoring := middleware.New(middleware.Config{
			Recorder: httpmertics.NewRecorder(httpmertics.Config{}),
		})

		// Web server
		server := http.NewServeMux()
		server.HandleFunc("/", probe)
		server.HandleFunc("/api/v1/s2s/event", parseRequest)
		// Wrap our main handler, we pass empty handler ID so the middleware inferes
		// the handler label from the URL.
		h := std.Handler("", httpMonitoring, server)
		go func() {
			log.Printf("Started web server at %s\n", *addr)
			if err := newHTTPServer(*addr, h).ListenAndServe(); err != nil {
				log.Printf("http.ListenAndServer: %v\n", err)
			}
		}()

		startMonitoringServer()

		// Wait until some signal is captured.
		sigC := make(chan os.Signal, 1)
		signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)
		<-sigC

	} else {
		log.Print("Select correct mode:\n -mode server or -mode consumer\n")
	}
}
