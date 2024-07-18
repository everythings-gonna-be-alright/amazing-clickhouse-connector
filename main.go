package main

import (
	"encoding/json"
	"flag"
	"fmt"
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
	"io"
	"log"
	"net/http"
	//	_ "net/http/pprof"
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
var clickhousePassword = flag.String("clickhousePassword", "", "Clickhouse password")
var clickhouseClusterName = flag.String("clickhouseClusterName", "test_cluster_two_shards", "Clickhouse cluster name")
var clickhouseDatabaseName = flag.String("clickhouseDatabaseName", "default", "Clickhouse database name")
var clickhouseBatchSize = flag.Int("clickhouseBatchSize", 1000, "Clickhouse batch size")
var clickhouseBatchTimeout = flag.Int("clickhouseBatchTimeout", 300, "Clickhouse batch timeout sec")
var clickhouseAsyncMode = flag.Bool("clickhouseAsyncMode", false, "Enable Clickhouse async insert mode instead batches")
var threads = flag.Int("threads", 10, "Number of threads")
var redisServer = flag.String("redisServer", "127.0.0.1:6379", "Redis queue server IP addr")
var redisConnection rmq.Connection
var clickhouseConnection clickhouse.Conn
var authKey = flag.String("authKey", "c1110b00-00x0-111e-110c-101b22x1111d", "Auth key for web server")

// Monitoring
var monitoringDocumentsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_documents_processed", Help: "Number of processed documents"}, []string{"consumer_name"})
var monitoringClickhouseConnectErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_connector_clickhouse_connecting_errors", Help: "Number of clickhouse connecting errors"}, []string{"consumer_name"})
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

// Mutexes for batching parallelism
var clickhouseMutex = sync.RWMutex{}
var clickhouseBatchesSizeMutex = sync.RWMutex{}

var taskQueue rmq.Queue

var redisErrChan = make(chan error, 10)

var hostName, _ = os.Hostname()

func BodyToJson(body []byte, rw http.ResponseWriter) ([]string, error) {
	var err error
	var jsonArray []string
	var dataMap []map[string]interface{}
	var dataSingleString interface{}
	// Logic for array of json documents
	err = json.Unmarshal(body, &dataMap)
	if err != nil {
		// Logic for handling single json document
		err = nil
		err = json.Unmarshal(body, &dataSingleString)
		if err != nil {
			http.Error(rw, "Invalid JSON", http.StatusBadRequest)
			log.Printf("Invalid JSON  %v\n%v", err, string(body))
			return nil, err
		}
	}

	if len(dataMap) > 0 {
		for _, element := range dataMap {
			jsonElement, _ := json.Marshal(element)
			jsonArray = append(jsonArray, string(jsonElement))
		}
	} else {
		jsonElement, _ := json.Marshal(dataSingleString)
		jsonArray = append(jsonArray, string(jsonElement))
	}

	return jsonArray, err
}

func parseRequest(rw http.ResponseWriter, req *http.Request) {
	defer promhttp.Handler()
	if req.Header.Get("x-auth-token") != *authKey {
		http.Error(rw, "Access denied", http.StatusUnauthorized)
		log.Printf("Access denied")
		return
	}
	if req.ContentLength == 0 {
		http.Error(rw, "Empty request body", http.StatusBadRequest)
		log.Printf("Empty request body")
		return
	}
	// log.Printf("Handle request %s from %s", req.URL, req.RemoteAddr)
	stringWithJson, err := io.ReadAll(req.Body)
	if err != nil {
		log.Printf("Body reading error: %S", err)
	}
	jsonArray, err := BodyToJson(stringWithJson, rw)
	if err != nil {
		return
	}
	for _, v := range jsonArray {
		err := rmqWrite(taskQueue, v)
		if err != nil {
			log.Printf("Write to Redis error: %s", err)
		}
	}
	rw.WriteHeader(http.StatusOK)
	rw.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(rw, "ok\n")
}

func probe(rw http.ResponseWriter, req *http.Request) {
	rw.WriteHeader(http.StatusOK)
	rw.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(rw, "ok\n")
}

func recordMetrics() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
		}
	}()
}

func main() {
	flag.Parse()
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

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			log.Printf("Started monitoring web server at %s\n", *monitoringAddr)
			err = http.ListenAndServe(*monitoringAddr, nil)
			if err != nil {
				log.Printf("http.ListenAndServer: %v\n", err)
			}
		}()

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
		// Connect to redis
		rmqConnect()
		taskQueue, err = redisConnection.OpenQueue("clickhouse_connector")

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
			err = http.ListenAndServe(*addr, h)
			if err != nil {
				log.Printf("http.ListenAndServer: %v\n", err)
			}
		}()

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			log.Printf("Started monitoring web server at %s\n", *monitoringAddr)
			err = http.ListenAndServe(*monitoringAddr, nil)
			if err != nil {
				log.Printf("http.ListenAndServer: %v\n", err)
			}
		}()

		// Wait until some signal is captured.
		sigC := make(chan os.Signal, 1)
		signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)
		<-sigC

	} else {
		log.Print("Select correct mode:\n -mode server or -mode consumer\n")
	}
}
