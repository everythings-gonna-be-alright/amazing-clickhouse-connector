package main

import (
	"fmt"
	"github.com/adjust/rmq/v5"
	"github.com/avast/retry-go"
	"log"
	_ "net/http/pprof"
	"time"
)

const (
	prefetchLimit   = 1000
	pollDuration    = 100 * time.Millisecond
	reportBatchSize = 10000
	consumeDuration = 10 * time.Millisecond
	shouldLog       = false
)

type Consumer struct {
	name   string
	count  int
	before time.Time
}

func rmqConnect() {
	var err error
	retry.Do(
		func() error {
			go rmqLogErrors(redisErrChan)
			redisConnection, err = rmq.OpenConnection("clickhouse_connector", "tcp", *redisServer, 0, redisErrChan)
			if err != nil {
				monitoringRedisConnectErrors.WithLabelValues(*redisServer).Inc()
				log.Printf("redis connection error: %v", err)
				return err
			}
			return nil
		},
		retry.MaxDelay(10*time.Second),
		retry.Delay(3*time.Second),
	)
	log.Printf("Connected to redis queue: %s", *redisServer)
}

func NewConsumer(tag int) *Consumer {
	return &Consumer{
		name:   fmt.Sprintf("%s-%d", hostName, tag),
		count:  0,
		before: time.Now(),
	}
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {

	payload := delivery.Payload()

	rmqDebug("start consume %s", payload)
	time.Sleep(consumeDuration)

	consumer.count++
	if consumer.count%reportBatchSize == 0 {
		//duration := time.Now().Sub(consumer.before)
		//consumer.before = time.Now()
		//perSecond := time.Second / (duration / reportBatchSize)
		//log.Printf("[%s] consumed %d elements. RPS: %d", consumer.name, consumer.count, perSecond)
	}

	if err := delivery.Ack(); err != nil {
		rmqDebug("failed to ack %s: %s", payload, err)
	} else {
		element, err := createMapFromJSON(payload)
		if err != nil {
			log.Printf("Create map from json error: %v", err)
		}
		payloadMap, _ := element.(map[string]interface{})
		if *clickhouseAsyncMode == true {
			err = insertIntoClickhouseAsync(clickhouseConnection, payloadMap, consumer.name)
		} else {
			err = insertIntoClickhouse(clickhouseConnection, payloadMap, consumer.name)
		}
		if err != nil {
			log.Printf("Clickhouse error: %v", err)
			delivery.Reject()
		}
		// Monitoring
		monitoringDocumentsProcessed.WithLabelValues(consumer.name).Inc()
		rmqDebug("acked %s", payload)
		// For debug ( stop after first element )
		//os.Exit(0
	}
}

// rmqLogErrors function for RMQ logging and monitoring
func rmqLogErrors(errChan <-chan error) {
	for err := range errChan {
		// Send info about error to prometheus
		monitoringRedisConnectErrors.WithLabelValues(*redisServer).Inc()
		switch err := err.(type) {
		case *rmq.HeartbeatError:
			if err.Count == rmq.HeartbeatErrorLimit {
				log.Print("heartbeat error (limit): ", err)
			} else {
				log.Print("heartbeat error: ", err)
			}
		case *rmq.ConsumeError:
			log.Print("consume error: ", err)
		case *rmq.DeliveryError:
			log.Print("delivery error: ", err)
		default:
			log.Print("other error: ", err)
		}
	}
}

// rmqDebug function for RMQ debug
func rmqDebug(format string, args ...interface{}) {
	if shouldLog {
		log.Printf(format, args...)
	}
}

// rmqWrite function simple write some data to redis queue
func rmqWrite(queue rmq.Queue, element string) error {
	err := queue.Publish(element)
	return err
}

// rmqReturnRejected function check rejected queues and simple return elements back to basic queue
func rmqReturnRejected() {
	go func() error {
		ticker := time.NewTicker(10 * time.Second)
		for _ = range ticker.C {
			queues, _ := redisConnection.GetOpenQueues()
			stats, _ := redisConnection.CollectStats(queues)
			for queue, queueStats := range stats.QueueStats {
				if queueStats.RejectedCount > 100 {
					taskRejectedQueue, err := redisConnection.OpenQueue(queue)
					_, err = taskRejectedQueue.ReturnRejected(10000)
					if err != nil {
						return err
					}
					log.Printf("[%s] Returned %d rejected elements to processing queue", queue, queueStats.RejectedCount)
				}
			}
		}
		return nil
	}()
}

// rmqCleaner function check rejected queues and simple return elements back to basic queue
func rmqCleaner() {
	go func() error {
		ticker := time.NewTicker(60 * time.Second)
		cleaner := rmq.NewCleaner(redisConnection)
		for _ = range ticker.C {
			returned, err := cleaner.Clean()
			if err != nil {
				log.Printf("rmq failed to clean: %s", err)
				continue
			}
			if returned > 0 {
				log.Printf("rmq cleaned %d", returned)
			}
		}
		return nil
	}()
}
