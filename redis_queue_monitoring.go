package main

import (
	"github.com/adjust/rmq/v5"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type queueStatsCounters struct {
	readyCount      prometheus.Gauge
	rejectedCount   prometheus.Gauge
	connectionCount prometheus.Gauge
	consumerCount   prometheus.Gauge
	unackedCount    prometheus.Gauge
}

func RecordRmqMetrics(connection rmq.Connection) {
	counters := registerCounters(connection)

	go func() {
		for {
			queues, _ := connection.GetOpenQueues()
			stats, _ := connection.CollectStats(queues)
			for queue, queueStats := range stats.QueueStats {
				if counter, ok := counters[queue]; ok {
					counter.readyCount.Set(float64(queueStats.ReadyCount))
					counter.rejectedCount.Set(float64(queueStats.RejectedCount))
					counter.connectionCount.Set(float64(queueStats.ConnectionCount()))
					counter.consumerCount.Set(float64(queueStats.ConsumerCount()))
					counter.unackedCount.Set(float64(queueStats.UnackedCount()))
				}
			}

			time.Sleep(1 * time.Second)
		}
	}()
}

func registerCounters(connection rmq.Connection) map[string]queueStatsCounters {
	rmqCounters := map[string]queueStatsCounters{}
	queues, _ := connection.GetOpenQueues()
	for _, queue := range queues {
		rmqCounters[queue] = queueStatsCounters{
			readyCount: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "rmq",
				Name:        "ready",
				Help:        "Number of ready messages on queue",
				ConstLabels: prometheus.Labels{"queue": queue},
			}),
			rejectedCount: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "rmq",
				Name:        "rejected",
				Help:        "Number of rejected messages on queue",
				ConstLabels: prometheus.Labels{"queue": queue},
			}),
			connectionCount: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "rmq",
				Name:        "connection",
				Help:        "Number of connections consuming a queue",
				ConstLabels: prometheus.Labels{"queue": queue},
			}),
			consumerCount: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "rmq",
				Name:        "consumer",
				Help:        "Number of consumers consuming messages for a queue",
				ConstLabels: prometheus.Labels{"queue": queue},
			}),
			unackedCount: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "rmq",
				Name:        "unacked",
				Help:        "Number of unacked messages on a consumer",
				ConstLabels: prometheus.Labels{"queue": queue},
			}),
		}

		prometheus.MustRegister(rmqCounters[queue].readyCount)
		prometheus.MustRegister(rmqCounters[queue].rejectedCount)
		prometheus.MustRegister(rmqCounters[queue].connectionCount)
		prometheus.MustRegister(rmqCounters[queue].consumerCount)
		prometheus.MustRegister(rmqCounters[queue].unackedCount)
	}

	return rmqCounters
}
