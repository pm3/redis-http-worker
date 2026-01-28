package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	eventsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Number of successfully processed events",
		},
		[]string{"stream", "type"},
	)

	eventsFailed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_failed_total",
			Help: "Number of failed events",
		},
		[]string{"stream", "type", "reason"},
	)

	eventAgeSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "event_age_seconds",
			Help:    "Time spent in stream (event creation -> processing start)",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"stream", "type"},
	)

	eventProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "event_processing_duration_seconds",
			Help:    "Time to process an event (HTTP call duration)",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)
)
