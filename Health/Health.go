package Health

import (
	RDConfig "RedisStreams/Config"
	RDLogging "RedisStreams/Logging"
	RDMetrics "RedisStreams/Metrics"
	"RedisStreams/QueueModule"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HealthServer provides HTTP endpoints for health checks and metrics
type HealthServer struct {
	mq      *QueueModule.RedisStreamMQ
	server  *http.Server
	logger  RDConfig.Logger
	metrics *RDMetrics.Metrics
	config  *RDConfig.Config
}

// NewHealthServer creates a new health check server
func NewHealthServer(mq *QueueModule.RedisStreamMQ, cfg *RDConfig.Config, port int, logger RDConfig.Logger, metrics *RDMetrics.Metrics) *HealthServer {
	if logger == nil {
		logger = &RDLogging.DefaultLogger{}
	}

	mux := http.NewServeMux()
	hs := &HealthServer{
		mq:      mq,
		logger:  logger,
		metrics: metrics,
		config:  cfg,
		server: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}

	// Register endpoints
	mux.HandleFunc("/health", hs.healthHandler)
	mux.HandleFunc("/health/ready", hs.readinessHandler)
	mux.HandleFunc("/health/live", hs.livenessHandler)
	mux.HandleFunc("/metrics", hs.metricsHandler)
	mux.HandleFunc("/metrics/prometheus", hs.prometheusHandler)
	mux.HandleFunc("/info", hs.infoHandler)
	mux.HandleFunc("/topics", hs.topicsHandler)

	return hs
}

// Start starts the health check server
func (hs *HealthServer) Start() error {
	hs.logger.Info("Starting health check server", "addr", hs.server.Addr)

	go func() {
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hs.logger.Error("Health server error", "error", err)
		}
	}()

	return nil
}

// Stop stops the health check server gracefully
func (hs *HealthServer) Stop() error {
	hs.logger.Info("Stopping health check server")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return hs.server.Shutdown(ctx)
}

// healthHandler provides overall health status
func (hs *HealthServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	health := hs.metrics.GetHealthStatus()

	// Add Redis connectivity check
	if err := hs.mq.Health(); err != nil {
		health["redis_connected"] = false
		health["redis_error"] = err.Error()
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		health["redis_connected"] = true
		w.WriteHeader(http.StatusOK)
	}

	// Add topic health
	topicHealth := make(map[string]interface{})
	for _, topicName := range hs.mq.ListTopics() {
		if info, err := hs.mq.GetTopicInfo(topicName); err == nil {
			topicHealth[topicName] = map[string]interface{}{
				"length":      info.Length,
				"last_id":     info.LastGeneratedID,
				"first_entry": info.FirstEntry,
			}
		} else {
			topicHealth[topicName] = map[string]interface{}{
				"error": err.Error(),
			}
		}
	}
	health["topics"] = topicHealth

	json.NewEncoder(w).Encode(health)
}

// readinessHandler checks if the service is ready to accept requests
func (hs *HealthServer) readinessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ready := true
	issues := []string{}

	// Check Redis connection
	if err := hs.mq.Health(); err != nil {
		ready = false
		issues = append(issues, fmt.Sprintf("Redis unhealthy: %v", err))
	}

	// Check if topics are accessible
	topics := hs.mq.ListTopics()
	if len(topics) == 0 {
		ready = false
		issues = append(issues, "No topics configured")
	}

	status := map[string]interface{}{
		"ready":     ready,
		"issues":    issues,
		"timestamp": time.Now().Unix(),
	}

	if ready {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(status)
}

// livenessHandler checks if the service is alive
func (hs *HealthServer) livenessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	status := map[string]interface{}{
		"alive":     true,
		"timestamp": time.Now().Unix(),
		"uptime":    hs.metrics.GetUptime().Seconds(),
	}

	json.NewEncoder(w).Encode(status)
}

// metricsHandler provides detailed metrics in JSON format
func (hs *HealthServer) metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	metrics := hs.mq.GetMetrics()
	json.NewEncoder(w).Encode(metrics)
}

// prometheusHandler provides metrics in Prometheus format
func (hs *HealthServer) prometheusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	metrics := hs.mq.GetMetrics()

	// Convert metrics to Prometheus format
	fmt.Fprintf(w, "# HELP redis_streams_messages_published_total Total number of messages published\n")
	fmt.Fprintf(w, "# TYPE redis_streams_messages_published_total counter\n")
	if published, ok := metrics["messages_published"].(map[string]int64); ok {
		for topic, count := range published {
			fmt.Fprintf(w, "redis_streams_messages_published_total{topic=\"%s\"} %d\n", topic, count)
		}
	}

	fmt.Fprintf(w, "# HELP redis_streams_messages_processed_total Total number of messages processed\n")
	fmt.Fprintf(w, "# TYPE redis_streams_messages_processed_total counter\n")
	if processed, ok := metrics["messages_processed"].(map[string]int64); ok {
		for topic, count := range processed {
			fmt.Fprintf(w, "redis_streams_messages_processed_total{topic=\"%s\"} %d\n", topic, count)
		}
	}

	fmt.Fprintf(w, "# HELP redis_streams_publish_errors_total Total number of publish errors\n")
	fmt.Fprintf(w, "# TYPE redis_streams_publish_errors_total counter\n")
	if publishErrors, ok := metrics["publish_errors"].(map[string]int64); ok {
		for topic, count := range publishErrors {
			fmt.Fprintf(w, "redis_streams_publish_errors_total{topic=\"%s\"} %d\n", topic, count)
		}
	}

	fmt.Fprintf(w, "# HELP redis_streams_processing_errors_total Total number of processing errors\n")
	fmt.Fprintf(w, "# TYPE redis_streams_processing_errors_total counter\n")
	if processingErrors, ok := metrics["processing_errors"].(map[string]int64); ok {
		for topic, count := range processingErrors {
			fmt.Fprintf(w, "redis_streams_processing_errors_total{topic=\"%s\"} %d\n", topic, count)
		}
	}

	fmt.Fprintf(w, "# HELP redis_streams_stream_length Current stream length\n")
	fmt.Fprintf(w, "# TYPE redis_streams_stream_length gauge\n")
	if streamLengths, ok := metrics["stream_lengths"].(map[string]int64); ok {
		for topic, length := range streamLengths {
			fmt.Fprintf(w, "redis_streams_stream_length{topic=\"%s\"} %d\n", topic, length)
		}
	}

	fmt.Fprintf(w, "# HELP redis_streams_avg_processing_time_ms Average processing time in milliseconds\n")
	fmt.Fprintf(w, "# TYPE redis_streams_avg_processing_time_ms gauge\n")
	if avgTimes, ok := metrics["avg_processing_time_ms"].(map[string]float64); ok {
		for topic, avgTime := range avgTimes {
			fmt.Fprintf(w, "redis_streams_avg_processing_time_ms{topic=\"%s\"} %.2f\n", topic, avgTime)
		}
	}

	fmt.Fprintf(w, "# HELP redis_streams_uptime_seconds Service uptime in seconds\n")
	fmt.Fprintf(w, "# TYPE redis_streams_uptime_seconds gauge\n")
	if uptime, ok := metrics["uptime_seconds"].(float64); ok {
		fmt.Fprintf(w, "redis_streams_uptime_seconds %.2f\n", uptime)
	}

	fmt.Fprintf(w, "# HELP redis_streams_connection_errors_total Total connection errors\n")
	fmt.Fprintf(w, "# TYPE redis_streams_connection_errors_total counter\n")
	if connErrors, ok := metrics["connection_errors"].(int64); ok {
		fmt.Fprintf(w, "redis_streams_connection_errors_total %d\n", connErrors)
	}

	fmt.Fprintf(w, "# HELP redis_streams_reconnections_total Total reconnections\n")
	fmt.Fprintf(w, "# TYPE redis_streams_reconnections_total counter\n")
	if reconnections, ok := metrics["reconnections"].(int64); ok {
		fmt.Fprintf(w, "redis_streams_reconnections_total %d\n", reconnections)
	}
}

// infoHandler provides general service information
func (hs *HealthServer) infoHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	info := map[string]interface{}{
		"service":    "redis-streams-mq",
		"version":    "1.0.0",
		"build_time": time.Now().Format(time.RFC3339), // In production, set at build time
		"go_version": "go1.21",                        // In production, use runtime.Version()
		"topics":     len(hs.mq.ListTopics()),
		"uptime":     hs.metrics.GetUptime().Seconds(),
	}
	if hs.config != nil {
		info["redis_addr"] = fmt.Sprintf("%s:%d", hs.config.Redis.Host, hs.config.Redis.Port)
		info["redis_db"] = hs.config.Redis.Database
		info["client_name"] = hs.config.Redis.ClientName
	}

	json.NewEncoder(w).Encode(info)
}

// topicsHandler provides information about configured topics
func (hs *HealthServer) topicsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	topics := make(map[string]interface{})

	for _, topicName := range hs.mq.ListTopics() {
		topicInfo := map[string]interface{}{
			"name": topicName,
		}

		// Get topic configuration
		if topic, exists := hs.mq.Topics[topicName]; exists {
			topicInfo["stream_name"] = topic.StreamName
			topicInfo["consumer_group"] = topic.ConsumerGroup
			topicInfo["max_len"] = topic.MaxLen
			topicInfo["max_age"] = topic.MaxAge.String()
			topicInfo["trim_strategy"] = topic.TrimStrategy
			topicInfo["retry_attempts"] = topic.RetryAttempts
			topicInfo["dead_letter_topic"] = topic.DeadLetterTopic
			topicInfo["description"] = topic.Description
		}

		// Get stream information
		if streamInfo, err := hs.mq.GetTopicInfo(topicName); err == nil {
			topicInfo["length"] = streamInfo.Length
			topicInfo["last_generated_id"] = streamInfo.LastGeneratedID
			topicInfo["first_entry"] = streamInfo.FirstEntry
			topicInfo["last_entry"] = streamInfo.LastEntry
			topicInfo["entries_added"] = streamInfo.EntriesAdded
		} else {
			topicInfo["stream_error"] = err.Error()
		}

		// Get metrics for this topic
		topicInfo["messages_published"] = hs.metrics.GetMessagesPublished(topicName)
		topicInfo["messages_processed"] = hs.metrics.GetMessagesProcessed(topicName)
		topicInfo["publish_errors"] = hs.metrics.GetPublishErrors(topicName)
		topicInfo["processing_errors"] = hs.metrics.GetProcessingErrors(topicName)
		topicInfo["avg_processing_time_ms"] = hs.metrics.GetAverageProcessingTime(topicName).Milliseconds()

		topics[topicName] = topicInfo
	}

	response := map[string]interface{}{
		"topics": topics,
		"total":  len(topics),
	}

	json.NewEncoder(w).Encode(response)
}
