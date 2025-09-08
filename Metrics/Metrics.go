package Metrics

import (
	"fmt"
	"sync"
	"time"
)

// Metrics holds various metrics for monitoring
type Metrics struct {
	mu sync.RWMutex

	// Message metrics
	messagesPublished map[string]int64
	messagesProcessed map[string]int64
	publishErrors     map[string]int64
	processingErrors  map[string]int64

	// Performance metrics
	processingTimes map[string][]time.Duration
	streamLengths   map[string]int64

	// Connection metrics
	connectionErrors int64
	reconnections    int64

	// Consumer metrics
	activeConsumers map[string]int64
	pendingMessages map[string]int64

	// Timestamps
	startTime   time.Time
	lastUpdated time.Time
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		messagesPublished: make(map[string]int64),
		messagesProcessed: make(map[string]int64),
		publishErrors:     make(map[string]int64),
		processingErrors:  make(map[string]int64),
		processingTimes:   make(map[string][]time.Duration),
		streamLengths:     make(map[string]int64),
		activeConsumers:   make(map[string]int64),
		pendingMessages:   make(map[string]int64),
		startTime:         time.Now(),
		lastUpdated:       time.Now(),
	}
}

// IncMessagesPublished increments published message count for a topic
func (m *Metrics) IncMessagesPublished(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messagesPublished[topic]++
	m.lastUpdated = time.Now()
}

// AddMessagesPublished adds to published message count for a topic
func (m *Metrics) AddMessagesPublished(topic string, count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messagesPublished[topic] += int64(count)
	m.lastUpdated = time.Now()
}

// IncMessagesProcessed increments processed message count for a topic
func (m *Metrics) IncMessagesProcessed(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messagesProcessed[topic]++
	m.lastUpdated = time.Now()
}

// IncPublishErrors increments publish error count for a topic
func (m *Metrics) IncPublishErrors(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishErrors[topic]++
	m.lastUpdated = time.Now()
}

// IncProcessingErrors increments processing error count for a topic
func (m *Metrics) IncProcessingErrors(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processingErrors[topic]++
	m.lastUpdated = time.Now()
}

// RecordProcessingTime records processing time for a topic
func (m *Metrics) RecordProcessingTime(topic string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Keep only last 100 measurements per topic to avoid memory issues
	times := m.processingTimes[topic]
	if len(times) >= 100 {
		times = times[1:]
	}
	m.processingTimes[topic] = append(times, duration)
	m.lastUpdated = time.Now()
}

// SetStreamLength sets current stream length for a topic
func (m *Metrics) SetStreamLength(topic string, length int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamLengths[topic] = length
	m.lastUpdated = time.Now()
}

// SetActiveConsumers sets active consumer count for a topic
func (m *Metrics) SetActiveConsumers(topic string, count int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeConsumers[topic] = count
	m.lastUpdated = time.Now()
}

// SetPendingMessages sets pending message count for a topic
func (m *Metrics) SetPendingMessages(topic string, count int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingMessages[topic] = count
	m.lastUpdated = time.Now()
}

// IncConnectionErrors increments connection error count
func (m *Metrics) IncConnectionErrors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectionErrors++
	m.lastUpdated = time.Now()
}

// IncReconnections increments reconnection count
func (m *Metrics) IncReconnections() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnections++
	m.lastUpdated = time.Now()
}

// GetMessagesPublished returns published message count for a topic
func (m *Metrics) GetMessagesPublished(topic string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.messagesPublished[topic]
}

// GetMessagesProcessed returns processed message count for a topic
func (m *Metrics) GetMessagesProcessed(topic string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.messagesProcessed[topic]
}

// GetProcessingErrors returns processing error count for a topic
func (m *Metrics) GetProcessingErrors(topic string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.processingErrors[topic]
}

// GetPublishErrors returns publish error count for a topic
func (m *Metrics) GetPublishErrors(topic string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.publishErrors[topic]
}

// GetAverageProcessingTime returns average processing time for a topic
func (m *Metrics) GetAverageProcessingTime(topic string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	times := m.processingTimes[topic]
	if len(times) == 0 {
		return 0
	}

	var total time.Duration
	for _, t := range times {
		total += t
	}
	return total / time.Duration(len(times))
}

// GetStreamLength returns current stream length for a topic
func (m *Metrics) GetStreamLength(topic string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streamLengths[topic]
}

// GetUptime returns uptime duration
func (m *Metrics) GetUptime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return time.Since(m.startTime)
}

// GetAll returns all metrics as a map
func (m *Metrics) GetAll() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Calculate average processing times
	avgProcessingTimes := make(map[string]float64)
	for topic, times := range m.processingTimes {
		if len(times) > 0 {
			var total time.Duration
			for _, t := range times {
				total += t
			}
			avgProcessingTimes[topic] = float64(total) / float64(len(times)) / float64(time.Millisecond)
		}
	}

	return map[string]interface{}{
		"uptime_seconds":         time.Since(m.startTime).Seconds(),
		"last_updated":           m.lastUpdated.Unix(),
		"messages_published":     m.messagesPublished,
		"messages_processed":     m.messagesProcessed,
		"publish_errors":         m.publishErrors,
		"processing_errors":      m.processingErrors,
		"avg_processing_time_ms": avgProcessingTimes,
		"stream_lengths":         m.streamLengths,
		"active_consumers":       m.activeConsumers,
		"pending_messages":       m.pendingMessages,
		"connection_errors":      m.connectionErrors,
		"reconnections":          m.reconnections,
	}
}

// GetHealthStatus returns health status based on metrics
func (m *Metrics) GetHealthStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := "healthy"
	issues := []string{}

	// Check for high error rates
	for topic, errors := range m.processingErrors {
		processed := m.messagesProcessed[topic]
		if processed > 0 {
			errorRate := float64(errors) / float64(processed) * 100
			if errorRate > 10.0 { // More than 10% error rate
				status = "unhealthy"
				issues = append(issues, fmt.Sprintf("High error rate for topic %s: %.2f%%", topic, errorRate))
			}
		}
	}

	// Check for connection issues
	if m.connectionErrors > 10 {
		status = "degraded"
		issues = append(issues, "High number of connection errors")
	}

	// Check for stale metrics (no updates in 5 minutes)
	if time.Since(m.lastUpdated) > 5*time.Minute {
		status = "stale"
		issues = append(issues, "Metrics not updated recently")
	}

	return map[string]interface{}{
		"status":     status,
		"issues":     issues,
		"last_check": time.Now().Unix(),
		"uptime":     time.Since(m.startTime).Seconds(),
	}
}

// Reset resets all metrics (useful for testing)
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messagesPublished = make(map[string]int64)
	m.messagesProcessed = make(map[string]int64)
	m.publishErrors = make(map[string]int64)
	m.processingErrors = make(map[string]int64)
	m.processingTimes = make(map[string][]time.Duration)
	m.streamLengths = make(map[string]int64)
	m.activeConsumers = make(map[string]int64)
	m.pendingMessages = make(map[string]int64)
	m.connectionErrors = 0
	m.reconnections = 0
	m.startTime = time.Now()
	m.lastUpdated = time.Now()
}

// GetSummary returns a summary of key metrics
func (m *Metrics) GetSummary() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	totalPublished := int64(0)
	totalProcessed := int64(0)
	totalPublishErrors := int64(0)
	totalProcessingErrors := int64(0)

	for _, count := range m.messagesPublished {
		totalPublished += count
	}
	for _, count := range m.messagesProcessed {
		totalProcessed += count
	}
	for _, count := range m.publishErrors {
		totalPublishErrors += count
	}
	for _, count := range m.processingErrors {
		totalProcessingErrors += count
	}

	return map[string]interface{}{
		"total_published":         totalPublished,
		"total_processed":         totalProcessed,
		"total_publish_errors":    totalPublishErrors,
		"total_processing_errors": totalProcessingErrors,
		"uptime_seconds":          time.Since(m.startTime).Seconds(),
		"topic_count":             len(m.messagesPublished),
	}
}
