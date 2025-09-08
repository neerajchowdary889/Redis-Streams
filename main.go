// Example usage of Redis Streams MQ module
// File: cmd/order-service/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	RDConfig "RedisStreams/Config"
	"RedisStreams/Health"
	RDLogging "RedisStreams/Logging"
	RDMetrics "RedisStreams/Metrics"
	redisstreams "RedisStreams/QueueModule"
)

// OrderEvent represents an order event
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Status     string    `json:"status"`
	Amount     float64   `json:"amount"`
	Items      []string  `json:"items"`
	Timestamp  time.Time `json:"timestamp"`
}

// PaymentRequest represents a payment request
type PaymentRequest struct {
	OrderID     string  `json:"order_id"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	PaymentType string  `json:"payment_type"`
}

func main() {
	// Load configuration
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "Config/config.yml"
	}

	config, err := RDConfig.LoadConfigFromPath(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create logger
	logger := RDLogging.NewLogger(config.Logging)

	// Create Redis Streams MQ client
	mq, err := redisstreams.New(config, logger)
	if err != nil {
		log.Fatalf("Failed to create Redis Streams MQ: %v", err)
	}
	defer mq.Close()

	// Start health check server
	healthServer := Health.NewHealthServer(mq, config, config.Monitoring.HealthCheckPort, logger, RDMetrics.NewMetrics())
	if err := healthServer.Start(); err != nil {
		log.Fatalf("Failed to start health server: %v", err)
	}
	defer healthServer.Stop()

	// Set up consumers
	setupConsumers(mq, logger)

	// Start publishing example messages
	go publishExampleMessages(mq, logger)

	logger.Info("Order service started",
		"config_path", configPath,
		"health_port", config.Monitoring.HealthCheckPort,
		"topics", len(config.Topics))

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutdown signal received, stopping service...")
}

// setupConsumers sets up message consumers for different topics
func setupConsumers(mq *redisstreams.RedisStreamMQ, logger RDConfig.Logger) {
	// Order created consumer
	orderCreatedConsumer := redisstreams.ConsumerConfig{
		TopicName:       "order.created",
		ConsumerName:    "order-processor-1",
		BatchSize:       5,
		BlockTimeout:    2 * time.Second,
		ConsumerTimeout: 30 * time.Second,
		AutoAck:         true,
		Description:     "Processes order creation events",
	}

	if err := mq.Subscribe(orderCreatedConsumer, handleOrderCreated); err != nil {
		logger.Error("Failed to subscribe to order.created", "error", err)
	}

	// Order updated consumer
	orderUpdatedConsumer := redisstreams.ConsumerConfig{
		TopicName:       "order.updated",
		ConsumerName:    "order-processor-1",
		BatchSize:       5,
		BlockTimeout:    2 * time.Second,
		ConsumerTimeout: 30 * time.Second,
		AutoAck:         true,
		Description:     "Processes order update events",
	}

	if err := mq.Subscribe(orderUpdatedConsumer, handleOrderUpdated); err != nil {
		logger.Error("Failed to subscribe to order.updated", "error", err)
	}

	// Payment request consumer
	paymentConsumer := redisstreams.ConsumerConfig{
		TopicName:       "payment.requested",
		ConsumerName:    "payment-processor-1",
		BatchSize:       3,
		BlockTimeout:    1 * time.Second,
		ConsumerTimeout: 45 * time.Second,
		AutoAck:         false, // Manual ack for payments
		MaxRetries:      5,
		Description:     "Processes payment requests",
	}

	if err := mq.Subscribe(paymentConsumer, handlePaymentRequest); err != nil {
		logger.Error("Failed to subscribe to payment.requested", "error", err)
	}
}

// handleOrderCreated processes order creation events
func handleOrderCreated(ctx context.Context, topic string, msgID string, fields map[string]interface{}) error {
	log.Printf("Processing order created event: %s", msgID)

	// Extract message data
	dataStr, ok := fields["data"].(string)
	if !ok {
		return fmt.Errorf("invalid message data format")
	}

	var orderEvent OrderEvent
	if err := json.Unmarshal([]byte(dataStr), &orderEvent); err != nil {
		return fmt.Errorf("failed to unmarshal order event: %w", err)
	}

	// Simulate order processing
	log.Printf("Processing order: ID=%s, Customer=%s, Amount=%.2f, Status=%s",
		orderEvent.OrderID, orderEvent.CustomerID, orderEvent.Amount, orderEvent.Status)

	// Simulate some processing time
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
	}

	// Business logic here...
	// For example, validate order, update inventory, etc.

	log.Printf("Successfully processed order: %s", orderEvent.OrderID)
	return nil
}

// handleOrderUpdated processes order update events
func handleOrderUpdated(ctx context.Context, topic string, msgID string, fields map[string]interface{}) error {
	log.Printf("Processing order updated event: %s", msgID)

	dataStr, ok := fields["data"].(string)
	if !ok {
		return fmt.Errorf("invalid message data format")
	}

	var orderEvent OrderEvent
	if err := json.Unmarshal([]byte(dataStr), &orderEvent); err != nil {
		return fmt.Errorf("failed to unmarshal order event: %w", err)
	}

	log.Printf("Order updated: ID=%s, New Status=%s", orderEvent.OrderID, orderEvent.Status)

	// Handle different status updates
	switch orderEvent.Status {
	case "confirmed":
		log.Printf("Order %s confirmed, preparing for fulfillment", orderEvent.OrderID)
	case "shipped":
		log.Printf("Order %s shipped, sending notification", orderEvent.OrderID)
	case "delivered":
		log.Printf("Order %s delivered, completing process", orderEvent.OrderID)
	case "cancelled":
		log.Printf("Order %s cancelled, initiating refund", orderEvent.OrderID)
	}

	return nil
}

// handlePaymentRequest processes payment requests with manual acknowledgment
func handlePaymentRequest(ctx context.Context, topic string, msgID string, fields map[string]interface{}) error {
	log.Printf("Processing payment request: %s", msgID)

	dataStr, ok := fields["data"].(string)
	if !ok {
		return fmt.Errorf("invalid message data format")
	}

	var paymentReq PaymentRequest
	if err := json.Unmarshal([]byte(dataStr), &paymentReq); err != nil {
		return fmt.Errorf("failed to unmarshal payment request: %w", err)
	}

	log.Printf("Processing payment: OrderID=%s, Amount=%.2f %s, Type=%s",
		paymentReq.OrderID, paymentReq.Amount, paymentReq.Currency, paymentReq.PaymentType)

	// Simulate payment processing
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
	}

	// Simulate payment success/failure
	if paymentReq.Amount > 1000 {
		log.Printf("High value payment requires manual approval: %s", paymentReq.OrderID)
		// Don't acknowledge - will be retried or handled manually
		return fmt.Errorf("payment amount too high for automatic processing")
	}

	log.Printf("Payment processed successfully: %s", paymentReq.OrderID)

	// Manual acknowledgment for successful payments
	// Note: In real implementation, you'd get the MQ instance and call AckMessage
	// mq.AckMessage("payment.requested", "payment-service", msgID)

	return nil
}

// publishExampleMessages publishes example messages for demonstration
func publishExampleMessages(mq *redisstreams.RedisStreamMQ, logger RDConfig.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	orderID := 1000

	for {
		select {
		case <-ticker.C:
			// Publish order created event
			orderEvent := OrderEvent{
				OrderID:    fmt.Sprintf("ORD-%d", orderID),
				CustomerID: fmt.Sprintf("CUST-%d", orderID%100),
				Status:     "created",
				Amount:     float64(50 + (orderID % 500)),
				Items:      []string{"item1", "item2"},
				Timestamp:  time.Now(),
			}

			headers := map[string]string{
				"source":      "order-service",
				"event_type":  "order_created",
				"customer_id": orderEvent.CustomerID,
			}

			if msgID, err := mq.Publish("order.created", orderEvent, headers); err != nil {
				logger.Error("Failed to publish order created event", "error", err)
			} else {
				logger.Debug("Published order created event",
					"message_id", msgID, "order_id", orderEvent.OrderID)
			}

			// Publish payment request
			paymentReq := PaymentRequest{
				OrderID:     orderEvent.OrderID,
				Amount:      orderEvent.Amount,
				Currency:    "USD",
				PaymentType: "credit_card",
			}

			if msgID, err := mq.Publish("payment.requested", paymentReq); err != nil {
				logger.Error("Failed to publish payment request", "error", err)
			} else {
				logger.Debug("Published payment request",
					"message_id", msgID, "order_id", paymentReq.OrderID)
			}

			orderID++

			// Publish batch messages every 10th iteration
			if orderID%10 == 0 {
				batchMessages := []redisstreams.BatchMessage{}
				for i := 0; i < 3; i++ {
					updateEvent := OrderEvent{
						OrderID:   fmt.Sprintf("ORD-%d", orderID-i-1),
						Status:    []string{"confirmed", "shipped", "delivered"}[i],
						Timestamp: time.Now(),
					}

					fields := make(map[string]interface{})
					if data, err := json.Marshal(updateEvent); err == nil {
						fields["data"] = string(data)
						fields["content_type"] = "application/json"
						fields["timestamp"] = time.Now().UnixNano()

						batchMessages = append(batchMessages, redisstreams.BatchMessage{
							Topic:  "order.updated",
							ID:     "*",
							Fields: fields,
						})
					}
				}

				if err := mq.PublishBatch(batchMessages); err != nil {
					logger.Error("Failed to publish batch messages", "error", err)
				} else {
					logger.Info("Published batch order updates", "count", len(batchMessages))
				}
			}

		case <-time.After(60 * time.Second):
			// Log metrics periodically
			metrics := mq.GetMetrics()
			logger.Info("Service metrics",
				"total_published", extractTotal(metrics["messages_published"]),
				"total_processed", extractTotal(metrics["messages_processed"]),
				"publish_errors", extractTotal(metrics["publish_errors"]),
				"processing_errors", extractTotal(metrics["processing_errors"]))
		}
	}
}

// extractTotal extracts total count from metrics map
func extractTotal(data interface{}) int64 {
	if topicMap, ok := data.(map[string]int64); ok {
		total := int64(0)
		for _, count := range topicMap {
			total += count
		}
		return total
	}
	return 0
}
