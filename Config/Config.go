package Config

import "time"

// Config holds the configuration for Redis Streams client
type Config struct {
	Redis       RedisConfig       `yaml:"redis"`
	Streams     StreamsConfig     `yaml:"streams"`
	Consumers   ConsumersConfig   `yaml:"consumers"`
	Topics      []TopicConfig     `yaml:"topics"`
	Monitoring  MonitoringConfig  `yaml:"monitoring"`
	Logging     LoggingConfig     `yaml:"logging"`
	Performance PerformanceConfig `yaml:"performance"`
}

// PerformanceConfig holds performance optimization settings
type PerformanceConfig struct {
	EnablePipelining     bool          `yaml:"enable_pipelining"`
	EnableCompression    bool          `yaml:"enable_compression"`
	EnableKeepAlive      bool          `yaml:"enable_keep_alive"`
	KeepAliveInterval    time.Duration `yaml:"keep_alive_interval"`
	MaxConcurrentStreams int           `yaml:"max_concurrent_streams"`
	WorkerPoolSize       int           `yaml:"worker_pool_size"`
	BatchFlushInterval   time.Duration `yaml:"batch_flush_interval"`
	MemoryPoolSize       int           `yaml:"memory_pool_size"`
}

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	Host         string        `yaml:"host"`
	Port         int           `yaml:"port"`
	Password     string        `yaml:"password"`
	Database     int           `yaml:"database"`
	PoolSize     int           `yaml:"pool_size"`
	MinIdleConns int           `yaml:"min_idle_conns"`
	MaxRetries   int           `yaml:"max_retries"`
	RetryBackoff time.Duration `yaml:"retry_backoff"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	PoolTimeout  time.Duration `yaml:"pool_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
	ClientName   string        `yaml:"client_name"`
	TLS          TLSConfig     `yaml:"tls"`
	// Performance optimizations
	MaxConnAge       time.Duration `yaml:"max_conn_age"`
	MinRetryBackoff  time.Duration `yaml:"min_retry_backoff"`
	MaxRetryBackoff  time.Duration `yaml:"max_retry_backoff"`
	DisableIndentity bool          `yaml:"disable_identity"`
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	CAFile             string `yaml:"ca_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// StreamsConfig holds stream-level configuration
type StreamsConfig struct {
	DefaultMaxLen       int64         `yaml:"default_max_len"`
	DefaultMaxAge       time.Duration `yaml:"default_max_age"`
	DefaultTrimStrategy string        `yaml:"default_trim_strategy"`
	AutoCreateStreams   bool          `yaml:"auto_create_streams"`
	TrimInterval        time.Duration `yaml:"trim_interval"`
}

// ConsumersConfig holds consumer-level configuration
type ConsumersConfig struct {
	DefaultBatchSize       int64         `yaml:"default_batch_size"`
	DefaultBlockTimeout    time.Duration `yaml:"default_block_timeout"`
	DefaultConsumerTimeout time.Duration `yaml:"default_consumer_timeout"`
	DefaultAutoAck         bool          `yaml:"default_auto_ack"`
	DeleteAfterAck         bool          `yaml:"delete_after_ack"`
	MaxPendingMessages     int64         `yaml:"max_pending_messages"`
	ClaimMinIdleTime       time.Duration `yaml:"claim_min_idle_time"`
	ClaimInterval          time.Duration `yaml:"claim_interval"`
}

// TopicConfig holds configuration for individual topics/channels
type TopicConfig struct {
	Name            string        `yaml:"name"`
	StreamName      string        `yaml:"stream_name"`
	MaxLen          int64         `yaml:"max_len"`
	MaxAge          time.Duration `yaml:"max_age"`
	TrimStrategy    string        `yaml:"trim_strategy"`
	ConsumerGroup   string        `yaml:"consumer_group"`
	DeadLetterTopic string        `yaml:"dead_letter_topic"`
	RetryAttempts   int           `yaml:"retry_attempts"`
	Description     string        `yaml:"description"`
}

// MonitoringConfig holds monitoring configuration
type MonitoringConfig struct {
	Enabled         bool          `yaml:"enabled"`
	MetricsInterval time.Duration `yaml:"metrics_interval"`
	HealthCheckPort int           `yaml:"health_check_port"`
	PrometheusPath  string        `yaml:"prometheus_path"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	Output     string `yaml:"output"`
	Structured bool   `yaml:"structured"`
}

// Logger interface for pluggable logging
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}
