package Config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ConfigLoader handles loading configuration from YAML files
type ConfigLoader struct {
	configPath string
	envPrefix  string
}

// NewConfigLoader creates a new configuration loader
func NewConfigLoader(configPath, envPrefix string) *ConfigLoader {
	if envPrefix == "" {
		envPrefix = "REDIS_STREAMS"
	}
	return &ConfigLoader{
		configPath: configPath,
		envPrefix:  envPrefix,
	}
}

// LoadConfig loads configuration from YAML file and environment variables
func (cl *ConfigLoader) LoadConfig() (*Config, error) {
	config := &Config{}

	// Load from file if provided
	if cl.configPath != "" {
		if err := cl.loadFromFile(config); err != nil {
			return nil, fmt.Errorf("failed to load config from file: %w", err)
		}
	}

	// Override with environment variables
	cl.loadFromEnv(config)

	// Validate and set defaults
	if err := cl.validateAndSetDefaults(config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}

// loadFromFile loads configuration from YAML file
func (cl *ConfigLoader) loadFromFile(config *Config) error {
	if _, err := os.Stat(cl.configPath); os.IsNotExist(err) {
		return fmt.Errorf("config file not found: %s", cl.configPath)
	}

	data, err := os.ReadFile(cl.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables in the YAML content
	expandedData := os.ExpandEnv(string(data))

	if err := yaml.Unmarshal([]byte(expandedData), config); err != nil {
		return fmt.Errorf("failed to unmarshal YAML config: %w", err)
	}

	return nil
}

// loadFromEnv loads configuration from environment variables
func (cl *ConfigLoader) loadFromEnv(config *Config) {
	// Redis configuration
	if host := cl.getEnv("REDIS_HOST"); host != "" {
		config.Redis.Host = host
	}
	if port := cl.getEnvInt("REDIS_PORT"); port > 0 {
		config.Redis.Port = port
	}
	if password := cl.getEnv("REDIS_PASSWORD"); password != "" {
		config.Redis.Password = password
	}
	if db := cl.getEnvInt("REDIS_DATABASE"); db >= 0 {
		config.Redis.Database = db
	}
	if poolSize := cl.getEnvInt("REDIS_POOL_SIZE"); poolSize > 0 {
		config.Redis.PoolSize = poolSize
	}
	if clientName := cl.getEnv("REDIS_CLIENT_NAME"); clientName != "" {
		config.Redis.ClientName = clientName
	}

	// TLS configuration
	if cl.getEnvBool("REDIS_TLS_ENABLED") {
		config.Redis.TLS.Enabled = true
	}
	if certFile := cl.getEnv("REDIS_TLS_CERT_FILE"); certFile != "" {
		config.Redis.TLS.CertFile = certFile
	}
	if keyFile := cl.getEnv("REDIS_TLS_KEY_FILE"); keyFile != "" {
		config.Redis.TLS.KeyFile = keyFile
	}
	if caFile := cl.getEnv("REDIS_TLS_CA_FILE"); caFile != "" {
		config.Redis.TLS.CAFile = caFile
	}

	// Timeout configurations
	if dialTimeout := cl.getEnvDuration("REDIS_DIAL_TIMEOUT"); dialTimeout > 0 {
		config.Redis.DialTimeout = dialTimeout
	}
	if readTimeout := cl.getEnvDuration("REDIS_READ_TIMEOUT"); readTimeout > 0 {
		config.Redis.ReadTimeout = readTimeout
	}
	if writeTimeout := cl.getEnvDuration("REDIS_WRITE_TIMEOUT"); writeTimeout > 0 {
		config.Redis.WriteTimeout = writeTimeout
	}

	// Logging configuration
	if level := cl.getEnv("LOG_LEVEL"); level != "" {
		config.Logging.Level = level
	}
	if format := cl.getEnv("LOG_FORMAT"); format != "" {
		config.Logging.Format = format
	}
	if output := cl.getEnv("LOG_OUTPUT"); output != "" {
		config.Logging.Output = output
	}
	if cl.getEnvBool("LOG_STRUCTURED") {
		config.Logging.Structured = true
	}

	// Monitoring configuration
	if cl.getEnvBool("MONITORING_ENABLED") {
		config.Monitoring.Enabled = true
	}
	if port := cl.getEnvInt("MONITORING_PORT"); port > 0 {
		config.Monitoring.HealthCheckPort = port
	}
	if interval := cl.getEnvDuration("MONITORING_METRICS_INTERVAL"); interval > 0 {
		config.Monitoring.MetricsInterval = interval
	}
}

// validateAndSetDefaults validates configuration and sets default values
func (cl *ConfigLoader) validateAndSetDefaults(config *Config) error {
	// Redis defaults
	if config.Redis.Host == "" {
		config.Redis.Host = "localhost"
	}
	if config.Redis.Port == 0 {
		config.Redis.Port = 6379
	}
	if config.Redis.PoolSize == 0 {
		config.Redis.PoolSize = 10
	}
	if config.Redis.MinIdleConns == 0 {
		config.Redis.MinIdleConns = 5
	}
	if config.Redis.MaxRetries == 0 {
		config.Redis.MaxRetries = 3
	}
	if config.Redis.RetryBackoff == 0 {
		config.Redis.RetryBackoff = 8 * time.Millisecond
	}
	if config.Redis.DialTimeout == 0 {
		config.Redis.DialTimeout = 5 * time.Second
	}
	if config.Redis.ReadTimeout == 0 {
		config.Redis.ReadTimeout = 3 * time.Second
	}
	if config.Redis.WriteTimeout == 0 {
		config.Redis.WriteTimeout = 3 * time.Second
	}
	if config.Redis.PoolTimeout == 0 {
		config.Redis.PoolTimeout = 4 * time.Second
	}
	if config.Redis.IdleTimeout == 0 {
		config.Redis.IdleTimeout = 5 * time.Minute
	}
	if config.Redis.ClientName == "" {
		config.Redis.ClientName = "redis-streams-client"
	}

	// Streams defaults
	if config.Streams.DefaultMaxLen == 0 {
		config.Streams.DefaultMaxLen = 1000000
	}
	if config.Streams.DefaultMaxAge == 0 {
		config.Streams.DefaultMaxAge = 24 * time.Hour
	}
	if config.Streams.DefaultTrimStrategy == "" {
		config.Streams.DefaultTrimStrategy = "MAXLEN"
	}
	if config.Streams.TrimInterval == 0 {
		config.Streams.TrimInterval = 5 * time.Minute
	}

	// Consumers defaults
	if config.Consumers.DefaultBatchSize == 0 {
		config.Consumers.DefaultBatchSize = 10
	}
	if config.Consumers.DefaultBlockTimeout == 0 {
		config.Consumers.DefaultBlockTimeout = 1 * time.Second
	}
	if config.Consumers.DefaultConsumerTimeout == 0 {
		config.Consumers.DefaultConsumerTimeout = 30 * time.Second
	}
	if config.Consumers.MaxPendingMessages == 0 {
		config.Consumers.MaxPendingMessages = 1000
	}
	if config.Consumers.ClaimMinIdleTime == 0 {
		config.Consumers.ClaimMinIdleTime = 30 * time.Second
	}
	if config.Consumers.ClaimInterval == 0 {
		config.Consumers.ClaimInterval = 60 * time.Second
	}

	// DeleteAfterAck default (disabled unless explicitly enabled)
	// No change needed; zero-value false is acceptable.

	// Logging defaults
	if config.Logging.Level == "" {
		config.Logging.Level = "INFO"
	}
	if config.Logging.Format == "" {
		config.Logging.Format = "text"
	}
	if config.Logging.Output == "" {
		config.Logging.Output = "stdout"
	}

	// Monitoring defaults
	if config.Monitoring.MetricsInterval == 0 {
		config.Monitoring.MetricsInterval = 30 * time.Second
	}
	if config.Monitoring.HealthCheckPort == 0 {
		config.Monitoring.HealthCheckPort = 8080
	}
	if config.Monitoring.PrometheusPath == "" {
		config.Monitoring.PrometheusPath = "/metrics"
	}

	// Validate topics
	if len(config.Topics) == 0 {
		return fmt.Errorf("no topics configured")
	}

	topicNames := make(map[string]bool)
	for i, topic := range config.Topics {
		if topic.Name == "" {
			return fmt.Errorf("topic at index %d has empty name", i)
		}

		if topicNames[topic.Name] {
			return fmt.Errorf("duplicate topic name: %s", topic.Name)
		}
		topicNames[topic.Name] = true

		// Set topic defaults
		if topic.StreamName == "" {
			config.Topics[i].StreamName = fmt.Sprintf("stream:%s", topic.Name)
		}
		if topic.ConsumerGroup == "" {
			config.Topics[i].ConsumerGroup = fmt.Sprintf("group:%s", topic.Name)
		}
		if topic.MaxLen == 0 {
			config.Topics[i].MaxLen = config.Streams.DefaultMaxLen
		}
		if topic.MaxAge == 0 {
			config.Topics[i].MaxAge = config.Streams.DefaultMaxAge
		}
		if topic.TrimStrategy == "" {
			config.Topics[i].TrimStrategy = config.Streams.DefaultTrimStrategy
		}
		if topic.RetryAttempts == 0 {
			config.Topics[i].RetryAttempts = 3
		}
	}

	return nil
}

// Helper methods for environment variable parsing

func (cl *ConfigLoader) getEnv(key string) string {
	return os.Getenv(fmt.Sprintf("%s_%s", cl.envPrefix, key))
}

func (cl *ConfigLoader) getEnvInt(key string) int {
	if value := cl.getEnv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return 0
}

func (cl *ConfigLoader) getEnvBool(key string) bool {
	value := strings.ToLower(cl.getEnv(key))
	return value == "true" || value == "1" || value == "yes" || value == "on"
}

func (cl *ConfigLoader) getEnvDuration(key string) time.Duration {
	if value := cl.getEnv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return 0
}

// LoadConfigFromPath is a convenience function to load config from a path
func LoadConfigFromPath(configPath string) (*Config, error) {
	loader := NewConfigLoader(configPath, "REDIS_STREAMS")
	return loader.LoadConfig()
}

// LoadConfigWithDefaults loads configuration with default values if no path provided
func LoadConfigWithDefaults(configPath string) (*Config, error) {
	// Try to load from default locations if no path provided
	if configPath == "" {
		// Try common config paths
		possiblePaths := []string{
			"config.yml",
			"config/redis-streams.yml",
			"/etc/redis-streams/config.yml",
			filepath.Join(os.Getenv("HOME"), ".redis-streams", "config.yml"),
		}

		for _, path := range possiblePaths {
			if _, err := os.Stat(path); err == nil {
				configPath = path
				break
			}
		}
	}

	return LoadConfigFromPath(configPath)
}

// SaveConfig saves configuration to YAML file (useful for generating templates)
func SaveConfig(config *Config, filePath string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Create directory if it doesn't exist
	if dir := filepath.Dir(filePath); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create config directory: %w", err)
		}
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}
