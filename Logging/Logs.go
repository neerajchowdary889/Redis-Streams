package Logging

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
	config "RedisStreams/Config"
)

// DefaultLogger implements basic logging to stdout
type DefaultLogger struct {
	logger *log.Logger
	level  LogLevel
}

// LogLevel represents logging levels
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

// NewLogger creates a new logger with the specified configuration
func NewLogger(config config.LoggingConfig) config.Logger {
	var output *os.File
	switch strings.ToLower(config.Output) {
	case "stdout", "":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		// File output
		file, err := os.OpenFile(config.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("Failed to open log file %s: %v, falling back to stdout", config.Output, err)
			output = os.Stdout
		} else {
			output = file
		}
	}

	level := parseLogLevel(config.Level)
	
	var logger *log.Logger
	if config.Structured {
		logger = log.New(output, "", 0) // No prefix for structured logs
	} else {
		logger = log.New(output, "[RedisStreams] ", log.LstdFlags)
	}

	return &DefaultLogger{
		logger: logger,
		level:  level,
	}
}

// parseLogLevel converts string level to LogLevel
func parseLogLevel(level string) LogLevel {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return DEBUG
	case "INFO", "":
		return INFO
	case "WARN", "WARNING":
		return WARN
	case "ERROR":
		return ERROR
	default:
		return INFO
	}
}

// Debug logs debug messages
func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	if l.level <= DEBUG {
		l.log("DEBUG", msg, args...)
	}
}

// Info logs info messages
func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	if l.level <= INFO {
		l.log("INFO", msg, args...)
	}
}

// Warn logs warning messages
func (l *DefaultLogger) Warn(msg string, args ...interface{}) {
	if l.level <= WARN {
		l.log("WARN", msg, args...)
	}
}

// Error logs error messages
func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	if l.level <= ERROR {
		l.log("ERROR", msg, args...)
	}
}

// log formats and writes log messages
func (l *DefaultLogger) log(level, msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	timestamp := time.Now().Format("2006-01-02T15:04:05.000Z")
	l.logger.Printf("[%s] %s: %s", timestamp, level, formatted)
}

// CreateTLSConfig creates TLS configuration from config
func CreateTLSConfig(config config.TLSConfig) *tls.Config {
	if !config.Enabled {
		return nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	// Load client certificate if provided
	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			log.Printf("Failed to load client certificate: %v", err)
		} else {
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	// Load CA certificate if provided
	if config.CAFile != "" {
		caCert, err := ioutil.ReadFile(config.CAFile)
		if err != nil {
			log.Printf("Failed to read CA certificate: %v", err)
		} else {
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				log.Printf("Failed to parse CA certificate")
			} else {
				tlsConfig.RootCAs = caCertPool
			}
		}
	}

	return tlsConfig
}