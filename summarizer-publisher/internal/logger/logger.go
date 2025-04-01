package logger

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

var log zerolog.Logger

// Init initializes the logger with the given level and adds console output
func Init(level string) {
	// Parse log level
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	// Configure zerolog
	zerolog.SetGlobalLevel(lvl)
	zerolog.TimeFieldFormat = time.RFC3339

	// Create console writer with pretty printing for development
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	// Create multi-writer to write to both console and a log file if needed
	var writers []io.Writer
	writers = append(writers, consoleWriter)

	// Initialize logger
	log = zerolog.New(zerolog.MultiLevelWriter(writers...)).
		With().
		Timestamp().
		Caller().
		Logger()

	Info("Logger initialized with level: " + level)
}

// Debug logs a debug message
func Debug(msg string) {
	log.Debug().Msg(msg)
}

// Debugf logs a formatted debug message
func Debugf(format string, v ...interface{}) {
	log.Debug().Msg(fmt.Sprintf(format, v...))
}

// Info logs an info message
func Info(msg string) {
	log.Info().Msg(msg)
}

// Infof logs a formatted info message
func Infof(format string, v ...interface{}) {
	log.Info().Msg(fmt.Sprintf(format, v...))
}

// Warn logs a warning message
func Warn(msg string) {
	log.Warn().Msg(msg)
}

// Warnf logs a formatted warning message
func Warnf(format string, v ...interface{}) {
	log.Warn().Msg(fmt.Sprintf(format, v...))
}

// Error logs an error message
func Error(msg string) {
	log.Error().Msg(msg)
}

// Errorf logs a formatted error message
func Errorf(format string, v ...interface{}) {
	log.Error().Msg(fmt.Sprintf(format, v...))
}

// Fatal logs a fatal message and exits
func Fatal(msg string) {
	log.Fatal().Msg(msg)
}

// Fatalf logs a formatted fatal message and exits
func Fatalf(format string, v ...interface{}) {
	log.Fatal().Msg(fmt.Sprintf(format, v...))
}

// WithField adds a field to the logger and returns a logger instance with the field
func WithField(key string, value interface{}) zerolog.Logger {
	return log.With().Interface(key, value).Logger()
}

// WithFields adds multiple fields to the logger and returns a logger instance with the fields
func WithFields(fields map[string]interface{}) zerolog.Logger {
	ctx := log.With()
	for k, v := range fields {
		ctx = ctx.Interface(k, v)
	}
	return ctx.Logger()
}
