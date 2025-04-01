package config

import (
	"fmt"
	"log"
	"strings"

	"github.com/spf13/viper"
)

// Config holds the application configuration
type Config struct {
	DB struct {
		Host     string
		Port     int
		User     string
		Password string
		SSLMode  string
	}
	NATS struct {
		URL        string
		StreamName string
		Subject    string
	}
	BatchSize int
	Interval  int // in seconds
	LogLevel  string
}

// Load reads the configuration from file or environment variables
func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Set defaults
	viper.SetDefault("db.port", 5432)
	viper.SetDefault("db.sslmode", "disable")
	viper.SetDefault("nats.url", "nats://localhost:4222")
	viper.SetDefault("nats.streamname", "SUPREME_COURT_SUMMARIZATION_EVENT")
	viper.SetDefault("nats.subject", "SUPREME_COURT_SUMMARIZATION_EVENT.summarize")
	viper.SetDefault("batchsize", 10)
	viper.SetDefault("interval", 300)
	viper.SetDefault("loglevel", "info") // Default log level

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("No config file found, using environment variables and defaults")
		} else {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config into struct: %w", err)
	}

	return &cfg, nil
}
